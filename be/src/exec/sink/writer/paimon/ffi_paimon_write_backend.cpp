// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/sink/writer/paimon/ffi_paimon_write_backend.h"

#include "exec/sink/writer/paimon/paimon_dpcm_codec.h"

#include <arrow/array/util.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <fmt/format.h>

#include <algorithm>
#include <map>
#include <string>
#include <vector>

#include <boost/algorithm/string/predicate.hpp>

#include "common/logging.h"
#include "common/status.h"
#include "exec/sink/writer/paimon/paimon_write_backend.h"
#include "format/table/paimon/paimon_arrow_block_convertor.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "util/string_util.h"

// The full paimon-rust C surface (handles, results, error codes). The header
// ships with libpaimon_c under thirdparty/installed/include.
#include "paimon_rust/paimon.h"

namespace doris {

namespace {

// A copy of the schema with every field nullable: the input's nullability is
// independent from the target table constraint, exactly as the JNI connector
// declares its input fields (PaimonWriteSchema.java: "Input nullability is
// independent from the target table constraint"). Explicit NULLs and
// INSERT-omitted null-filled columns flow through to the Paimon writer, which
// returns its own constraint diagnostics; the Arrow assembly itself must not
// reject them first.
std::shared_ptr<arrow::Schema> nullable_input_schema(const arrow::Schema& schema) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    fields.reserve(schema.num_fields());
    for (const auto& field : schema.fields()) {
        fields.push_back(field->WithNullable(true));
    }
    return arrow::schema(std::move(fields));
}

// ---------------------------------------------------------------------------
// RAII wrappers over the paimon-rust C handles (same pattern as
// PaimonRustTableReader). Each handle is an opaque pointer owned by Rust and
// released by a matching paimon_*_free function.
// ---------------------------------------------------------------------------
#define PAIMON_OWNED(type, freefn)                        \
    struct type##_deleter {                               \
        void operator()(paimon_##type* p) const {         \
            if (p) {                                      \
                freefn(p);                                \
            }                                             \
        }                                                 \
    };                                                    \
    using type##_ptr = std::unique_ptr<paimon_##type, type##_deleter>

PAIMON_OWNED(table, paimon_table_free);
PAIMON_OWNED(write_builder, paimon_write_builder_free);
PAIMON_OWNED(table_write, paimon_table_write_free);
PAIMON_OWNED(commit_messages, paimon_commit_messages_free);
PAIMON_OWNED(error, paimon_error_free);

// RAII over a by-value paimon_bytes buffer (mirrors PaimonBytes in the reader).
class RustBytes {
public:
    explicit RustBytes(paimon_bytes bytes) : bytes_(bytes) {}
    ~RustBytes() { paimon_bytes_free(bytes_); }

    RustBytes(const RustBytes&) = delete;
    RustBytes& operator=(const RustBytes&) = delete;

    const uint8_t* data() const { return bytes_.data; }
    size_t size() const { return bytes_.len; }

private:
    paimon_bytes bytes_;
};

// Render a paimon_error into a string, taking ownership so it is freed on
// every path (including the exception-free early returns above).
std::string consume_error(paimon_error* err) {
    error_ptr owned(err);
    if (err == nullptr) {
        return "unknown error";
    }
    return std::string(reinterpret_cast<const char*>(err->message.data), err->message.len);
}

// ---------------------------------------------------------------------------
// FE-delivered storage options -> paimon-rust FileIO option keys.
//
// Mirrors PaimonRustTableReader::_build_options exactly, including its scheme
// scoping: the pinned paimon-rust storage dispatcher picks the FileIO parser
// from the table path's URI scheme, and Doris normalizes every object store's
// credentials into AWS_* / use_path_style aliases. Those can only be translated
// into the two families the oss and s3 parsers read, so every other scheme
// (hdfs://, file://, and the COS/OBS/GCS/Azdls families the FE gates to JNI)
// passes through untouched. Synthesizing s3.* keys for another parser would
// turn a missing-credential error into a misleading auth failure.
// ---------------------------------------------------------------------------
void normalize_rust_storage_options(std::map<std::string, std::string>* options,
                                    const std::string& table_path) {
    auto copy_if_missing = [options](const char* from_key, const char* to_key) {
        if (options->find(to_key) != options->end()) {
            return;
        }
        auto it = options->find(from_key);
        if (it != options->end() && !it->second.empty()) {
            (*options)[to_key] = it->second;
        }
    };

    std::string scheme;
    if (const auto sep = table_path.find("://"); sep != std::string::npos) {
        scheme = to_lower(table_path.substr(0, sep));
    }
    if (scheme == "oss") {
        // The OSS parser reads only the four fs.oss.* keys (and retry
        // settings); native fs.oss.* options pass through untouched. It has no
        // region / anonymous / assume-role handling, so nothing else is mapped.
        copy_if_missing("AWS_ENDPOINT", "fs.oss.endpoint");
        copy_if_missing("AWS_ACCESS_KEY", "fs.oss.accessKeyId");
        copy_if_missing("AWS_SECRET_KEY", "fs.oss.accessKeySecret");
        copy_if_missing("AWS_TOKEN", "fs.oss.securityToken");
        return;
    }
    if (scheme == "s3" || scheme == "s3a") {
        copy_if_missing("AWS_ACCESS_KEY", "s3.access-key");
        copy_if_missing("AWS_SECRET_KEY", "s3.secret-key");
        copy_if_missing("AWS_TOKEN", "s3.session.token");
        copy_if_missing("AWS_ENDPOINT", "s3.endpoint");
        copy_if_missing("AWS_REGION", "s3.region");
        copy_if_missing("use_path_style", "s3.path-style-access");
        // The FE storage-properties channel marks anonymous access with
        // AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS and assume-role with
        // AWS_ROLE_ARN / AWS_EXTERNAL_ID; the crate reads s3.anonymous
        // (skip_signature) and the s3.assumed.role.* family.
        auto provider = options->find("AWS_CREDENTIALS_PROVIDER_TYPE");
        if (provider != options->end() && provider->second == "ANONYMOUS") {
            (*options)["s3.anonymous"] = "true";
        }
        copy_if_missing("AWS_ROLE_ARN", "s3.assumed.role.arn");
        copy_if_missing("AWS_EXTERNAL_ID", "s3.assumed.role.externalId");
        // OSS-shaped options on an S3 warehouse (cross-protocol alias).
        copy_if_missing("fs.oss.accessKeyId", "s3.access-key");
        copy_if_missing("fs.oss.accessKeySecret", "s3.secret-key");
        copy_if_missing("fs.oss.sessionToken", "s3.session.token");
        copy_if_missing("fs.oss.endpoint", "s3.endpoint");
        copy_if_missing("fs.oss.region", "s3.region");
    }
}

} // namespace

// pimpl holding the backend's RAII handles: the table is declared before the
// write builder, so reverse destruction frees the builder -- which references
// the table -- first.
struct FfiPaimonWriteBackend::FfiWriteHandles {
    table_ptr table;
    write_builder_ptr write_builder;
};

// Out-of-line defaults: this is the only TU where FfiWriteHandles is complete,
// so the implicitly generated bodies -- including the constructor's exception
// cleanup and the members' destructors -- compile only here.
FfiPaimonWriteBackend::FfiPaimonWriteBackend() = default;
FfiPaimonWriteBackend::~FfiPaimonWriteBackend() = default;

// The paimon-rust TableWrite as the typed opaque handle the writer consumes:
// holds the PAIMON_OWNED unique_ptr, so destruction frees the handle no matter
// which path drops it, and get() hands the C ABI a typed pointer (no casts at
// the call sites).
struct FfiPaimonTableWrite {
    static FfiPaimonTableWritePtr adopt(paimon_table_write* raw) {
        return std::make_unique<FfiPaimonTableWrite>(table_write_ptr(raw));
    }
    paimon_table_write* get() const { return write.get(); }
    table_write_ptr write;
};

Status FfiPaimonWriteBackend::build_column_permutation(
        const arrow::Schema& schema, const std::vector<std::string>& sink_column_names,
        std::vector<size_t>* permutation) {
    permutation->clear();
    permutation->assign(schema.num_fields(), kMissingColumn);
    // Every sink column must name one distinct table field; a table field the
    // INSERT list omits stays kMissingColumn and the writer fills it with
    // null, exactly as the JNI writer's expand-by-name writes do.
    for (size_t j = 0; j < sink_column_names.size(); ++j) {
        const std::string& column = sink_column_names[j];
        int found = -1;
        int found_case_insensitive = -1;
        for (int i = 0; i < static_cast<int>(schema.num_fields()); ++i) {
            const std::string& field = schema.field(i)->name();
            if (field == column) {
                found = i;
                break;
            }
            if (found_case_insensitive < 0 && boost::iequals(field, column)) {
                found_case_insensitive = i;
            }
        }
        const int table_field = found >= 0 ? found : found_case_insensitive;
        if (table_field < 0) {
            return Status::InternalError(
                    "paimon-rust FFI writer sink column '{}' is not in table schema", column);
        }
        if ((*permutation)[table_field] != kMissingColumn) {
            return Status::InternalError(
                    "paimon-rust FFI writer duplicate write column '{}'", column);
        }
        (*permutation)[table_field] = j;
    }
    return Status::OK();
}

Status FfiPaimonWriteBackend::validate_permutation(const arrow::Schema& schema,
                                                   const std::vector<size_t>& permutation,
                                                   size_t sink_column_count) {
    if (permutation.size() != static_cast<size_t>(schema.num_fields())) {
        return Status::InternalError(
                "paimon-rust FFI writer column permutation covers {} of {} table fields",
                permutation.size(), schema.num_fields());
    }
    std::vector<size_t> present;
    present.reserve(permutation.size());
    for (size_t position : permutation) {
        if (position == kMissingColumn) {
            continue;
        }
        if (position >= sink_column_count) {
            return Status::InternalError(
                    "paimon-rust FFI writer column permutation names block position {} beyond "
                    "the {} sink columns",
                    position, sink_column_count);
        }
        present.push_back(position);
    }
    std::sort(present.begin(), present.end());
    if (std::adjacent_find(present.begin(), present.end()) != present.end()) {
        return Status::InternalError(
                "paimon-rust FFI writer column permutation repeats a block position");
    }
    return Status::OK();
}

Status FfiPaimonWriteBackend::open(const TPaimonTableSink& sink, RuntimeState* state,
                                   RuntimeProfile* profile) {
    DORIS_CHECK(sink.__isset.backend_type && sink.backend_type == TPaimonWriteBackendType::FFI);
    DORIS_CHECK(sink.__isset.write_mode);
    DORIS_CHECK(sink.__isset.transaction_id && sink.transaction_id > 0);
    DORIS_CHECK(sink.__isset.commit_user && !sink.commit_user.empty());
    DORIS_CHECK(sink.__isset.table_location && !sink.table_location.empty());
    DORIS_CHECK(sink.__isset.table_schema_json && !sink.table_schema_json.empty());
    DORIS_CHECK(sink.__isset.db_name && !sink.db_name.empty());
    DORIS_CHECK(sink.__isset.table_name && !sink.table_name.empty());
    DORIS_CHECK(sink.__isset.column_names);
    DORIS_CHECK(profile != nullptr);

    if (_opened) {
        return Status::InternalError("FfiPaimonWriteBackend opened twice");
    }

    _timezone = state->timezone_obj();

    // The Rust C ABI has no changelog producer (only with_overwrite); fail
    // closed instead of silently appending.
    if (sink.write_mode == TPaimonWriteMode::CHANGELOG) {
        return Status::NotSupported(
                "paimon-rust FFI writer does not support CHANGELOG write mode; use the JNI "
                "write path");
    }

    // Step 1: open the table from the FE-supplied path + TableSchema JSON, the
    // same call the rust reader makes. FE already resolved the schema, so no
    // schema-file round trip and no catalog credentials are needed here; branch
    // travels separately because the Rust side rejects null/empty.
    std::map<std::string, std::string> options;
    if (sink.__isset.table_options) {
        options.insert(sink.table_options.begin(), sink.table_options.end());
    }
    normalize_rust_storage_options(&options, sink.table_location);
    std::vector<paimon_option> c_options;
    c_options.reserve(options.size());
    for (const auto& kv : options) {
        c_options.emplace_back(paimon_option {kv.first.c_str(), kv.second.c_str()});
    }
    const std::string branch = sink.__isset.branch && !sink.branch.empty() ? sink.branch : "main";

    // Writes never route through the Alluxio cache; pass a plain FileIO.
    paimon_result_get_table tbl_res = paimon_table_from_schema_json(
            sink.table_location.c_str(), sink.table_schema_json.c_str(), sink.db_name.c_str(),
            sink.table_name.c_str(), branch.c_str(),
            c_options.empty() ? nullptr : c_options.data(), c_options.size());
    if (tbl_res.error != nullptr) {
        return Status::InternalError(
                "paimon-rust FFI writer open table {}.{} at {} failed: {}", sink.db_name,
                sink.table_name, sink.table_location, consume_error(tbl_res.error));
    }
    table_ptr table(tbl_res.table);

    // Step 2: fetch the target Arrow schema from the same table handle the
    // writes go into -- the schema the Rust writer normalizes against, so the
    // Doris-side Block->Arrow conversion can never drift from it.
    ArrowSchema c_schema {};
    if (paimon_error* err = paimon_table_export_arrow_schema(table.get(), &c_schema)) {
        return Status::InternalError("paimon-rust FFI writer export arrow schema failed: {}",
                                     consume_error(err));
    }
    // ImportSchema releases the C struct on success and failure alike.
    auto schema_result = arrow::ImportSchema(&c_schema);
    if (!schema_result.ok()) {
        return Status::InternalError("paimon-rust FFI writer import arrow schema failed: {}",
                                     schema_result.status().ToString());
    }
    std::shared_ptr<arrow::Schema> arrow_schema = std::move(schema_result).ValueOrDie();

    // The Rust writer's schema contract is positional against the full table
    // schema, while the sink delivers the Block in sink.column_names order
    // (which follows the INSERT's column list). Map every table-order field to
    // its sink position -- kMissingColumn for INSERT-omitted fields -- and
    // build the sink-order conversion schema the Block converts against; the
    // omitted fields are filled with null in write(), mirroring the JNI
    // writer's expand-by-name writes.
    std::vector<size_t> column_permutation;
    RETURN_IF_ERROR(
            build_column_permutation(*arrow_schema, sink.column_names, &column_permutation));
    std::vector<std::shared_ptr<arrow::Field>> sink_fields;
    sink_fields.reserve(sink.column_names.size());
    std::vector<size_t> sink_to_table(sink.column_names.size(), kMissingColumn);
    for (size_t i = 0; i < column_permutation.size(); ++i) {
        if (column_permutation[i] != kMissingColumn) {
            sink_to_table[column_permutation[i]] = i;
        }
    }
    for (size_t j = 0; j < sink_to_table.size(); ++j) {
        if (sink_to_table[j] == kMissingColumn) {
            // build_column_permutation maps every sink column onto a table
            // field; reaching here means the two vectors disagree, which only
            // a concurrent schema change could produce.
            return Status::InternalError(
                    "paimon-rust FFI writer sink column {} has no table field",
                    sink.column_names[j]);
        }
        sink_fields.push_back(
                arrow_schema->field(static_cast<int>(sink_to_table[j]))->WithNullable(true));
    }
    std::shared_ptr<arrow::Schema> convertor_schema = arrow::schema(std::move(sink_fields));

    // Step 3: build the write builder. The commit_user travels in the sink
    // payload and is shared by every writer of this plan (FE dedupes commits on
    // it), so a per-LocalState writer set reuses one logical commit.
    paimon_result_write_builder wb_res = paimon_table_new_write_builder_with_commit_user(
            table.get(), sink.commit_user.c_str());
    if (wb_res.error != nullptr) {
        return Status::InternalError("paimon-rust FFI writer new write builder failed: {}",
                                     consume_error(wb_res.error));
    }
    write_builder_ptr write_builder(wb_res.write_builder);

    if (sink.write_mode == TPaimonWriteMode::OVERWRITE) {
        if (paimon_error* err = paimon_write_builder_with_overwrite(write_builder.get())) {
            return Status::InternalError(
                    "paimon-rust FFI writer set overwrite mode failed: {}",
                    consume_error(err));
        }
    }

    _handles = std::make_unique<FfiWriteHandles>(std::move(table), std::move(write_builder));
    _arrow_schema = std::move(arrow_schema);
    _column_permutation = std::move(column_permutation);
    _convertor_schema = std::move(convertor_schema);
    _opened = true;

    size_t omitted = 0;
    for (size_t position : _column_permutation) {
        if (position == kMissingColumn) {
            ++omitted;
        }
    }
    LOG(INFO) << "Paimon Rust FFI writer opened: table=" << sink.table_location
              << ", columns=" << sink.column_names.size() << " ("
              << _arrow_schema->num_fields() - omitted << " of " << _arrow_schema->num_fields()
              << " table fields)"
              << ", write_mode=" << sink.write_mode << ", txn=" << sink.transaction_id;
    return Status::OK();
}

Status FfiPaimonWriteBackend::create_writer(std::unique_ptr<IPaimonWriter>* writer) {
    DORIS_CHECK(_opened);
    // The TableWrite handle is created per writer (one per LocalState) from the
    // shared write builder; prepare_commit on it produces this writer's
    // CommitMessages only.
    paimon_result_table_write tw_res =
            paimon_write_builder_new_write(_handles->write_builder.get());
    if (tw_res.error != nullptr) {
        return Status::InternalError("paimon-rust FFI writer new write failed: {}",
                                     consume_error(tw_res.error));
    }
    // RAII until the adapter takes ownership, so an allocation failure between
    // creation and handoff cannot leak the TableWrite handle.
    table_write_ptr table_write(tw_res.write);
    // Copy, not move: create_writer() is repeatable by design (one TableWrite
    // per writer from the shared builder).
    std::vector<size_t> permutation(_column_permutation);
    RETURN_IF_ERROR(
            validate_permutation(*_arrow_schema, permutation, _convertor_schema->num_fields()));
    auto ffi_writer = std::make_unique<FfiPaimonWriter>(
            FfiPaimonTableWrite::adopt(table_write.release()), _convertor_schema, _arrow_schema,
            std::move(permutation), _timezone);
    // Bind the Arrow column writers once, as the JNI path does; a binding
    // failure must surface here rather than on the first batch.
    RETURN_IF_ERROR(ffi_writer->init());
    *writer = std::move(ffi_writer);
    return Status::OK();
}

Status FfiPaimonWriteBackend::close() {
    // Reverse of open() is implicit: FfiWriteHandles destroys the write builder
    // (it references the table) before the table, by member order. Writers'
    // TableWrite handles are owned and RAII-freed by their FfiPaimonWriter
    // adapters, which are gone before close() runs.
    _handles.reset();
    _arrow_schema.reset();
    _column_permutation.clear();
    _convertor_schema.reset();
    _opened = false;
    return Status::OK();
}

// ---------------------------------------------------------------------------
// FfiPaimonWriter
// ---------------------------------------------------------------------------

FfiPaimonWriter::FfiPaimonWriter(FfiPaimonTableWritePtr table_write,
                                 std::shared_ptr<arrow::Schema> convertor_schema,
                                 std::shared_ptr<arrow::Schema> arrow_schema,
                                 std::vector<size_t> column_permutation, cctz::time_zone timezone)
        : _table_write(std::move(table_write)),
          _arrow_block_convertor(convertor_schema, timezone),
          _arrow_schema(std::move(arrow_schema)),
          _convertor_schema(std::move(convertor_schema)),
          _batch_schema(nullable_input_schema(*_arrow_schema)),
          _column_permutation(std::move(column_permutation)),
          _timezone(timezone) {}

// Out-of-line for the same reason as the backend's: only this TU sees
// FfiPaimonTableWrite complete.
FfiPaimonWriter::~FfiPaimonWriter() = default;

Status FfiPaimonWriter::init() {
    return _arrow_block_convertor.init();
}

Status FfiPaimonWriter::write(RuntimeState* /*state*/, Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }
    if (_table_write == nullptr) {
        return Status::InternalError("paimon-rust FFI writer used after abort");
    }
    if (_convertor_schema == nullptr || _convertor_schema->num_fields() != block.columns()) {
        return Status::InvalidArgument(
                "Paimon FFI writer sink schema column count does not match Doris Block: "
                "schema={}, block={}",
                _convertor_schema == nullptr ? 0 : _convertor_schema->num_fields(),
                block.columns());
    }

    // Convert the Block in its delivered sink order against the sink-order
    // conversion schema, then assemble the full table-order batch the Rust
    // writer validates positionally: present fields take their converted
    // column, INSERT-omitted fields become all-null columns, mirroring the
    // JNI writer's expand-by-name writes.
    std::shared_ptr<arrow::RecordBatch> sink_batch;
    RETURN_IF_ERROR(_arrow_block_convertor.convert_to_arrow(block, &_arrow_pool, &sink_batch));

    std::vector<std::shared_ptr<arrow::Array>> table_columns;
    table_columns.reserve(_arrow_schema->num_fields());
    constexpr size_t kMissing = FfiPaimonWriteBackend::kMissingColumn;
    for (int i = 0; i < _arrow_schema->num_fields(); ++i) {
        if (_column_permutation[i] != kMissing) {
            table_columns.push_back(
                    sink_batch->column(static_cast<int>(_column_permutation[i])));
            continue;
        }
        auto null_array =
                arrow::MakeArrayOfNull(_arrow_schema->field(i)->type(), sink_batch->num_rows(),
                                       static_cast<arrow::MemoryPool*>(&_arrow_pool));
        if (!null_array.ok()) {
            return Status::InternalError(
                    "paimon-rust FFI writer build null column for omitted field '{}' failed: {}",
                    _arrow_schema->field(i)->name(), null_array.status().ToString());
        }
        table_columns.push_back(std::move(null_array).ValueOrDie());
    }
    std::shared_ptr<arrow::RecordBatch> record_batch =
            arrow::RecordBatch::Make(_batch_schema, sink_batch->num_rows(),
                                     std::move(table_columns));

    // Null pre-check with the exact diagnostic the Java writer produces, so
    // regression assertions on 'Cannot write null to non-null column(x)' are
    // backend-agnostic. An INSERT-omitted field is null by construction here;
    // a present field checks its delivered column. The stock crate's
    // primary-key path re-validates with the same diagnostic (and the correct
    // DELETE-kind exceptions), but its append path reports a lower-level
    // schema error instead, so the check lives here uniformly for every
    // engine. Nullability comes from the Rust-exported table schema, not the
    // nullable-ified batch schema this assembly validates against.
    for (int i = 0; i < _arrow_schema->num_fields(); ++i) {
        const auto& field = _arrow_schema->field(i);
        if (!field->nullable() && record_batch->column(i)->null_count() > 0) {
            return Status::InvalidArgument("Cannot write null to non-null column({})",
                                           field->name());
        }
    }

    ArrowArray c_array {};
    ArrowSchema c_schema {};
    auto arrow_status = arrow::ExportRecordBatch(*record_batch, &c_array, &c_schema);
    if (!arrow_status.ok()) {
        return Status::InternalError("Failed to export Paimon Arrow RecordBatch: {}",
                                     arrow_status.ToString());
    }
    Defer release_c_data {[&] {
        if (c_array.release != nullptr) {
            c_array.release(&c_array);
        }
        if (c_schema.release != nullptr) {
            c_schema.release(&c_schema);
        }
    }};

    if (paimon_error* err =
                paimon_table_write_write_arrow_batch(_table_write->get(), &c_array, &c_schema)) {
        return Status::InternalError("paimon-rust FFI writer write batch failed: {}",
                                     consume_error(err));
    }
    return Status::OK();
}

Status FfiPaimonWriter::prepare_commit(std::vector<TPaimonCommitMessage>& messages) {
    if (_table_write == nullptr) {
        return Status::InternalError("paimon-rust FFI writer used after abort");
    }

    // Close the writer's data files and collect this writer's CommitMessages.
    paimon_result_prepare_commit pc_res = paimon_table_write_prepare_commit(_table_write->get());
    if (pc_res.error != nullptr) {
        return Status::InternalError("paimon-rust FFI writer prepare commit failed: {}",
                                     consume_error(pc_res.error));
    }
    commit_messages_ptr msgs(pc_res.messages);

    // The message count is queried from the handle rather than carried in the
    // prepare result, so paimon_result_prepare_commit keeps the layout other
    // consumers already depend on.
    const size_t total_messages = paimon_commit_messages_count(msgs.get());

    // Adaptive chunk + DPCM framing via the shared codec (paimon_dpcm_codec.h).
    // This writer never merges other writers' messages into its handle, so the
    // count at prepare time is the range bound here.
    auto serialize_range = [&](size_t start, size_t count, std::string* body) -> Status {
        paimon_result_bytes bytes_res =
                paimon_commit_messages_serialize_range(msgs.get(), start, count);
        if (bytes_res.error != nullptr) {
            return Status::InternalError(
                    "paimon-rust FFI writer serialize commit messages [{},{}) failed: {}", start,
                    start + count, consume_error(bytes_res.error));
        }
        RustBytes bytes(bytes_res.bytes);
        body->assign(reinterpret_cast<const char*>(bytes.data()), bytes.size());
        return Status::OK();
    };
    // The serializer version comes from the linked library, not from this
    // translation unit: it names the Paimon Java release the FE decodes with,
    // so moving that release only changes the paimon-rust patch.
    const int32_t serializer_version = paimon_commit_message_serializer_version();
    return build_dpcm_framed_commit_payloads(total_messages, serializer_version, serialize_range,
                                             &messages);
}

Status FfiPaimonWriter::abort() {
    // Dropping the TableWrite before commit discards the staged data files;
    // paimon's orphan-file cleanup reclaims them, the same abort semantics as
    // the Java writer's abort(). After this the writer must not be used.
    _table_write.reset();
    return Status::OK();
}

} // namespace doris
