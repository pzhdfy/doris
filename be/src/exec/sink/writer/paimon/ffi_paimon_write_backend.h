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

#pragma once

#include <cctz/time_zone.h>
#include <gen_cpp/DataSinks_types.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/sink/writer/paimon/paimon_write_backend.h"
#include "format/parquet/arrow_memory_pool.h"
#include "format/table/paimon/paimon_arrow_block_convertor.h"
#include "runtime/runtime_profile.h"

namespace arrow {
class Schema;
}

namespace doris {

class RuntimeState;

/// Native Rust write backend: drives libpaimon_c (paimon-rust) directly from
/// the BE process, with no JVM in the write path. The lifecycle mirrors the
/// JNI backend -- open() opens the table and the write builder,
/// create_writer() hands out a per-LocalState writer adapter, prepare_commit()
/// serializes CommitMessages for the FE committer -- but every step goes
/// through the C ABI:
///
///   open:           paimon_table_from_schema_json  (table path + FE schema json
///                   paimon_table_export_arrow_schema  + storage options)
///                   paimon_table_new_write_builder_with_commit_user
///                   paimon_write_builder_with_overwrite (OVERWRITE mode)
///   write:          Block -> Arrow RecordBatch -> Arrow C Data
///                   paimon_table_write_write_arrow_batch
///   prepare_commit: paimon_table_write_prepare_commit
///                   paimon_commit_messages_count
///                   paimon_commit_messages_serialize_range (adaptive chunks)
///                   paimon_commit_message_serializer_version (DPCM header)
///
/// The serializer version is read from the linked library instead of being
/// compiled into the framing code, so moving the bundled Paimon Java release
/// (1.4.x / v11 -> 2.1 / v14) only changes the paimon-rust patch.
///
/// The writer type is selected by the session variable
/// enable_paimon_rust_writer; planning, transactions and the FE-side commit are
/// shared with the JNI path.
class FfiPaimonWriteBackend final : public IPaimonWriteBackend {
public:
    Status open(const TPaimonTableSink& sink, RuntimeState* state,
                RuntimeProfile* profile) override;
    Status create_writer(std::unique_ptr<IPaimonWriter>* writer) override;
    Status close() override;
    PaimonBackendType type() const override { return PaimonBackendType::FFI; }

    // Both constructor and destructor are declared here and defined in the
    // .cpp: the implicitly generated versions would be instantiated inline at
    // every construction site, and the default constructor's exception cleanup
    // destroys the pimpl member -- complete only in this class's own TU.
    FfiPaimonWriteBackend();
    ~FfiPaimonWriteBackend() override;

    /// Map each table-order Arrow field onto the sink output position carrying
    /// it: exact name match first, case-insensitive fallback second (Doris
    /// normalizes column names to lowercase, paimon preserves the declared
    /// case), and a field no sink column covers fails closed by name. A pure
    /// function of the names, so unit-testable without live FFI handles.
    static Status build_column_permutation(const arrow::Schema& schema,
                                           const std::vector<std::string>& sink_column_names,
                                           std::vector<size_t>* permutation);

    /// A permutation must name one distinct block position for every table
    /// field (the Rust writer's schema contract is positional). Empty, short or
    /// repeating permutations fail closed instead of mis-converting rows.
    static Status validate_permutation(const arrow::Schema& schema,
                                       const std::vector<size_t>& permutation);

private:
    /// The paimon-rust table and write-builder handles, RAII-wrapped (defined
    /// in the .cpp, like PaimonRustTableReader's handle struct, so that
    /// paimon_rust/paimon.h stays out of this header). Member order carries the
    /// teardown contract: the write builder references the table, so the table
    /// is declared first and reverse destruction frees the builder before it.
    struct FfiWriteHandles;
    std::unique_ptr<FfiWriteHandles> _handles;
    std::shared_ptr<arrow::Schema> _arrow_schema;
    /// Table-order field -> Block column position, computed in open() from
    /// sink.column_names. Copied (never moved) into each create_writer()
    /// writer so repeated writers all get the full mapping.
    std::vector<size_t> _column_permutation;
    /// Session timezone, captured in open() and reused by every writer adapter
    /// (the JNI backend does the same): the Arrow conversion of TIMESTAMP
    /// columns depends on it, and write() must not re-read it per batch.
    cctz::time_zone _timezone;
    bool _opened = false;
};

/// Opaque owning handle for the paimon-rust TableWrite: complete only in the
/// .cpp, so handing one to FfiPaimonWriter moves a typed handle instead of a
/// raw void* without leaking paimon_rust/paimon.h into this header.
struct FfiPaimonTableWrite;
using FfiPaimonTableWritePtr = std::unique_ptr<FfiPaimonTableWrite>;

class FfiPaimonWriter final : public IPaimonWriter {
public:
    /// The backend owns the table/write-builder handles and outlives this
    /// adapter; the TableWrite handle is created per writer and freed when this
    /// adapter is destroyed (abort() drops it early to discard staged files).
    /// `column_permutation` maps each Arrow schema (table-order) field to the
    /// position its column occupies in the Doris Block.
    explicit FfiPaimonWriter(FfiPaimonTableWritePtr table_write,
                             std::shared_ptr<arrow::Schema> arrow_schema,
                             std::vector<size_t> column_permutation, cctz::time_zone timezone);
    ~FfiPaimonWriter() override;

    Status write(RuntimeState* state, Block& block) override;
    Status prepare_commit(std::vector<TPaimonCommitMessage>& messages) override;
    Status abort() override;

    /// Build the Arrow column bindings once per writer, as the JNI writer does:
    /// re-creating the convertor per batch would re-resolve the schema the
    /// backend already exported from the Rust table.
    Status init();

private:
    FfiPaimonTableWritePtr _table_write;
    ArrowMemoryPool<> _arrow_pool;
    paimon::PaimonArrowBlockConvertor _arrow_block_convertor;
    std::shared_ptr<arrow::Schema> _arrow_schema;
    std::vector<size_t> _column_permutation;
    cctz::time_zone _timezone;
};

} // namespace doris
