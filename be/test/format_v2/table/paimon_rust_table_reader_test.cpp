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

#include "format_v2/table/paimon_rust_table_reader.h"

#include <gtest/gtest.h>

#include <cmath>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "format/table/paimon_rust_predicate_converter.h"
#include "format_v2/column_data.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/url_coding.h"

#include <chrono>

#include "cctz/time_zone.h"

namespace doris::format::paimon {
namespace {

ColumnDefinition make_column(const std::string& name, const DataTypePtr& type,
                             bool is_partition_key = false) {
    ColumnDefinition column;
    column.name = name;
    column.type = type->is_nullable() ? type : make_nullable(type);
    column.is_partition_key = is_partition_key;
    return column;
}

TFileRangeDesc make_rust_range() {
    TFileRangeDesc range;
    TTableFormatFileDesc table_format_params;
    table_format_params.__set_table_format_type("paimon");
    TPaimonFileDesc paimon_params;
    paimon_params.__set_reader_type(TPaimonReaderType::PAIMON_RUST);
    std::string encoded;
    base64_encode("dummy-split-bytes", &encoded);
    paimon_params.__set_paimon_split(encoded);
    paimon_params.__set_paimon_table("/paimon/warehouse/db.db/t");
    paimon_params.__set_db_name("db");
    paimon_params.__set_table_name("t");
    paimon_params.__set_paimon_table_schema_json("{}");
    table_format_params.__set_paimon_params(paimon_params);
    range.__set_table_format_params(table_format_params);
    return range;
}

} // namespace

class PaimonRustTableReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _query_options.__set_batch_size(3);
        _runtime_state = RuntimeState::create_unique(_query_options, _query_globals);
    }

    Status init_reader_with_count(PaimonRustTableReader* reader,
                                  std::vector<GlobalIndex> count_columns) {
        return reader->init({.projected_columns = {_projected_column},
                             .conjuncts = {},
                             .format = FileFormat::JNI,
                             .scan_params = nullptr,
                             .io_ctx = nullptr,
                             .runtime_state = _runtime_state.get(),
                             .scanner_profile = nullptr,
                             .push_down_agg_type = TPushAggOp::type::COUNT,
                             .push_down_count_columns = std::move(count_columns)});
    }

    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    std::unique_ptr<RuntimeState> _runtime_state;
    ColumnDefinition _projected_column = make_column("k", std::make_shared<DataTypeInt32>());
};

TEST_F(PaimonRustTableReaderTest, ValidatesRustSplit) {
    PaimonRustTableReader reader;

    // Missing paimon_split.
    auto range = make_rust_range();
    range.table_format_params.paimon_params.__isset.paimon_split = false;
    auto status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_split"), std::string::npos) << status;

    // Missing paimon_table (table path).
    range = make_rust_range();
    range.table_format_params.paimon_params.__isset.paimon_table = false;
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_table"), std::string::npos) << status;

    // Missing db_name.
    range = make_rust_range();
    range.table_format_params.paimon_params.__isset.db_name = false;
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing db_name"), std::string::npos) << status;

    // Missing table_name.
    range = make_rust_range();
    range.table_format_params.paimon_params.__isset.table_name = false;
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing table_name"), std::string::npos) << status;

    // Missing paimon_table_schema_json.
    range = make_rust_range();
    range.table_format_params.paimon_params.__isset.paimon_table_schema_json = false;
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing paimon_table_schema_json"), std::string::npos)
            << status;

    // A mismatched reader_type is a protocol error.
    range = make_rust_range();
    range.table_format_params.paimon_params.__set_reader_type(TPaimonReaderType::PAIMON_JNI);
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("invalid reader_type"), std::string::npos) << status;

    // A complete range validates cleanly.
    EXPECT_TRUE(reader.TEST_validate_rust_split(make_rust_range()).ok());
}

TEST_F(PaimonRustTableReaderTest, ValidatesVectorSplit) {
    PaimonRustTableReader reader;

    auto make_vector_range = [] {
        // Vector mode: paimon_split is deliberately an empty string (FE avoids
        // serializing the same DataSplit twice) and the payload is the serialized
        // BucketVectorSearchSplit.
        auto range = make_rust_range();
        range.table_format_params.paimon_params.__set_paimon_split("");
        TPaimonVectorPayload payload;
        payload.__set_payload_type(TPaimonVectorPayloadType::BUCKET_SPLIT_BYTES);
        payload.__set_bytes("PKVSPLIT-dummy");
        range.table_format_params.paimon_params.__set_vector_payload(payload);
        return range;
    };

    // Missing vector_payload: a protocol error, not a fall-through to the empty
    // paimon_split check (whose error text would point the reader at the wrong field).
    auto range = make_vector_range();
    range.table_format_params.paimon_params.__isset.vector_payload = false;
    auto status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing vector_payload"), std::string::npos) << status;

    // Empty payload bytes are equally a protocol error.
    range = make_vector_range();
    range.table_format_params.paimon_params.vector_payload.bytes.clear();
    status = reader.TEST_validate_rust_split(range);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("missing vector_payload"), std::string::npos) << status;

    // A complete vector range validates cleanly -- including the empty paimon_split,
    // which the normal path would reject.
    EXPECT_TRUE(reader.TEST_validate_rust_split(make_vector_range()).ok());
}

// ---------------------------------------------------------------- score -> distance
//
// paimon-rust encodes the distance as a score per metric (see
// convert_distance_to_score in crates/paimon/src/lumina/reader.rs); the reader has
// to undo exactly that so the column means what the Doris function returns. For l2
// the score is 1/(1 + SQUARED distance), hence sqrt(1/score - 1). The reference
// points below are exact in binary floating point.

using ScoreTransform = PaimonRustTableReader::ScoreTransform;
static constexpr auto L2 = ScoreTransform::L2;
static constexpr auto INNER_PRODUCT = ScoreTransform::InnerProduct;

// A one-column block holding `scores` in a (possibly nullable) FLOAT column,
// standing in for what the arrow fill loop leaves in the distance column.
static Block _score_block(const std::vector<float>& scores, bool nullable) {
    DataTypePtr type = std::make_shared<DataTypeFloat32>();
    if (nullable) {
        type = std::make_shared<DataTypeNullable>(type);
    }
    MutableColumnPtr column = type->create_column();
    for (float score : scores) {
        column->insert_data(reinterpret_cast<const char*>(&score), sizeof(float));
    }
    Block block;
    block.insert(ColumnWithTypeAndName(std::move(column), type, kPaimonSearchDistanceColumn));
    return block;
}

// The float at `row` of the block's only column, whether nullable or not.
static float _score_at(const Block& block, size_t row) {
    const auto& column = block.get_by_position(0).column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
        return assert_cast<const ColumnFloat32&>(nullable->get_nested_column()).get_data()[row];
    }
    return assert_cast<const ColumnFloat32&>(*column).get_data()[row];
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceL2) {
    // score 1.0 -> squared 0 -> distance 0 (an exact hit)
    // score 0.5 -> squared 1 -> distance 1
    // score 0.2 -> squared 4 -> distance 2
    //
    // Keep the score == 1.0 row: it is the legal boundary, sitting immediately next
    // to the score > 1 corruption case below, and 1/1 - 1 is exactly 0 in floating
    // point. A guard phrased as "sqrt(radicand) if radicand > 0, else sentinel"
    // passes every other case here while misreporting exact hits as corrupt.
    Block block = _score_block({1.0F, 0.5F, 0.2F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, 0, 3, L2);

    EXPECT_FLOAT_EQ(0.0F, _score_at(block, 0));
    EXPECT_FLOAT_EQ(1.0F, _score_at(block, 1));
    EXPECT_FLOAT_EQ(2.0F, _score_at(block, 2));
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceClampsCatastrophicCancellation) {
    // The lumina kernel computes squared L2 as ||a||^2 + ||b||^2 - 2<a,b>, which can
    // yield a small negative residual when a ~= b, i.e. score slightly ABOVE 1. That
    // is a near-exact hit, not corruption: it clamps to distance 0. Only a score past
    // kScoreAboveOneTolerance is sentineled.
    Block block = _score_block({1.0F + 1e-4F, 1.0F + 5e-2F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, 0, 2, L2);

    EXPECT_FLOAT_EQ(0.0F, _score_at(block, 0));
    EXPECT_FLOAT_EQ(PaimonRustTableReader::kCorruptScoreAboveOne, _score_at(block, 1));
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceSkipsRowsBeforeOffset) {
    // Block columns are append-only and a block is reused across get_block calls
    // when a batch is fully filtered out, so rows converted by an earlier call must
    // be left alone. Here rows 0-1 already hold distances; only the two raw scores
    // appended after them may be touched.
    Block block = _score_block({0.0F, 2.0F, 0.5F, 0.2F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, /*row_offset=*/2,
                                                      /*num_rows=*/2, L2);

    // Untouched: converting again would turn distance 2.0 into sqrt(1/2 - 1) garbage.
    EXPECT_FLOAT_EQ(0.0F, _score_at(block, 0));
    EXPECT_FLOAT_EQ(2.0F, _score_at(block, 1));
    // Converted.
    EXPECT_FLOAT_EQ(1.0F, _score_at(block, 2));
    EXPECT_FLOAT_EQ(2.0F, _score_at(block, 3));
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceNullable) {
    // FE declares the score column non-nullable, but the slot decides the actual
    // block column, so the nullable shape must work too: null rows stay null and
    // their payload is not read.
    Block block = _score_block({0.5F, 0.0F, 0.2F}, /*nullable=*/true);
    auto nullable = IColumn::mutate(std::move(block.get_by_position(0).column));
    assert_cast<ColumnNullable*>(nullable.get())->get_null_map_data()[1] = 1;
    block.get_by_position(0).column = std::move(nullable);

    PaimonRustTableReader::_convert_score_to_distance(&block, 0, 0, 3, L2);

    EXPECT_FLOAT_EQ(1.0F, _score_at(block, 0));
    EXPECT_FLOAT_EQ(2.0F, _score_at(block, 2));
    const auto* null_column =
            check_and_get_column<ColumnNullable>(block.get_by_position(0).column.get());
    ASSERT_NE(null_column, nullptr);
    EXPECT_EQ(1, null_column->get_null_map_data()[1]);
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceFlagsOutOfRangeScores) {
    // Unreachable for a well-formed index (d >= 0 implies score in (0, 1]), but the
    // distances come out of the closed-source lumina kernel unclamped. Computing the
    // inverse anyway would give +inf (1/0) and NaN (sqrt of a negative), either of
    // which makes the coordinator's Top-N comparisons ill-defined. Each case maps to
    // its own negative sentinel: negative is unreachable for a real Euclidean
    // distance, stays finite and totally ordered, and tells which way it broke.
    Block block = _score_block({0.0F, -1.0F, 1.5F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, 0, 3, L2);

    EXPECT_FLOAT_EQ(PaimonRustTableReader::kCorruptScoreNotPositive, _score_at(block, 0));
    EXPECT_FLOAT_EQ(PaimonRustTableReader::kCorruptScoreNotPositive, _score_at(block, 1));
    EXPECT_FLOAT_EQ(PaimonRustTableReader::kCorruptScoreAboveOne, _score_at(block, 2));
    for (size_t i = 0; i < 3; ++i) {
        const float distance = _score_at(block, i);
        EXPECT_TRUE(std::isfinite(distance)) << "row " << i << " = " << distance;
        // Must be distinguishable from any genuine distance, which is >= 0.
        EXPECT_LT(distance, 0.0F) << "row " << i;
    }
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceEmptyBatch) {
    // An empty batch must be a no-op rather than an out-of-range write.
    Block block = _score_block({0.5F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, /*row_offset=*/1,
                                                     /*num_rows=*/0, L2);
    EXPECT_FLOAT_EQ(0.5F, _score_at(block, 0));
}

TEST_F(PaimonRustTableReaderTest, ConvertScoreToDistanceInnerProductIsIdentity) {
    // For inner_product the score IS the value the Doris function returns, so every
    // row must survive untouched -- including negative and out-of-(0,1] values, which
    // are legal here (an inner product is unbounded) and must NOT be mistaken for
    // the out-of-range l2 scores that get a corruption sentinel.
    Block block = _score_block({0.5F, 0.0F, -3.25F, 42.0F}, /*nullable=*/false);
    PaimonRustTableReader::_convert_score_to_distance(&block, 0, 0, 4, INNER_PRODUCT);

    EXPECT_FLOAT_EQ(0.5F, _score_at(block, 0));
    EXPECT_FLOAT_EQ(0.0F, _score_at(block, 1));
    EXPECT_FLOAT_EQ(-3.25F, _score_at(block, 2));
    EXPECT_FLOAT_EQ(42.0F, _score_at(block, 3));
}

// ---------------------------------------------------------------- metric parsing

TEST_F(PaimonRustTableReaderTest, ParseScoreTransformAcceptsSupportedMetrics) {
    ScoreTransform transform = INNER_PRODUCT;
    ASSERT_TRUE(PaimonRustTableReader::parse_score_transform(TVectorMetric::L2, &transform).ok());
    EXPECT_EQ(L2, transform);

    ASSERT_TRUE(
            PaimonRustTableReader::parse_score_transform(TVectorMetric::DOT_PRODUCT, &transform)
                    .ok());
    EXPECT_EQ(INNER_PRODUCT, transform);
}

TEST_F(PaimonRustTableReaderTest, ParseScoreTransformRejectsUnsupportedMetrics) {
    // Must fail rather than fall back to l2: guessing would sqrt an inner product,
    // or return a cosine score where a distance was promised. v1 does not push
    // cosine down, so seeing it here means FE and BE disagree.
    ScoreTransform transform = L2;
    for (auto metric : {TVectorMetric::COSINE, TVectorMetric::DEFAULT, TVectorMetric::HAMMING}) {
        auto status = PaimonRustTableReader::parse_score_transform(metric, &transform);
        EXPECT_FALSE(status.ok()) << "metric " << static_cast<int>(metric) << " must be rejected";
        EXPECT_NE(status.to_string().find("unsupported distance metric"), std::string::npos)
                << status;
    }
}

// ------------------------------------------------- arrow field -> block column

TEST_F(PaimonRustTableReaderTest, ArrowFieldToBlockColumnRenamesScoreField) {
    // FE names the output column "..._to_dis" because the reader turns the score
    // into a distance, but paimon-rust keeps emitting the Arrow field as
    // "__paimon_search_score". The fill loop silently skips Arrow fields it cannot
    // match, so if this mapping regressed the distance column would come back
    // empty rather than error out -- hence the explicit assertion.
    EXPECT_EQ(kPaimonSearchDistanceColumn,
              PaimonRustTableReader::arrow_field_to_block_column(kPaimonRustScoreField));
    // paimon-rust may return the canonical (any-case) field name.
    EXPECT_EQ(kPaimonSearchDistanceColumn,
              PaimonRustTableReader::arrow_field_to_block_column("__Paimon_Search_Score"));
}

TEST_F(PaimonRustTableReaderTest, ArrowFieldToBlockColumnLowercasesOtherFields) {
    // Ordinary table columns are only case-folded: paimon-rust resolves projections
    // case-insensitively and returns the table's canonical casing, while block
    // columns are FE-normalized to lowercase.
    EXPECT_EQ("is_qt", PaimonRustTableReader::arrow_field_to_block_column("is_QT"));
    EXPECT_EQ("id", PaimonRustTableReader::arrow_field_to_block_column("id"));
    // Not the score field, so it must NOT be rewritten to the distance column.
    EXPECT_EQ("__paimon_search_score_extra",
              PaimonRustTableReader::arrow_field_to_block_column("__paimon_search_score_extra"));
}

TEST_F(PaimonRustTableReaderTest, TableLevelCountEmitsSyntheticRows) {
    // COUNT(*) with a table-level row count takes the base-class metadata path:
    // prepare_split never opens the rust pipeline and get_block emits synthetic rows.
    PaimonRustTableReader reader;
    ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());

    SplitReadOptions options;
    options.current_range = make_rust_range();
    options.current_split_format = FileFormat::JNI;
    options.all_runtime_filters_applied = true;
    options.current_range.table_format_params.__set_table_level_row_count(5);
    ASSERT_TRUE(reader.prepare_split(options).ok());
    EXPECT_TRUE(reader.current_split_uses_metadata_count());

    Block block = Block({ColumnWithTypeAndName(
            _projected_column.type->create_column(), _projected_column.type,
            _projected_column.name)});
    bool eos = false;
    // The base-class count contract emits batches until a call finds remaining==0:
    // batch_size(3) splits 5 rows into 3 + 2, and only the following call reports eos.
    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 3);
    EXPECT_FALSE(eos);

    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 2);
    EXPECT_FALSE(eos);

    ASSERT_TRUE(reader.get_block(&block, &eos).ok());
    EXPECT_EQ(block.rows(), 0);
    EXPECT_TRUE(eos);
}

TEST_F(PaimonRustTableReaderTest, TableLevelCountDisabledByConjuncts) {
    // A row predicate makes the metadata shortcut unsafe: the split must not report a
    // metadata count. It proceeds to the rust pipeline instead, which fails on the dummy
    // split bytes of this test range rather than emitting synthetic count rows.
    PaimonRustTableReader reader;
    ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());

    SplitReadOptions options;
    options.current_range = make_rust_range();
    options.current_split_format = FileFormat::JNI;
    options.all_runtime_filters_applied = true;
    options.current_range.table_format_params.__set_table_level_row_count(5);
    options.conjuncts = VExprContextSPtrs {};
    options.conjuncts->push_back(VExprContext::create_shared(VLiteral::create_shared(
            std::make_shared<DataTypeInt32>(), Field::create_field<TYPE_INT>(1))));

    const auto status = reader.prepare_split(options);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(reader.current_split_uses_metadata_count());

    Block block = Block({ColumnWithTypeAndName(
            _projected_column.type->create_column(), _projected_column.type,
            _projected_column.name)});
    bool eos = false;
    const auto get_block_status = reader.get_block(&block, &eos);
    EXPECT_FALSE(get_block_status.ok());
    EXPECT_NE(get_block_status.to_string().find("paimon-rust reader is not initialized"),
              std::string::npos)
            << get_block_status;
}

TEST_F(PaimonRustTableReaderTest, FillsPartitionConstantsForMissingArrowColumns) {
    PaimonRustTableReader reader;
    const auto data_type = make_nullable(std::make_shared<DataTypeInt32>());
    const auto partition_type = make_nullable(std::make_shared<DataTypeString>());
    // The output block matches the projected columns exactly (get_block contract):
    // position 0 is the data column k (absent from the arrow batch -> default fill),
    // position 1 is the partition key dt (materialized from split metadata).
    reader.TEST_set_projected_columns(
            {make_column("k", std::make_shared<DataTypeInt32>()),
             make_column("dt", std::make_shared<DataTypeString>(), /*is_partition_key=*/true)});
    std::map<std::string, Field> partition_values;
    partition_values.emplace("dt", Field::create_field<TYPE_STRING>("2024-01-01"));
    reader.TEST_set_partition_values(std::move(partition_values));

    Block block = Block({ColumnWithTypeAndName(data_type->create_column(), data_type, "k"),
                         ColumnWithTypeAndName(partition_type->create_column(), partition_type,
                                               "dt")});
    const size_t rows = 4;
    ASSERT_TRUE(reader.TEST_fill_non_arrow_columns(&block, rows).ok());

    // The data column is absent from both the arrow batch and split metadata: filled
    // with defaults.
    EXPECT_EQ(block.get_by_position(0).column->size(), rows);

    // The partition position is a constant column with the split value broadcast
    // to every row.
    const auto& column_with_type = block.get_by_position(1);
    EXPECT_EQ(column_with_type.column->size(), rows);
    const auto* const_column = check_and_get_column<ColumnConst>(*column_with_type.column);
    ASSERT_NE(const_column, nullptr);
    EXPECT_EQ(const_column->size(), rows);
    const auto value_field = const_column->get_field();
    const auto& value = value_field.get<TYPE_STRING>();
    EXPECT_EQ(std::string(value.data(), value.size()), "2024-01-01");
}

TEST_F(PaimonRustTableReaderTest, MaterializesInSessionTimezone) {
    // TIMESTAMP_LTZ values materialize as session-local civil times: the
    // materialization timezone must come from the session, not a fixed default
    // (epoch 0 reads as 08:00 in a +08:00 session and 00:00 in UTC).
    const auto hour_of_epoch_zero = [](const cctz::time_zone& tz) {
        return tz.lookup(cctz::time_point<cctz::seconds>(std::chrono::seconds(0))).cs.hour();
    };

    PaimonRustTableReader reader;
    _runtime_state->set_timezone("+08:00");
    ASSERT_TRUE(init_reader_with_count(&reader, std::vector<GlobalIndex> {}).ok());
    EXPECT_EQ(hour_of_epoch_zero(reader.TEST_ctz()), 8);

    PaimonRustTableReader utc_reader;
    _runtime_state->set_timezone("UTC");
    ASSERT_TRUE(init_reader_with_count(&utc_reader, std::vector<GlobalIndex> {}).ok());
    EXPECT_EQ(hour_of_epoch_zero(utc_reader.TEST_ctz()), 0);
}

} // namespace doris::format::paimon
