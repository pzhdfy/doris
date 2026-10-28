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

#include "exec/sink/writer/paimon/paimon_write_backend.h"

#include <arrow/type.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "exec/sink/writer/paimon/ffi_paimon_write_backend.h"
#include "exec/sink/writer/paimon/jni_paimon_write_backend.h"

namespace doris {

TEST(PaimonWriteBackendFactoryTest, SelectBackendType) {
    TPaimonTableSink sink;
    EXPECT_EQ(PaimonBackendType::JNI, PaimonWriteBackendFactory::select_backend_type(sink));

    sink.__set_backend_type(TPaimonWriteBackendType::FFI);
    EXPECT_EQ(PaimonBackendType::FFI, PaimonWriteBackendFactory::select_backend_type(sink));
}

TEST(PaimonWriteBackendFactoryTest, ValidateFfiColumnPermutation) {
    // A three-field table schema: the permutation must name one distinct block
    // position per field. Guards the handoff in FfiPaimonWriteBackend::
    // create_writer(): a moved-out backend member left later writers with an
    // empty permutation, which must fail closed instead of mis-converting rows.
    auto arrow_schema = arrow::schema({arrow::field("id", arrow::int32()),
                                       arrow::field("name", arrow::utf8()),
                                       arrow::field("score", arrow::float64())});

    EXPECT_TRUE(FfiPaimonWriteBackend::validate_permutation(*arrow_schema, {0, 1, 2}).ok());
    // Out-of-order (INSERT INTO t (b, a)) is a valid mapping.
    EXPECT_TRUE(FfiPaimonWriteBackend::validate_permutation(*arrow_schema, {2, 0, 1}).ok());

    // Empty: the moved-out member state.
    auto status = FfiPaimonWriteBackend::validate_permutation(*arrow_schema, {});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("covers 0 of 3"));

    // Short permutation.
    status = FfiPaimonWriteBackend::validate_permutation(*arrow_schema, {0, 1});
    EXPECT_FALSE(status.ok());

    // Long permutation.
    status = FfiPaimonWriteBackend::validate_permutation(*arrow_schema, {0, 1, 2, 0});
    EXPECT_FALSE(status.ok());

    // Repeated block position: two table fields cannot share one column.
    status = FfiPaimonWriteBackend::validate_permutation(*arrow_schema, {0, 0, 1});
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("repeats a block position"));
}

TEST(PaimonWriteBackendFactoryTest, BuildFfiColumnPermutation) {
    // Table fields keep paimon's declared case while sink columns follow the
    // INSERT's list, lowercased by Doris: id / Name / score.
    auto arrow_schema = arrow::schema({arrow::field("id", arrow::int32()),
                                       arrow::field("Name", arrow::utf8()),
                                       arrow::field("score", arrow::float64())});

    std::vector<size_t> permutation;
    // Table order == sink order.
    ASSERT_TRUE(FfiPaimonWriteBackend::build_column_permutation(
                        *arrow_schema, {"id", "name", "score"}, &permutation)
                        .ok());
    EXPECT_EQ((std::vector<size_t> {0, 1, 2}), permutation);

    // INSERT INTO t (score, id, name): table-order field i reads from the
    // corresponding sink position, case-insensitively for Name.
    ASSERT_TRUE(FfiPaimonWriteBackend::build_column_permutation(
                        *arrow_schema, {"score", "id", "name"}, &permutation)
                        .ok());
    EXPECT_EQ((std::vector<size_t> {1, 2, 0}), permutation);

    // A table field no sink column covers fails closed, named in the error.
    auto status = FfiPaimonWriteBackend::build_column_permutation(*arrow_schema,
                                                                  {"id", "name"}, &permutation);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("table field 'score'"));

    // Empty sink columns fail the first field.
    status = FfiPaimonWriteBackend::build_column_permutation(*arrow_schema, {}, &permutation);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("table field 'id'"));

    // An exact match wins over a case-insensitive one: two sink columns that
    // both fold onto 'Name' must map to the exact spelling's position.
    ASSERT_TRUE(FfiPaimonWriteBackend::build_column_permutation(
                        *arrow_schema, {"id", "name", "Name", "score"}, &permutation)
                        .ok());
    EXPECT_EQ((std::vector<size_t> {0, 2, 3}), permutation);
}

TEST(JniPaimonWriteBackendTest, OpenAbiAndWriteModes) {
    EXPECT_STREQ(
            "(Ljava/lang/String;Ljava/util/Map;[Ljava/lang/String;JLjava/lang/String;ZZLjava/lang/"
            "String;JJJ)V",
            PAIMON_JNI_WRITER_OPEN_SIGNATURE);

    auto append = PaimonJniWriterOpenMode::from_write_mode(TPaimonWriteMode::APPEND);
    EXPECT_FALSE(append.overwrite);
    EXPECT_FALSE(append.changelog);

    auto overwrite = PaimonJniWriterOpenMode::from_write_mode(TPaimonWriteMode::OVERWRITE);
    EXPECT_TRUE(overwrite.overwrite);
    EXPECT_FALSE(overwrite.changelog);

    auto changelog = PaimonJniWriterOpenMode::from_write_mode(TPaimonWriteMode::CHANGELOG);
    EXPECT_FALSE(changelog.overwrite);
    EXPECT_TRUE(changelog.changelog);
}

} // namespace doris
