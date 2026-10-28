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

#include <gen_cpp/DataSinks_types.h>

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/sink/writer/paimon/paimon_dpcm_codec.h"

namespace doris {

namespace {

// Records each serialize call. The serializer itself is a lambda capturing
// this by reference: build_dpcm_framed_commit_payloads takes its serializer
// argument by value, so a functor with mutable state would record into the
// copy and the test would see nothing.
struct CallRecorder {
    std::vector<std::pair<size_t, size_t>> calls;
};

} // namespace

TEST(PaimonDpcmCodecTest, EmptyProducesNoPayloads) {
    std::vector<TPaimonCommitMessage> out;
    size_t calls = 0;
    auto serializer = [&](size_t, size_t, std::string*) -> Status {
        ++calls;
        return Status::OK();
    };
    ASSERT_TRUE(build_dpcm_framed_commit_payloads(0, 12, serializer, &out).ok());
    EXPECT_TRUE(out.empty());
    EXPECT_EQ(0, calls);
}

TEST(PaimonDpcmCodecTest, FrameLayoutMatchesJavaCodec) {
    std::vector<TPaimonCommitMessage> out;
    CallRecorder recorder;
    auto serializer = [&](size_t start, size_t count, std::string* body) -> Status {
        recorder.calls.emplace_back(start, count);
        body->assign(10 * count, 'x');
        return Status::OK();
    };
    ASSERT_TRUE(build_dpcm_framed_commit_payloads(3, 12, serializer, &out).ok());
    ASSERT_EQ(1, out.size());

    const std::string& payload = out[0].payload;
    ASSERT_EQ(12 + 30, payload.size());
    EXPECT_EQ(0, payload.compare(0, 4, "DPCM"));
    EXPECT_EQ(12, read_i32_be(payload, 4));   // version from the frame header
    EXPECT_EQ(30, read_i32_be(payload, 8));    // body length
    EXPECT_EQ(std::string(30, 'x'), payload.substr(12));
    // One chunk covering all three messages.
    ASSERT_EQ(1, recorder.calls.size());
    EXPECT_EQ(0, recorder.calls[0].first);
    EXPECT_EQ(3, recorder.calls[0].second);
}

TEST(PaimonDpcmCodecTest, HalvesChunkSizeUntilEachChunkFits) {
    // 512 messages * 32 KiB = 16 MiB > 8 MiB budget -> the first chunk must
    // halve; 256 * 32 KiB = 8 MiB + header still > 8 MiB -> halve again; 128
    // chunks fit. Every halving retry re-serializes the same starting range,
    // so the call sequence starts (0,512) (0,256) (0,128) and then walks
    // forward in steps of 128.
    constexpr size_t kBytesPerMessage = 32 * 1024;
    std::vector<TPaimonCommitMessage> out;
    CallRecorder recorder;
    auto serializer = [&](size_t start, size_t count, std::string* body) -> Status {
        recorder.calls.emplace_back(start, count);
        body->assign(kBytesPerMessage * count, 'x');
        return Status::OK();
    };
    ASSERT_TRUE(build_dpcm_framed_commit_payloads(512, 12, serializer, &out).ok());
    EXPECT_EQ(4, out.size());

    ASSERT_GE(recorder.calls.size(), 3);
    // The halving retries re-serialize the same start.
    EXPECT_EQ(0, recorder.calls[0].first);
    EXPECT_EQ(512, recorder.calls[0].second);
    EXPECT_EQ(0, recorder.calls[1].first);
    EXPECT_EQ(256, recorder.calls[1].second);
    EXPECT_EQ(0, recorder.calls[2].first);
    EXPECT_EQ(128, recorder.calls[2].second);
    // The shipped chunks each carry 128 messages' bytes and fit the budget.
    for (const auto& msg : out) {
        EXPECT_LE(msg.payload.size(), DPCM_MAX_PAYLOAD_BYTES);
        EXPECT_EQ(kBytesPerMessage * 128, read_i32_be(msg.payload, 8));
    }
    // Chunk starts advance 0, 128, 256, 384 across the four shipped payloads.
    std::vector<size_t> shipped_starts;
    for (const auto& call : recorder.calls) {
        if (call.second == 128) {
            shipped_starts.push_back(call.first);
        }
    }
    EXPECT_EQ(4, shipped_starts.size());
    for (size_t i = 0; i < shipped_starts.size(); ++i) {
        EXPECT_EQ(i * 128, shipped_starts[i]);
    }
}

TEST(PaimonDpcmCodecTest, SingleOversizedMessageStillShips) {
    // A single message whose body alone exceeds the budget must not be
    // dropped or infinitely halved: chunk_size bottoms out at 1 and the
    // oversized payload ships (mirrors the legacy paimon-cpp behaviour of
    // sending it rather than failing the commit).
    std::vector<TPaimonCommitMessage> out;
    auto serializer = [](size_t, size_t count, std::string* body) -> Status {
        body->assign(DPCM_MAX_PAYLOAD_BYTES * count, 'x');
        return Status::OK();
    };
    ASSERT_TRUE(build_dpcm_framed_commit_payloads(1, 12, serializer, &out).ok());
    ASSERT_EQ(1, out.size());
    EXPECT_GT(out[0].payload.size(), DPCM_MAX_PAYLOAD_BYTES);
}

TEST(PaimonDpcmCodecTest, SerializerFailureAbortsWithoutPartialPayloads) {
    // The first attempt succeeds but is oversized, forcing a halving retry;
    // the retry fails. The FE committer must never see a prefix of a failed
    // prepare, so no payload may reach the output.
    std::vector<TPaimonCommitMessage> out;
    CallRecorder recorder;
    auto failing = [&](size_t start, size_t count, std::string* body) -> Status {
        recorder.calls.emplace_back(start, count);
        if (recorder.calls.size() >= 2) {
            return Status::InternalError("boom");
        }
        // Oversized: 16 MiB body forces the budget halving path.
        body->assign(16 * 1024 * 1024, 'x');
        return Status::OK();
    };
    ASSERT_FALSE(build_dpcm_framed_commit_payloads(4, 12, failing, &out).ok());
    EXPECT_TRUE(out.empty());
}

} // namespace doris
