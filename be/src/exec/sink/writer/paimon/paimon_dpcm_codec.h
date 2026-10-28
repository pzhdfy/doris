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

#include <gen_cpp/DataSinks_types.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"

namespace doris {

/// DPCM framing and adaptive chunking for Paimon commit messages, used by the
/// native Rust write backend (ffi_paimon_write_backend.cpp). Byte-identical to
/// the same algorithm hand-inlined in the JNI writer stacks -- the Java
/// connector's PaimonCommitCodec and the legacy paimon-cpp writer -- so the FE
/// committer deserializes every backend's payloads identically.
///
/// A chunk payload is: magic "DPCM" | serializer version (BE i32) | body
/// length (BE i32) | body, where the body is the serialized message list for
/// the chunk's range. Chunking starts at 512 messages; a body whose framed
/// size exceeds the 8 MiB budget halves the chunk size and re-serializes the
/// same range.
///
/// `serializer_version` is supplied by the caller from
/// `paimon_commit_message_serializer_version()` rather than compiled in here:
/// the version must name the Paimon Java release the FE decodes with, so it
/// has to follow the paimon-rust build instead of this translation unit.
constexpr size_t DPCM_HEADER_BYTES = 12;
constexpr size_t DPCM_MAX_PAYLOAD_BYTES = 8 * 1024 * 1024;
constexpr size_t DPCM_INITIAL_CHUNK_SIZE = 512;

inline void append_i32_be(std::string* out, int32_t value) {
    const uint32_t u = static_cast<uint32_t>(value);
    out->push_back(static_cast<char>((u >> 24) & 0xFF));
    out->push_back(static_cast<char>((u >> 16) & 0xFF));
    out->push_back(static_cast<char>((u >> 8) & 0xFF));
    out->push_back(static_cast<char>(u & 0xFF));
}

inline int32_t read_i32_be(const std::string& payload, size_t offset) {
    return static_cast<int32_t>((static_cast<uint32_t>(static_cast<uint8_t>(payload[offset]))
                                 << 24) |
                                (static_cast<uint32_t>(static_cast<uint8_t>(payload[offset + 1]))
                                 << 16) |
                                (static_cast<uint32_t>(static_cast<uint8_t>(payload[offset + 2]))
                                 << 8) |
                                static_cast<uint32_t>(static_cast<uint8_t>(payload[offset + 3])));
}

/// Serialize [0, total_messages) into DPCM-framed commit payload chunks.
///
/// `serialize_range(start, count, &body)` must produce the serializer's list
/// body for messages [start, start + count). On failure it returns a non-ok
/// Status, which aborts the whole prepare with that error -- a partial payload
/// set must never reach the FE committer.
template <typename SerializeRange>
Status build_dpcm_framed_commit_payloads(size_t total_messages, int32_t serializer_version,
                                         SerializeRange serialize_range,
                                         std::vector<TPaimonCommitMessage>* out) {
    out->clear();
    if (total_messages == 0) {
        return Status::OK();
    }
    size_t chunk_size = DPCM_INITIAL_CHUNK_SIZE;
    size_t i = 0;
    while (i < total_messages) {
        const size_t end = std::min(i + chunk_size, total_messages);
        std::string body;
        RETURN_IF_ERROR(serialize_range(i, end - i, &body));
        if (body.size() + DPCM_HEADER_BYTES > DPCM_MAX_PAYLOAD_BYTES && chunk_size > 1) {
            chunk_size = std::max<size_t>(1, chunk_size / 2);
            continue;
        }

        TPaimonCommitMessage msg;
        std::string& payload = msg.payload;
        payload.reserve(DPCM_HEADER_BYTES + body.size());
        payload.append("DPCM", 4);
        append_i32_be(&payload, serializer_version);
        append_i32_be(&payload, static_cast<int32_t>(body.size()));
        payload.append(body);
        msg.__isset.payload = true;
        out->emplace_back(std::move(msg));
        i = end;
    }
    return Status::OK();
}

} // namespace doris
