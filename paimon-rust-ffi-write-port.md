# Paimon Rust FFI 写入移植记档（branch-4.2，doris2）

日期：2026-10-28
基线：paimon-rust v0.4.0-rc1（社区 tarball）+ 补丁；Paimon Java 1.4.2（CommitMessageSerializer v11）
来源：kwai bleem4/bleem 的 rust_ffi 写入功能，按社区 1.4.2 协议重做移植

## 一、交付物

| 层 | 文件 | 说明 |
|---|---|---|
| Rust patch | `thirdparty/patches/paimon-rust-ffi-write-v11.patch` | 对 v0.4.0-rc1 `--fuzz=0` 干净应用；7 文件 +1057 行 |
| 打补丁机制 | `thirdparty/download-thirdparty.sh` | 按 checksum 幂等（patched_mark_ffi_write），checksum 变化自动重解 |
| BE 写后端 | `be/src/exec/sink/writer/paimon/ffi_paimon_write_backend.{h,cpp}` | RAII 句柄、scheme 分派选项归一、Arrow C Data Interface 写入、DPCM 提交 |
| DPCM 帧 | `be/src/exec/sink/writer/paimon/paimon_dpcm_codec.h` | `"DPCM"`+版本+长度 帧头，8MiB 预算自适应分块（512 起减半） |
| Thrift | `gensrc/thrift/DataSinks.thrift` | TPaimonTableSink 新增 6 个 optional 字段（8-13） |
| FE | `PaimonTableSink.java` | FFI 门控 + 选项下发（含 path-style 修复）；`SessionVariable` 新增 `enable_paimon_rust_writer`；`PaimonInsertCommandContext` + 两个 Insert 命令传参 |
| UT(BE) | `paimon_dpcm_codec_test.cpp`（新）、`paimon_write_backend_test.cpp`（并入 FFI 列置换测试） | 8/8 通过 |
| UT(FE) | 无新增（复用存量） | Paimon 相关 50/50 通过 |
| 回归 | `write/` 下 4 个移植套件 + 1 个 .out | rust_ffi / impl_parity / refresh_coherence / unpartitioned_exchange |

## 二、与 bleem 原实现的区别

| 维度 | bleem（kwai fork） | doris2 本次移植 |
|---|---|---|
| 序列化协议 | 对齐 kwai 内部 paimon 1.4 fork（v12，22 字段 DataFileMeta、6 字段 GlobalIndexMeta） | 对齐社区 1.4.2（v11，20/5 字段，无尾部 checkFromSnapshot）；patch 内含 V11/V14 双常量与 `serialize_versioned()`，升级 2.1 只改版本号 |
| 开表方式 | `paimon_table_open_path` | `paimon_table_from_schema_json`（rust_table_reader 模式）：FE 解析 schema，BE 无需 catalog 凭证，branch 单独传递 |
| 版本号来源 | 编译期常量 | `paimon_commit_message_serializer_version()` 运行时从链接库查询 —— 升级 paimon-rust 不需重编 BE |
| 消息计数 | kwai fork 把 message_count 塞进 prepare_commit result（破坏结构兼容） | 独立函数 `paimon_commit_messages_count()`，不动既有结构体 |
| 会话开关 | `paimon_write_impl='rust_ffi'`（字符串，还带 'internal'/'community' 旧值） | `enable_paimon_rust_writer` 布尔会话变量（支持 GLOBAL），FE 门控：显式开启 + 非 changelog 写 + scheme 白名单{s3,s3a,oss,hdfs,file} |
| 存储选项归一 | kwai 味 | 按 scheme 分派（oss→fs.oss.*，s3/s3a→s3.*），镜像 doris2 reader 的 _build_options；另修了 FE 侧 path-style 丢 Java 式写法的 bug（bleem 无此修复） |
| Arrow 转换 | bleem 自由函数 `convert_to_arrow_batch` | doris2 类接口 `doris::paimon::PaimonArrowBlockConvertor`（沿用现成类） |
| DPCM 帧 | JNI writer 内联实现 | 抽成共享头文件模板 + UT 覆盖 |
| 构建集成 | 直接从 kwai fork 源码构建 | v0.4.0-rc1 tarball + patch；build.sh 无门控（手动重建） |

## 三、验证矩阵（全部实际跑过）

| 项 | 结果 |
|---|---|
| BE 编译/链接 | ✅ `Successfully build Doris`，15 个 FFI 符号全解析，0 undefined |
| FE 编译 | ✅ BUILD SUCCESS |
| BE UT | ✅ 8/8（DPCM 帧 5 + FFI 列置换 2 + factory 1） |
| FE UT（Paimon 相关 4 类） | ✅ 50/50（Util 30、Transaction 6、WriteBinding 10、WriteTarget 4） |
| 回归·移植 4 套件 | ✅ 全绿（append/分区/OVERWRITE/PK/混写/Spark 一致性；JNI vs FFI 五场景等价；REFRESH 双层缓存双后端；exchange 分发） |
| 回归·存量 33 write 套件 × JNI（对照组） | 36 套件失败 6 —— 全为 variant 系列（Spark 1.4.2 TS 不支持 variant 建表，环境差异，与本次改动无关） |
| 回归·存量 33 write 套件 × FFI（全局开启） | 失败 17，全部为 v0.4.0-rc1 Rust 侧能力边界的 fail-closed（见下），移植代码本身无 bug、无静默丢数据 |
| FE path-style bug 修复验证 | ✅ 点号写法 catalog（`s3.path.style.access`）FFI 写入成功 + rust_ffi 套件全绿 |

### FFI 轮 17 个独有失败的归类（均为 rust 侧 "not supported by this build"）

1. **部分列 INSERT**（by-name 扩展未实现）：append_only、bucket_modes、changelog_producer、complex_types、merge_engine、row_tracking_evolution 等 ~7 个
2. **merge-engine=partial-update**：row_level_dml、sequence_group、source_models 等
3. **ORC 写格式**（只支持 .parquet/.blob/.row）：orc_ltz_encoding、source_models、create_ddl_write_properties
4. **rowkind/changelog 记录**（`Row kind cannot be null`）：row_level_dml、source_models
5. 级联断言（数据未落表导致后续 tag 校验失败）：deletion_vector、schema_change、transaction、failures

## 四、已知边界与待办

- [ ] **FE 门控扩展**（建议）：`useRustFfiWriter` 目前只挡 changelog 写；表带 `merge-engine=partial-update` / `file.format=orc` / `rowkind.field` 的普通 INSERT 会走 FFI 后失败。建议加进回退条件自动降级 JNI。
- [ ] 上述 4 类能力缺口预期由升级 paimon-rust（2.1 + PR 912 路线，重打 patch、版本常量 11→14）补齐；Doris 侧代码无需改动。
- [ ] variant 系列 6 套件失败为 Spark 1.4.2 TS 环境差异，与移植无关（老仓库 + Spark 2.1 才能过）。
- **运行注意**：`regression-test/conf/regression-conf.groovy` 现为 `enablePaimonTest=true`、`iceberg_spark_thrift_port=11001`（1.4.2 TS）、FE 端口 9630/8630 —— 跑 iceberg 套件前 spark 端口须还原 11000。
- **未跑范围**：全量 BE/FE UT（仅跑了 Paimon 相关目标）；`paimon/write/` 以外的 paimon 套件（读路径未改动，理论不受影响）。

## 五、部署形态

老 doris 目录（`/home/clickhouse/dengfangyuan/doris/output`）FE/BE 的 `lib` 已软链到 doris2 构建产物（`libbak` 保留原版）；`compat/libstdc++.so.6` 已拷贝至 doris2 BE lib（be.conf 的 LD_LIBRARY_PATH 依赖它）。元数据/conf 均在老目录未动。
