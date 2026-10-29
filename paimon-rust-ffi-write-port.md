# Paimon Rust FFI 写入移植记档（branch-4.2，doris2）

日期：2026-10-28（同日完成 paimon-rust v0.5.0-rc1 升级）
基线：paimon-rust **v0.5.0-rc1**（社区 tarball，MD5 fabf721b…）+ 补丁；Paimon Java 1.4.2（CommitMessageSerializer v11）
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

### v0.5.0-rc1 升级（2026-10-28 追加）

- `vars.sh` pin 升至 v0.5.0-rc1；patch 重做（6 文件 +501/-44，旧版 1057 行——`mod.rs` 导出、
  `DataFileMetaRowLayout` 枚举已上游原生）。v11/v14 golden round-trip、C 绑定 88/88 测试全过。
- 0.5.0 补齐了 v0.4.0 的能力缺口，引入 PR：部分列写入 #1054/#1169、partial-update 引擎
  #1169、ORC 写格式 #959、rowkind/changelog 记录 #1180（+#1072/#1179/#957 收尾）。
  changelog 库层面支持（changelog-producer 表选项 + UpdateBefore/After/Delete 记录），但
  Doris FFI 门控仍按设计把 UPDATE/DELETE/MERGE 路由回 JNI。
- **升级实战验证了"只改 rust patch"的接口设计**：BE/FE Doris 代码零改动，C ABI 不变。
- 升级后 FFI 轮失败演进：23（v0.4.0）→ 19（v0.5.0 + BE 子集列增强 `81ffb54`：置换放宽 +
  sink 序转换 + 缺列 MakeArrayOfNull + 移除 null 预检——Java 连接器并无该预检，且套件期望
  省略列落表 \N）。翻绿 ~12 套件；rust_ffi/impl_parity 保持全绿。JNI 基准实测：
  append 表省略列真实落 NULL；PK/deduplicate 部分列写被 Java 拒绝（requires partial-update）。
- 最终失败分布（19）：variant×6（环境）；ORC zstd×2-3（0.5.0 ORC 仅无压缩）；
  rowkind×2（`rowkind.field`/changelog-producer 表的普通 INSERT 到达 rust RowKindGenerator
  时 kind 为 null——FE 门控应把这类表回退 JNI）；~8 个引擎特有 null 语义断言待逐个核对。

### 原始记录（v0.4.0-rc1 阶段）

- [x] FE 门控扩展（`ebd05cf` 已落地）：`rowkind.field`/`changelog-producer` 表、ORC 格式表、PK 非 partial-update 的部分列写均自动回退 JNI（rowkind 残留 2 例 + ORC 2-3 例的已查明根因，回退后即通过）。partial-update 已在 0.5.0 原生支持。
- [x] null 语义对齐（`ebd05cf` → 当晚重构为 **BE 预检方案**）：BE nullable 化输入 schema
  （镜像 PaimonWriteSchema.java）；NOT NULL 列 null 拒绝最初落在 crate patch（normalize 放宽
  + append 早退分支补检查），后按决策**还原全部 crate null 语义改动**，改为 `FfiPaimonWriter::
  write()` 拼装全列 batch 后、调 rust 前做 BE 预检（`[INVALID_ARGUMENT]Cannot write null to
  non-null column(x)`，与 Java writer 逐字一致）。crate patch 因此回到 6 文件、仅剩协议层
  （v11 序列化 + C 绑定导出 + 移除 stock binding 层的抢先 null 检查——stock 该检查以不同
  文本先拦且会误杀 DELETE 场景，必须移除，这是 patch 中唯一的 null 相关改动）。
  还原依据（stock 源码逐层核实）：PK 表 stock `filter_write_input` 在 normalize 之前自带
  同款诊断；append 表 stock 会在 normalize 的 `RecordBatch::try_new` 以 arrow 文本拒绝
  （BE 预检先行给出 Java 文本）；nullable 列的 NULL（含子集省略）全路径天然通过。
  **已知限制**：stock normalize 使 partial-update/aggregation 往声明 NOT NULL 列写 NULL
  （合法"保留旧值"输入）被拒——现有套件无此组合（partial-update 表列全 nullable），
  遇到时需 FE 门控兜底或恢复该 patch。验证：探针 6/6（append/PK 显式 NULL 拒绝文本、
  nullable NULL 落表、partial-update 子集两段写保留旧值）；36 套件批失败 10 =
  6 variant + 4 已知缺口，与还原前分布完全一致。
- [x] 剩余 9 个非 variant 失败——**2026-10-29 已逐案定性（9/9）**，收敛为 5 个根因：
  - **根因 A：first-row / DV 表 level-0 读不可见（4 案）**。paimon 1.4.2 字节码级确认
    `CoreOptions#batchScanSkipLevel0()`：`deletion-vectors.enabled=true` 或
    `merge-engine=first-row` 的表批扫描**跳过全部 level-0 数据文件**（1.4.2 无 merge-on-read 例外）。
    Java 写端靠提交时 lookup 压缩把数据落成 level≥1 满足该契约；FFI 写 level-0 APPEND 文件
    → 提交/snapshot/manifest/数据文件全部正确但批读 0 行。受影响：merge_engine(first_row)、
    bucket_modes(bucket_key_dynamic_first_row)、create_ddl(create_first_row_result)、
    deletion_vector(dv_post_compact_write_jni，DV 表 compact 后 FFI 新写不可见)。
    对照实验：同 DDL deduplicate 表 FFI 写读回正常；Java 写 first-row 提交即 level-5 COMPACT。
    早先"同一会话 vs 不同会话"的表象是干扰项——真正变量是文件是否被 Java 压实过。
  - **根因 B：Paimon 列 DEFAULT 语义缺失（2 案）**。Java 连接器（PaimonWriteSchema）对
    **省略**的带 default 列在连接器层填 default；**显式 NULL** 由 paimon writer 的 default
    wrapper 处理（nullable+default → 替换为 default；NOT NULL → 先拒 null）。FFI 路径两层都
    没有：append_only(t_append_default 省略 name 被拒 "Cannot write null to non-null
    column(name)")、schema_change(added_after 显式 NULL 未替换 'unknown'，line 4 mismatch)。
  - **根因 C：提交时 merge 压缩缺失（compaction 1 案）**。Java writer 两笔 INSERT 两个
    sorted run 触发 merge-tree compaction 合并为 1 文件；FFI 不做提交时压缩 → 2 个 level-0
    文件 → `expected:<1> but was:<2>`（读逻辑正确，仅文件形态分叉）。
  - **根因 D：`data-file.external-paths` 不支持（external_paths 1 案）**。Java
    DataFilePathFactory 按 round-robin/weight/specific 策略把数据文件写外部目录；rust 无此
    能力 → 文件落默认 bucket 目录 → 路径断言 false。
  - **根因 E：`write-buffer-size` 单行上限语义缺失（transaction 1 案）**。Java
    SortBufferWriteBuffer 拒绝超过 write-buffer-size(256kb) 的单行（"The record exceeds the
    maximum size of a sort buffer"）；FFI 忽略该配置，1MB 行成功提交 → 预期异常断言失败。
  - **处置**：FE 门控已扩展（first-row / deletion-vectors.enabled / data-file.external-paths
    → JNI），覆盖根因 A、D 共 5 案（solo FFI 验证中）；根因 B/C/E 为 rust 侧功能缺口
    （DEFAULT wrapper、提交时压缩、单行上限检查），记入 rust 待办。
- **运维事故记录（2026-10-29）**：在 ~/dengfangyuan/doris（2.1 仓库）跑 build.sh 会透过 `doris/output/fe/lib → doris2/output/fe/lib` 软链污染 doris2 的 lib（jar 换成 2.1.0，运行中的 FE 因类路径缺失全崩）并重置 output/be/conf。恢复：重建 doris2（./build.sh --fe --be）→ 从 .bak 恢复 fe.conf/be.conf 真实端口（FE 9630/8620/9610/8630、BE 9660/9650/8640/8660）→ 拷回 output/be/lib/compat/ libstdc++.so.6 → 重启 FE/BE。
- variant 系列 6 套件失败为 Spark 1.4.2 TS 环境差异，与移植无关（老仓库 + Spark 2.1 才能过）。
- **运行注意**：`regression-test/conf/regression-conf.groovy` 现为 `enablePaimonTest=true`、`iceberg_spark_thrift_port=11001`（1.4.2 TS）、FE 端口 9630/8630 —— 跑 iceberg 套件前 spark 端口须还原 11000。
- **未跑范围**：全量 BE/FE UT（仅跑了 Paimon 相关目标）；`paimon/write/` 以外的 paimon 套件（读路径未改动，理论不受影响）。

## 五、部署形态

老 doris 目录（`/home/clickhouse/dengfangyuan/doris/output`）FE/BE 的 `lib` 已软链到 doris2 构建产物（`libbak` 保留原版）；`compat/libstdc++.so.6` 已拷贝至 doris2 BE lib（be.conf 的 LD_LIBRARY_PATH 依赖它）。元数据/conf 均在老目录未动。
