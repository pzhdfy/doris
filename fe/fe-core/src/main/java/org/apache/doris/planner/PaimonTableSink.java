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

package org.apache.doris.planner;

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.credentials.CredentialUtils;
import org.apache.doris.datasource.credentials.VendedCredentialsFactory;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonTransaction;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.paimon.PaimonWriteBinding;
import org.apache.doris.datasource.paimon.PaimonWriteTarget;
import org.apache.doris.datasource.property.metastore.PaimonJdbcMetaStoreProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertCommandContext;
import org.apache.doris.nereids.trees.plans.commands.insert.PaimonInsertCommandContext;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TPaimonTableSink;
import org.apache.doris.thrift.TPaimonWriteBackendType;
import org.apache.doris.thrift.TPaimonWriteMode;

import com.google.common.base.Preconditions;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.table.FileStoreTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Paimon table sink.
 *
 * Generates TPaimonTableSink payload consumed by BE, including serialized table
 * metadata, Hadoop authentication config, transaction identity, write mode,
 * and sink column names.
 *
 * The upstream Exchange may establish concurrent HASH_FIXED writer ownership;
 * partition and bucket handling inside each writer remains delegated to the SDK.
 */
public class PaimonTableSink extends BaseExternalTableDataSink {
    public static final String ROW_KIND_COLUMN = "__DORIS_PAIMON_ROW_KIND__";

    /// Warehouse schemes the pinned paimon-rust FileIO can serve. Must stay in
    /// sync with PaimonScanNode.RUST_VERIFIED_LOCATION_SCHEMES: the same
    /// storage dispatcher and option families are involved on both sides.
    private static final Set<String> RUST_WRITER_VERIFIED_SCHEMES =
            new HashSet<>(Arrays.asList("s3", "s3a", "oss", "hdfs", "file"));
    private final PaimonExternalTable targetTable;
    private final PaimonWriteTarget writeTarget;
    private final DMLCommandType dmlCommandType;
    private List<Expr> outputExprs;
    private List<Column> cols;

    private static final HashSet<TFileFormatType> supportedTypes = new HashSet<TFileFormatType>() {{
            add(TFileFormatType.FORMAT_ORC);
            add(TFileFormatType.FORMAT_PARQUET);
        }};

    public PaimonTableSink(PaimonWriteTarget writeTarget, DMLCommandType dmlCommandType) {
        super();
        this.writeTarget = writeTarget;
        this.targetTable = writeTarget.getDorisTable();
        this.dmlCommandType = dmlCommandType;
    }

    public void setCols(List<Column> cols) {
        this.cols = cols;
    }

    public void setOutputExprs(List<Expr> outputExprs) {
        this.outputExprs = outputExprs;
    }

    @Override
    protected Set<TFileFormatType> supportedFileFormatTypes() {
        return supportedTypes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("PAIMON TABLE SINK\n");
        if (explainLevel == TExplainLevel.BRIEF) {
            return strBuilder.toString();
        }
        strBuilder.append(prefix).append("  table: ").append(targetTable.getName()).append("\n");
        return strBuilder.toString();
    }

    @Override
    public void bindDataSink(Optional<InsertCommandContext> insertCtx) throws AnalysisException {
        TPaimonTableSink tSink = new TPaimonTableSink();
        PaimonInsertCommandContext ctx = (PaimonInsertCommandContext) insertCtx.get();
        Preconditions.checkState(ctx.getTxnId() > 0,
                "Paimon transaction must begin before sink binding");

        PaimonTransaction transaction;
        PaimonWriteBinding binding;
        try {
            transaction = (PaimonTransaction) targetTable.getCatalog()
                    .getTransactionManager().getTransaction(ctx.getTxnId());
            binding = PaimonWriteBinding.create(writeTarget, ctx);
        } catch (AnalysisException e) {
            throw e;
        } catch (UserException e) {
            throw new AnalysisException("Failed to bind Paimon write transaction: "
                    + e.getMessage(), e);
        }
        transaction.bind(binding);

        tSink.setTransactionId(ctx.getTxnId());
        tSink.setCommitUser(ctx.getCommitUser());

        // Thrift column_names is the single column-order protocol shared by BE
        // Arrow conversion and the Java writer schema.
        List<String> outputColumnNames = outputColumnNames();

        // FE owns table metadata resolution. BE and the JNI writer consume this
        // exact table instance instead of loading catalog metadata independently.
        tSink.setSerializedTable(binding.getSerializedTable());

        // Backend selection: the native Rust writer needs a payload the C ABI
        // can consume natively (table path + resolved TableSchema JSON + storage
        // options) instead of the Java serialized table. The serialized table
        // stays in the payload either way, because the FE committer
        // (PaimonTransaction) still decodes it through the Java SDK -- only the
        // BE-side writer implementation switches.
        //
        // Every condition here is a fail-closed gate: when any of them does not
        // hold we keep the JNI writer, which the JNI path has always handled.
        if (useRustFfiWriter(ctx)) {
            tSink.setBackendType(TPaimonWriteBackendType.FFI);
            tSink.setTableLocation(getTableLocation());
            tSink.setTableSchemaJson(getTableSchemaJson());
            tSink.setDbName(targetTable.getDbName());
            tSink.setTableName(targetTable.getName());
            String branch = getBranch();
            if (branch != null && !branch.isEmpty()) {
                tSink.setBranch(branch);
            }
            tSink.setTableOptions(buildRustWriterOptions());
        } else {
            tSink.setBackendType(TPaimonWriteBackendType.JNI);
        }

        if (isChangelogWrite()) {
            tSink.setWriteMode(TPaimonWriteMode.CHANGELOG);
        } else if (ctx.isOverwrite()) {
            tSink.setWriteMode(TPaimonWriteMode.OVERWRITE);
        } else {
            tSink.setWriteMode(TPaimonWriteMode.APPEND);
        }

        tSink.setHadoopConfig(binding.getHadoopConfig());

        tSink.setColumnNames(outputColumnNames);

        tDataSink = new TDataSink(TDataSinkType.PAIMON_TABLE_SINK);
        tDataSink.setPaimonTableSink(tSink);
    }

    /// Storage/credential options for the native writer's FileIO layer,
    /// mirroring what PaimonScanNode sends the rust reader: vended-credential
    /// storage properties flattened to backend keys, plus the JDBC-metastore
    /// paimon options.
    private Map<String, String> buildRustWriterOptions() {
        Map<String, String> options = new HashMap<>();
        if (!(targetTable.getCatalog() instanceof PaimonExternalCatalog)) {
            return options;
        }
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) targetTable.getCatalog();
        Map<StorageProperties.Type, StorageProperties> storageMap =
                VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(
                        catalog.getCatalogProperty().getMetastoreProperties(),
                        catalog.getCatalogProperty().getStoragePropertiesMap(),
                        writeTarget.getTable());
        options.putAll(CredentialUtils.getBackendPropertiesFromStorageMap(storageMap));
        // Path-style resolution for the native writer. CredentialUtils emits only
        // its fixed backend key set and defaults use_path_style to "false", but
        // the Java SDK (the JNI writer's S3FileIO) additionally reads the
        // s3.path.style.access / fs.s3a.path.style.access spellings straight
        // from the catalog properties — so a MinIO catalog written the Java way
        // has no effect on the native writer, which then builds virtual-host
        // URLs and fails. Forward any recognized spelling from the raw catalog
        // properties; an already-true backend value keeps winning.
        for (String key : Arrays.asList("use_path_style", "s3.path-style-access",
                "s3.path.style.access", "fs.s3a.path.style.access")) {
            String value = catalog.getCatalogProperty().getProperties().get(key);
            if (Boolean.parseBoolean(value)
                    && !Boolean.parseBoolean(options.get("use_path_style"))) {
                options.put("use_path_style", "true");
            }
        }
        if (catalog.getCatalogProperty().getMetastoreProperties()
                instanceof PaimonJdbcMetaStoreProperties) {
            options.putAll(((PaimonJdbcMetaStoreProperties)
                    catalog.getCatalogProperty().getMetastoreProperties())
                    .getBackendPaimonOptions());
        }
        return options;
    }

    /// Whether this write can go through the native (paimon-rust) writer.
    ///
    /// The native writer has no changelog producer and no bucket/partition
    /// planning of its own, so every case the Rust ABI cannot express has to
    /// stay on JNI. The reader applies the same style of pre-flight gating
    /// (PaimonScanNode) rather than failing at BE open time.
    private boolean useRustFfiWriter(PaimonInsertCommandContext ctx) {
        // Explicit opt-in only: the JNI writer is the default everywhere.
        if (!Boolean.TRUE.equals(ctx.getEnableRustWriter())) {
            return false;
        }
        // The Rust C ABI only exposes with_overwrite(); an UPDATE/DELETE/MERGE
        // produces a changelog write, which must go through JNI.
        if (isChangelogWrite()) {
            return false;
        }
        FileStoreTable table = writeTarget.getTable();
        if (table == null) {
            return false;
        }
        // Table configurations the Rust write path cannot express. Row kinds
        // reach the Rust writer only through the changelog write already routed
        // to JNI above, so a plain INSERT into a rowkind.field or
        // changelog-producer table arrives at the Rust RowKindGenerator without
        // a kind value and fails; ORC tables default to a compressed write the
        // pinned paimon-rust ORC writer (uncompressed-only) rejects; and a
        // subset-column INSERT into a primary-key table without
        // merge-engine=partial-update is rejected by the Java writer with its
        // engine diagnostic -- route there rather than diverging. Fall back to
        // JNI in every case: the JNI writer is the reference behavior.
        Map<String, String> options = table.schema().options();
        if (!options.getOrDefault("rowkind.field", "").isEmpty()
                || !"none".equalsIgnoreCase(options.getOrDefault("changelog-producer", "none"))) {
            return false;
        }
        if ("orc".equalsIgnoreCase(options.getOrDefault("file.format", "parquet"))) {
            return false;
        }
        // Batch scans skip level-0 files on these tables (paimon
        // CoreOptions#batchScanSkipLevel0). The Java writer satisfies that
        // contract through commit-time lookup compaction; the native writer
        // publishes plain level-0 files, which then become invisible to every
        // batch reader, so route to the reference writer instead.
        if ("first-row".equalsIgnoreCase(options.getOrDefault("merge-engine", "deduplicate"))
                || Boolean.parseBoolean(options.getOrDefault("deletion-vectors.enabled", "false"))) {
            return false;
        }
        // External data-file directories with a rolling strategy are resolved
        // by the Java DataFilePathFactory; the pinned paimon-rust writer has no
        // support for them and writes into the default bucket directory.
        if (!options.getOrDefault("data-file.external-paths", "").isEmpty()) {
            return false;
        }
        if (!table.schema().primaryKeys().isEmpty() && !cols.isEmpty()
                && !"partial-update".equalsIgnoreCase(
                        options.getOrDefault("merge-engine", "deduplicate"))) {
            Set<String> fields = table.schema().fieldNames().stream()
                    .map(name -> name.toLowerCase(Locale.ROOT))
                    .collect(java.util.stream.Collectors.toSet());
            boolean subset = cols.stream()
                    .anyMatch(column -> !fields.contains(column.getName().toLowerCase(Locale.ROOT)))
                    || cols.size() < fields.size();
            if (subset) {
                return false;
            }
        }
        // Storage schemes the pinned paimon-rust FileIO cannot serve. The
        // reader gates on the same whitelist; a warehouse outside it would fail
        // at open with a misleading auth error instead of falling back.
        String location = table.location().toString();
        int schemeEnd = location.indexOf("://");
        if (schemeEnd <= 0) {
            // Scheme-less (bare path / file) warehouses are served natively.
            return true;
        }
        String scheme = location.substring(0, schemeEnd).toLowerCase(Locale.ROOT);
        return RUST_WRITER_VERIFIED_SCHEMES.contains(scheme);
    }

    private String getTableLocation() {
        FileStoreTable table = writeTarget.getTable();
        return table == null ? null : table.location().toString();
    }

    /// The FE-resolved TableSchema JSON the BE opens the table with: the same
    /// encoder the rust reader uses, so reader and writer resolve one schema.
    private String getTableSchemaJson() {
        FileStoreTable table = writeTarget.getTable();
        return table == null ? null : PaimonUtil.encodeTableSchemaToJson(table.schema());
    }

    /// Paimon branch; null for the canonical "main", which the BE defaults to.
    private String getBranch() {
        FileStoreTable table = writeTarget.getTable();
        if (table == null) {
            return null;
        }
        String branch = CoreOptions.branch(table.schema().options());
        return Identifier.DEFAULT_MAIN_BRANCH.equals(branch) ? null : branch;
    }

    private List<String> outputColumnNames() throws AnalysisException {
        int columnOffset = isChangelogWrite() ? 1 : 0;
        if (cols.size() + columnOffset != outputExprs.size()) {
            throw new AnalysisException("Paimon sink output column size mismatch, columns="
                    + cols.size() + ", exprs=" + outputExprs.size());
        }
        List<String> names = new ArrayList<>(outputExprs.size());
        if (isChangelogWrite()) {
            names.add(ROW_KIND_COLUMN);
        }
        for (Column col : cols) {
            names.add(col.getName());
        }
        return names;
    }

    private boolean isChangelogWrite() {
        return dmlCommandType == DMLCommandType.UPDATE
                || dmlCommandType == DMLCommandType.DELETE
                || dmlCommandType == DMLCommandType.MERGE;
    }

}
