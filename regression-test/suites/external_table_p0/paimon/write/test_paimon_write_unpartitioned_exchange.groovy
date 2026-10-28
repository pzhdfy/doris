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

suite("test_paimon_write_unpartitioned_exchange", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    // Guards the exchange sink's EXTERNAL_TABLE_SINK_UNPARTITIONED dispatch. The planner
    // picks it for a write-only append fixed-bucket table when the bucket route falls
    // back (here: a partial-column INSERT whose sink output misses the bucket key), and
    // the BE exchange sink used to drop every block of that partition type while the
    // INSERT still reported success. Both write backends share the dispatch.
    //
    // Readback via the JNI reader: reader choice is orthogonal to the write path.
    sql "SET enable_paimon_rust_reader=false"

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    String catalogName = "test_pw_unpart_exch_catalog"
    // One pass per write backend: [label, enable_paimon_rust_writer].
    def writeBackends = [["jni", "false"], ["rust_ffi", "true"]]
    writeBackends.each { backend ->
        String impl = backend[0]
        String flag = backend[1]
        sql "SET enable_paimon_rust_writer=${flag}"
        String dbName = "test_pw_unpart_exch_db_${impl}"

        spark_paimon_multi """
            CREATE DATABASE IF NOT EXISTS paimon.${dbName};

            DROP TABLE IF EXISTS paimon.${dbName}.t_exch_unpart;
            CREATE TABLE paimon.${dbName}.t_exch_unpart (
                id INT, name STRING
            ) USING paimon
            TBLPROPERTIES (
                'bucket' = '4',
                'bucket-key' = 'name',
                'write-only' = 'true'
            );
        """

        sql """drop catalog if exists ${catalogName}"""
        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type' = 'paimon',
                'paimon.catalog.type' = 'filesystem',
                'warehouse' = 's3://warehouse/wh',
                's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.region' = 'us-east-1',
                's3.path.style.access' = 'true'
            );
        """
        sql """switch ${catalogName}"""
        sql """use ${dbName}"""

        try {
            // Full-column INSERT: bucket key is in the sink output, so the planner routes
            // through the fixed-bucket distribution. This is the control leg for both
            // write backends.
            sql """
                INSERT INTO t_exch_unpart
                SELECT CAST(number AS INT), concat('row_', CAST(number AS STRING))
                FROM numbers("number" = "4")
            """
            qt_exch_full """
                SELECT COUNT(*), COUNT(name), MIN(id), MAX(id) FROM t_exch_unpart
            """

            if (impl == "jni") {
                // Partial-column INSERT whose sink output misses the bucket key: the
                // fixed-bucket route falls back and the planner requires the unpartitioned
                // external-sink distribution. The Java writer expands the subset by name,
                // filling the missing column with nulls.
                def unpartitionedPlan = sql """
                    EXPLAIN SHAPE PLAN
                    INSERT INTO t_exch_unpart (id)
                    SELECT CAST(number AS INT)
                    FROM numbers("number" = "3")
                """
                assertTrue(unpartitionedPlan.flatten().join("\n")
                        .contains("DistributionSpecExternalTableSinkUnPartitioned"))

                sql """
                    INSERT INTO t_exch_unpart (id)
                    SELECT CAST(number AS INT) + 10
                    FROM numbers("number" = "3")
                """
                // Every row from the unpartitioned exchange must land: 4 full-column rows
                // with names plus 3 partial rows whose name is null. Before the dispatch
                // fix the partial rows vanished while the INSERT reported success.
                qt_exch_partial """
                    SELECT COUNT(*), COUNT(name), MIN(id), MAX(id) FROM t_exch_unpart
                """
                qt_exch_partial_rows """
                    SELECT id, name FROM t_exch_unpart ORDER BY id
                """
            } else {
                // The Rust FFI writer is fail-closed on partial-column INSERTs (documented
                // gap: by-name expansion needs the paimon-rust subset-schema support), so
                // this leg keeps the full-column control above; the exchange dispatch that
                // the JNI leg exercises is shared operator code.
                logger.info("skip partial-column unpartitioned leg for impl ${impl}")
            }
        } finally {
            try {
                sql """switch ${catalogName}"""
                sql """use ${dbName}"""
                spark_paimon_multi """DROP TABLE IF EXISTS paimon.${dbName}.t_exch_unpart"""
            } catch (Throwable t) {
                logger.warn("cleanup failed", t)
            }
        }
    }
}
