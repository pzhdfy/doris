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

// Regression for dual-layer cache coherence on REFRESH: Doris FE metacache
// invalidation must also invalidate Paimon's CachingCatalog table entry;
// otherwise REFRESH repopulates the Doris cache with the same stale Paimon
// table handle (old schema, old bucket config, old paths). Both the read
// path (query planning) and the write path (getPaimonTableForWrite) go
// through the table loader, so both must see the fresh handle — verified
// once per write backend (JNI and Rust FFI).
suite("test_paimon_write_refresh_coherence", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    sql "SET enable_paimon_rust_reader=false"

    String catalogName = "test_pw_refresh_catalog"
    // One pass per write backend: [label, enable_paimon_rust_writer].
    def writeBackends = [["jni", "false"], ["rust_ffi", "true"]]
    writeBackends.each { backend ->
        String impl = backend[0]
        String flag = backend[1]
        sql "SET enable_paimon_rust_writer=${flag}"
        String dbName = "test_pw_refresh_db_${impl}"
        String tableName = "reload_coherence_t"

        spark_paimon_multi """
            CREATE DATABASE IF NOT EXISTS paimon.${dbName};
            DROP TABLE IF EXISTS paimon.${dbName}.${tableName};
            CREATE TABLE paimon.${dbName}.${tableName} (
                id INT, name STRING
            ) USING paimon;
            INSERT INTO paimon.${dbName}.${tableName} VALUES (1, 'before_refresh');
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
            // Phase 1: Read the initial 2-column schema.
            def descBefore = sql """DESCRIBE ${tableName}"""
            assertEquals(2, descBefore.size())
            def rowsBefore = sql """SELECT id, name FROM ${tableName} ORDER BY id"""
            assertEquals([[1, "before_refresh"]], rowsBefore)

            // Phase 2: External modification - Spark adds a column and inserts a row with it.
            spark_paimon """
                ALTER TABLE paimon.${dbName}.${tableName} ADD COLUMN new_col STRING AFTER name
            """
            spark_paimon """
                INSERT INTO paimon.${dbName}.${tableName} VALUES (2, 'after_refresh', 'new_value')
            """

            // Phase 3: REFRESH must invalidate both Doris metacache AND the
            // Paimon CachingCatalog. If the Paimon layer returns the same stale
            // handle with the old 2-column schema, the REFRESH is a no-op for
            // schema changes.
            sql """REFRESH CATALOG ${catalogName}"""

            // Phase 4: Read path must see the new schema (3 columns).
            def descAfter = sql """DESCRIBE ${tableName}"""
            assertEquals(3, descAfter.size())
            def rowsAfter = sql """SELECT id, name, new_col FROM ${tableName} ORDER BY id"""
            assertEquals(2, rowsAfter.size())
            assertEquals("new_value", rowsAfter[1][2])

            // Phase 5: Write path must also see the new schema (write with the new column).
            sql """INSERT INTO ${tableName} VALUES (3, 'write_after_refresh', 'write_new_col')"""
            def rowsFinal = sql """SELECT id, name, new_col FROM ${tableName} ORDER BY id"""
            assertEquals(3, rowsFinal.size())
            assertEquals("write_new_col", rowsFinal[2][2])

            logger.info("paimon refresh coherence (${impl} writer): dual-layer cache invalidation verified")
        } finally {
            sql "SET enable_paimon_rust_writer=false"
            sql """switch internal"""
            sql """drop catalog if exists ${catalogName}"""
        }
    }  // writeBackends.each
}
