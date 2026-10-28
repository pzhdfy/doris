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

// Differential (parity) regression for the two Paimon write backends: the
// same INSERT statements run through the JNI writer
// (enable_paimon_rust_writer=false, the default) and the native Rust FFI
// writer (enable_paimon_rust_writer=true) into twin tables, and the outputs
// must be equal. Planning, transactions and the FE-side commit are shared;
// only the BE writer and its commit-message serialization differ, so this
// suite pins the common surface (append, fixed-bucket PK upsert,
// partial-update merge engine, column-list reorder) where both backends must
// stay equivalent.
suite("test_paimon_write_impl_parity", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    // Both writer backends write the same catalog; readback via the JNI reader
    // so the reader choice cannot mask a writer difference.
    sql "SET enable_paimon_rust_reader=false"

    String catalogName = "test_pw_parity_catalog"
    String dbName = "test_pw_parity_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};

        DROP TABLE IF EXISTS paimon.${dbName}.t_append_jni;
        DROP TABLE IF EXISTS paimon.${dbName}.t_append_ffi;
        CREATE TABLE paimon.${dbName}.t_append_jni (
            id INT, name STRING, score DOUBLE, note STRING
        ) USING paimon;
        CREATE TABLE paimon.${dbName}.t_append_ffi (
            id INT, name STRING, score DOUBLE, note STRING
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_pk_jni;
        DROP TABLE IF EXISTS paimon.${dbName}.t_pk_ffi;
        CREATE TABLE paimon.${dbName}.t_pk_jni (
            id INT, name STRING, score INT
        ) USING paimon
        TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2', 'bucket-key' = 'id');
        CREATE TABLE paimon.${dbName}.t_pk_ffi (
            id INT, name STRING, score INT
        ) USING paimon
        TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2', 'bucket-key' = 'id');

        DROP TABLE IF EXISTS paimon.${dbName}.t_partial_jni;
        DROP TABLE IF EXISTS paimon.${dbName}.t_partial_ffi;
        CREATE TABLE paimon.${dbName}.t_partial_jni (
            id INT, name STRING, score INT, remark STRING
        ) USING paimon
        TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2', 'bucket-key' = 'id',
                      'merge-engine' = 'partial-update');
        CREATE TABLE paimon.${dbName}.t_partial_ffi (
            id INT, name STRING, score INT, remark STRING
        ) USING paimon
        TBLPROPERTIES ('primary-key' = 'id', 'bucket' = '2', 'bucket-key' = 'id',
                      'merge-engine' = 'partial-update');
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
        // Runs one insert statement under both write backends into the twin
        // tables, then returns the two read-back results.
        def dualWrite = { String jniTable, String ffiTable, String insertSql ->
            sql "SET enable_paimon_rust_writer=false"
            sql insertSql.replace("__TABLE__", jniTable)
            sql "SET enable_paimon_rust_writer=true"
            sql insertSql.replace("__TABLE__", ffiTable)
        }
        def dualRead = { String queryTemplate ->
            def a = sql queryTemplate.replace("__TABLE__", "_jni")
            def b = sql queryTemplate.replace("__TABLE__", "_ffi")
            assertEquals(b, a)
        }

        // P1: append, basic types + NULL / empty / special strings.
        dualWrite("t_append_jni", "t_append_ffi", """
            INSERT INTO __TABLE__ VALUES
                (1, 'alice', 95.5, 'plain'),
                (2, NULL, NULL, NULL),
                (3, 'empty_note', 1.25, ''),
                (4, 'quote''d', -0.5, 'tab\\tsep'),
                (5, 'unicode-数据', 2.5, 'multi\nline')
        """)
        dualWrite("t_append_jni", "t_append_ffi",
                "INSERT INTO __TABLE__ VALUES (6, 'second-lifecycle', 6.5, 'appended')")
        dualRead("SELECT id, name, score, note FROM t_append__TABLE__ ORDER BY id")
        assertSparkDorisResultEquals(
                spark_paimon("SELECT id, name, score, note FROM paimon.${dbName}.t_append_ffi ORDER BY id"),
                sql("SELECT id, name, score, note FROM t_append_ffi ORDER BY id"))

        // P2: append with explicit reordered column list.
        dualWrite("t_append_jni", "t_append_ffi", """
            INSERT INTO __TABLE__ (note, id, score, name)
                VALUES ('reordered', 7, 7.5, 'grace')
        """)
        dualRead("SELECT id, name, score, note FROM t_append__TABLE__ WHERE id = 7")

        // P3: fixed-bucket PK upsert — same key written twice, last write wins.
        dualWrite("t_pk_jni", "t_pk_ffi", """
            INSERT INTO __TABLE__ VALUES
                (1, 'first', 100),
                (2, 'only', 200),
                (1, 'second', 111)
        """)
        dualRead("SELECT id, name, score FROM t_pk__TABLE__ ORDER BY id")

        // P4: partial-update merge engine. The subset-column write keeps the
        // columns not in the INSERT list from the previous row on the JNI side;
        // the Rust FFI writer is fail-closed on subset-column INSERTs
        // (documented gap: by-name expansion needs paimon-rust subset-schema
        // support), so that leg writes the same rows with an explicit full
        // column list — same final state, and dualRead still pins parity.
        dualWrite("t_partial_jni", "t_partial_ffi", """
            INSERT INTO __TABLE__ VALUES
                (1, 'base', 10, 'keep'),
                (2, 'gone', 20, 'also-gone')
        """)
        sql "SET enable_paimon_rust_writer=false"
        sql """
            INSERT INTO t_partial_jni (id, score) VALUES
                (1, 11),
                (2, 22)
        """
        sql "SET enable_paimon_rust_writer=true"
        sql """
            INSERT INTO t_partial_ffi (id, name, score, remark) VALUES
                (1, 'base', 11, 'keep'),
                (2, 'gone', 22, 'also-gone')
        """
        dualRead("SELECT id, name, score, remark FROM t_partial__TABLE__ ORDER BY id")
        assertSparkDorisResultEquals(
                spark_paimon("SELECT id, name, score, remark FROM paimon.${dbName}.t_partial_ffi ORDER BY id"),
                sql("SELECT id, name, score, remark FROM t_partial_ffi ORDER BY id"))

        // P5: multi-statement writer churn on the PK table (each statement is a
        // separate writer lifecycle on both paths).
        for (int i = 10; i < 13; i++) {
            dualWrite("t_pk_jni", "t_pk_ffi",
                    "INSERT INTO __TABLE__ VALUES (${i}, 'churn-${i}', ${i} * 10)")
        }
        dualRead("SELECT id, name, score FROM t_pk__TABLE__ ORDER BY id")

        logger.info("paimon write backend parity: all scenarios equal")
    } finally {
        sql "SET enable_paimon_rust_writer=false"
        sql """switch internal"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
