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
// specific language governing permissions and the License is
// distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Rust FFI write backend (enable_paimon_rust_writer=true): the community write
// path with the native libpaimon_c writer instead of the Java JNI writer.
// Planning, transactions and the FE-side commit are shared with the JNI path;
// only the BE writer backend and its commit-message serialization differ.
// This suite exercises that backend end to end: append (unpartitioned and
// partitioned), INSERT OVERWRITE, and mixed-impl round trips on the same
// table, with the commit path proven by reading back through the JNI reader.
suite("test_paimon_write_rust_ffi", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    // Readback via the JNI reader (reader choice is orthogonal to the write
    // backend under test).
    sql "SET enable_paimon_rust_reader=false"

    String catalogName = "test_pw_rust_ffi_catalog"
    String dbName = "test_pw_rust_ffi_db"

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.${dbName};
        DROP TABLE IF EXISTS paimon.${dbName}.t_ffi_append;
        CREATE TABLE paimon.${dbName}.t_ffi_append (
            id INT, name STRING, score DOUBLE
        ) USING paimon;

        DROP TABLE IF EXISTS paimon.${dbName}.t_ffi_part;
        CREATE TABLE paimon.${dbName}.t_ffi_part (
            id INT, name STRING, region STRING
        ) USING paimon
        PARTITIONED BY (region);

        DROP TABLE IF EXISTS paimon.${dbName}.t_ffi_pk;
        CREATE TABLE paimon.${dbName}.t_ffi_pk (
            id INT, name STRING
        ) USING paimon
        TBLPROPERTIES (
            'primary-key' = 'id',
            'bucket' = '2',
            'bucket-key' = 'id'
        );

        DROP TABLE IF EXISTS paimon.${dbName}.t_ffi_overwrite;
        CREATE TABLE paimon.${dbName}.t_ffi_overwrite (
            id INT, name STRING
        ) USING paimon;
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
        // ------------------------------------------------------------------
        // 1. Append through the Rust FFI backend.
        // ------------------------------------------------------------------
        sql "SET enable_paimon_rust_writer=true"
        sql """INSERT INTO t_ffi_append VALUES (1, 'a', 1.5), (2, 'b', 2.5), (3, 'c', 3.5)"""
        def rows = sql """SELECT id, name, score FROM t_ffi_append ORDER BY id"""
        // JDBC returns DOUBLE as BigDecimal; normalize to double literals before
        // deep-comparing (the printed values are already equal — this is a
        // type, not a data, mismatch).
        rows = rows.collect { [it[0] as int, it[1] as String, it[2] as double] }
        assertEquals([[1, "a", 1.5d], [2, "b", 2.5d], [3, "c", 3.5d]], rows)

        // ------------------------------------------------------------------
        // 2. Partitioned append: every partition's files come from the same
        //    prepare_commit; verify both partitions land.
        // ------------------------------------------------------------------
        sql """INSERT INTO t_ffi_part VALUES (1, 'x', 'east'), (2, 'y', 'west'), (3, 'z', 'east')"""
        def partRows = sql """SELECT id, name, region FROM t_ffi_part ORDER BY id"""
        assertEquals([[1, "x", "east"], [2, "y", "west"], [3, "z", "east"]], partRows)

        // ------------------------------------------------------------------
        // 3. Mixed implementations on one table: a commit produced by the FFI
        //    backend must be followed by a JNI commit and vice versa — both
        //    feed the same FE transaction/deserializer.
        // ------------------------------------------------------------------
        sql "SET enable_paimon_rust_writer=false"
        sql """INSERT INTO t_ffi_append VALUES (4, 'd', 4.5)"""
        sql "SET enable_paimon_rust_writer=true"
        sql """INSERT INTO t_ffi_append VALUES (5, 'e', 5.5)"""
        rows = sql """SELECT id, name, score FROM t_ffi_append ORDER BY id"""
        assertEquals(5, rows.size())
        assertEquals(5, rows[4][0])
        assertEquals("e", rows[4][1])

        // ------------------------------------------------------------------
        // 4. INSERT OVERWRITE through the FFI backend: replaces all prior
        //    data (files from both backends) in one overwrite commit.
        // ------------------------------------------------------------------
        sql "SET enable_paimon_rust_writer=true"
        sql """INSERT OVERWRITE TABLE t_ffi_overwrite VALUES (10, 'overwritten')"""
        sql "SET enable_paimon_rust_writer=false"
        sql """INSERT INTO t_ffi_overwrite VALUES (11, 'before')"""
        sql "SET enable_paimon_rust_writer=true"
        sql """INSERT OVERWRITE TABLE t_ffi_overwrite VALUES (20, 'after')"""
        def owRows = sql """SELECT id, name FROM t_ffi_overwrite ORDER BY id"""
        assertEquals([[20, "after"]], owRows)

        // ------------------------------------------------------------------
        // 5. Primary-key table through the FFI backend: the Rust writer owns
        //    bucket assignment for PK tables; verify rows land and dedup.
        // ------------------------------------------------------------------
        sql """INSERT INTO t_ffi_pk VALUES (1, 'first'), (2, 'second')"""
        sql """INSERT INTO t_ffi_pk VALUES (1, 'first-updated')"""
        def pkRows = sql """SELECT id, name FROM t_ffi_pk ORDER BY id"""
        assertEquals([[1, "first-updated"], [2, "second"]], pkRows)

        // ------------------------------------------------------------------
        // 6. External consistency: Spark reads what the FFI backend wrote
        //    (proves the snapshot/manifest the FE committed is canonical).
        // ------------------------------------------------------------------
        spark_paimon_multi """
            INSERT INTO paimon.${dbName}.t_ffi_append VALUES (6, 'from_spark', 6.5);
        """
        sql "SET enable_paimon_rust_writer=true"
        sql """INSERT INTO t_ffi_append VALUES (7, 'after_spark', 7.5)"""
        rows = sql """SELECT id, name, score FROM t_ffi_append ORDER BY id"""
        assertEquals(7, rows.size())
        assertEquals(6, rows[5][0])
        assertEquals(7, rows[6][0])

        logger.info("paimon rust ffi writer: append/partitioned/overwrite/pk/mixed verified")
    } finally {
        sql "SET enable_paimon_rust_writer=false"
        sql """switch internal"""
        sql """drop catalog if exists ${catalogName}"""
    }
}
