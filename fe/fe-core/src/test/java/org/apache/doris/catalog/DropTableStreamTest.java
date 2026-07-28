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

package org.apache.doris.catalog;

import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.DropStreamCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

public class DropTableStreamTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.allow_replica_on_same_host = true;
        Config.enable_table_stream = true;

        createDatabase("test_stream");
        String createTableStr1 = "create table if not exists test_stream.tbl1\n" + "(k1 int, k2 int)\n" + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW', "
                + "'binlog.need_historical_value' = 'true'); ";
        createTable(createTableStr1);

        String createStreamStr1 =  "create stream test_stream.s1 on table test_stream.tbl1\n"
                + "properties('show_initial_rows' = 'true'); ";
        createTable(createStreamStr1);
        String createStreamStr2 =  "create stream test_stream.s2 on table test_stream.tbl1\n"
                + "properties('type' = 'append_only', 'show_initial_rows' = 'true'); ";
        createTable(createStreamStr2);
    }

    private void dropStream(String sql) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        if (parsed instanceof DropStreamCommand) {
            ((DropStreamCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    @Test
    public void testNormalDropStream() throws Exception {
        // test drop
        ExceptionChecker
                .expectThrowsNoException(() ->
                        dropStream("drop stream test_stream.s1;"));
        // test force drop
        ExceptionChecker
                .expectThrowsNoException(() ->
                        dropStream("drop stream test_stream.s2 force;"));

        // test if exist
        ExceptionChecker
                .expectThrowsNoException(() ->
                        dropStream("drop stream if exists test_stream.s3;"));
    }

    @Test
    public void testAbnormalDropStream() throws Exception {
        // test not exist
        ExceptionChecker.expectThrowsWithMsg(DdlException.class, "Unknown table 's3' in test_stream",
                () -> dropStream("drop stream test_stream.s3;"));
    }

    @Test
    public void testCloudDropRequiresForce() {
        String previousCloudUniqueId = Config.cloud_unique_id;
        Config.cloud_unique_id = "cloud_table_stream_ut";
        try {
            Exception exception = Assertions.assertThrows(Exception.class,
                    () -> dropStream("drop stream test_stream.not_reached;"));
            Assertions.assertTrue(exception.getMessage().contains("only supports DROP STREAM ... FORCE"));
        } finally {
            Config.cloud_unique_id = previousCloudUniqueId;
        }
    }

    @Test
    public void testDropDatabaseRemovesOwnedStream() throws Exception {
        String dbName = "test_stream_drop_database";
        createDatabase(dbName);
        createBinlogTable(dbName + ".tbl");
        createTableStream(dbName + ".s", dbName + ".tbl");

        Database db = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(dbName);
        long streamId = db.getTableOrMetaException("s").getId();
        Assertions.assertTrue(Env.getCurrentEnv().getTableStreamManager()
                .getTableStreamIds(db).contains(streamId));

        dropDatabase(dbName);

        Assertions.assertFalse(Env.getCurrentInternalCatalog().getDb(dbName).isPresent());
        Assertions.assertFalse(Env.getCurrentEnv().getTableStreamManager()
                .getTableStreamIds(db).contains(streamId));
        assertTableInRecycleBin(streamId);
    }

    @Test
    public void testCrossDatabaseBaseDropKeepsStreamAndStreamStillRecycles() throws Exception {
        String baseDbName = "test_stream_cross_base";
        String streamDbName = "test_stream_cross_owner";
        createDatabase(baseDbName);
        createDatabase(streamDbName);
        createBinlogTable(baseDbName + ".tbl");
        createTableStream(streamDbName + ".s", baseDbName + ".tbl");

        Database baseDb = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(baseDbName);
        Database streamDb = (Database) Env.getCurrentInternalCatalog().getDbOrMetaException(streamDbName);
        OlapTableStream stream = (OlapTableStream) streamDb.getTableOrMetaException("s");
        long streamId = stream.getId();

        dropTableWithSql("drop table " + baseDbName + ".tbl force");

        Assertions.assertNull(baseDb.getTableNullable("tbl"));
        Assertions.assertSame(stream, streamDb.getTableOrMetaException("s"));
        Assertions.assertNull(stream.getBaseTableInfo().getTableNullable());
        Assertions.assertTrue(Env.getCurrentEnv().getTableStreamManager()
                .getTableStreamIds(streamDb).contains(streamId));

        dropStream("drop stream " + streamDbName + ".s");

        Assertions.assertNull(streamDb.getTableNullable("s"));
        Assertions.assertFalse(Env.getCurrentEnv().getTableStreamManager()
                .getTableStreamIds(streamDb).contains(streamId));
        assertTableInRecycleBin(streamId);

        dropDatabase(baseDbName);
        dropDatabase(streamDbName);
    }

    private void createBinlogTable(String qualifiedTableName) throws Exception {
        createTable("create table " + qualifiedTableName + " (\n"
                + "  k1 int,\n"
                + "  k2 int\n"
                + ")\n"
                + "unique key(k1)\n"
                + "distributed by hash(k1) buckets 1\n"
                + "properties('replication_num' = '1', 'binlog.enable' = 'true', "
                + "'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true')");
    }

    private void createTableStream(String qualifiedStreamName, String qualifiedBaseTableName) throws Exception {
        createTable("create stream " + qualifiedStreamName + " on table " + qualifiedBaseTableName
                + " properties('show_initial_rows' = 'true')");
    }

    private void assertTableInRecycleBin(long tableId) {
        Set<Long> dbIds = new HashSet<>();
        Set<Long> tableIds = new HashSet<>();
        Set<Long> partitionIds = new HashSet<>();
        Env.getCurrentRecycleBin().getRecycleIds(dbIds, tableIds, partitionIds);
        Assertions.assertTrue(tableIds.contains(tableId));
    }

    @Override
    protected void runAfterAll() throws Exception {
        dropDatabase("test_stream");
    }
}
