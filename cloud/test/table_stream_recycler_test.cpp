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

#include <brpc/controller.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/bvars.h"
#include "common/config.h"
#include "common/defer.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/versioned_value.h"
#include "rate-limiter/rate_limiter.h"
#include "recycler/recycler.h"
#include "resource-manager/resource_manager.h"
#include "snapshot/snapshot_manager.h"

namespace doris::cloud {
namespace {

constexpr std::string_view kInstanceId = "table_stream_recycler_instance";
constexpr int64_t kBaseDbId = 1001;
constexpr int64_t kBaseTableId = 1002;
constexpr int64_t kStreamDbId = 1003;
constexpr int64_t kStreamId = 1004;
constexpr int64_t kPartitionId = 1005;

void put_offset(Transaction* txn, int64_t partition_id, int64_t stream_id = kStreamId) {
    TableStreamOffsetPB offset;
    offset.set_partition_id(partition_id);
    offset.set_state(TableStreamOffsetStatePB::TABLE_STREAM_OFFSET_CONSUMED);
    offset.set_offset_tso(100);
    const auto latest_key =
            table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId, kStreamDbId,
                                     stream_id, partition_id});
    const auto versioned_key =
            versioned::table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                                kStreamDbId, stream_id, partition_id});
    txn->put(latest_key, offset.SerializeAsString());
    versioned_put(txn, versioned_key, Versionstamp(11, 0), offset.SerializeAsString());
    offset.set_offset_tso(110);
    versioned_put(txn, versioned_key, Versionstamp(12, 0), offset.SerializeAsString());
}

void put_stream_recycle_index(Transaction* txn, int64_t stream_id,
                              RecycleIndexPB::State state) {
    RecycleIndexPB recycle_index;
    recycle_index.set_db_id(kBaseDbId);
    recycle_index.set_table_id(kBaseTableId);
    recycle_index.set_creation_time(0);
    recycle_index.set_expiration(0);
    recycle_index.set_state(state);
    recycle_index.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    recycle_index.set_stream_db_id(kStreamDbId);
    txn->put(recycle_index_key({std::string(kInstanceId), stream_id}),
             recycle_index.SerializeAsString());
}

bool key_exists(TxnKv* txn_kv, std::string_view key) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    return txn->get(key, &value, true) == TxnErrorCode::TXN_OK;
}

size_t range_size(TxnKv* txn_kv, const std::string& prefix) {
    std::unique_ptr<Transaction> txn;
    EXPECT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string end = prefix;
    end.push_back('\xff');
    std::unique_ptr<RangeGetIterator> iter;
    EXPECT_EQ(txn->get(prefix, end, &iter, true), TxnErrorCode::TXN_OK);
    return iter->size();
}

InstanceRecycler make_recycler(const std::shared_ptr<TxnKv>& txn_kv) {
    InstanceInfoPB instance;
    instance.set_instance_id(std::string(kInstanceId));
    return InstanceRecycler(txn_kv, instance, RecyclerThreadPoolGroup {},
                            std::make_shared<TxnLazyCommitter>(txn_kv));
}

TEST(TableStreamRecyclerTest, RecycleStreamDeletesOnlyOffsets) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);

    RecycleIndexPB recycle_index;
    recycle_index.set_db_id(kBaseDbId);
    recycle_index.set_table_id(kBaseTableId);
    recycle_index.set_creation_time(0);
    recycle_index.set_expiration(0);
    recycle_index.set_state(RecycleIndexPB::DROPPED);
    recycle_index.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    recycle_index.set_stream_db_id(kStreamDbId);
    txn->put(recycle_index_key({std::string(kInstanceId), kStreamId}),
             recycle_index.SerializeAsString());
    const std::string unrelated_key = meta_tablet_key(
            {std::string(kInstanceId), kBaseTableId, kStreamId, kPartitionId, 2001});
    txn->put(unrelated_key, "unrelated physical data");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_indexes(), 0);

    EXPECT_FALSE(
            key_exists(txn_kv.get(), recycle_index_key({std::string(kInstanceId), kStreamId})));
    EXPECT_EQ(range_size(txn_kv.get(),
                         table_stream_offset_key_prefix(std::string(kInstanceId), kBaseDbId,
                                                        kBaseTableId, kStreamDbId, kStreamId)),
              0);
    EXPECT_EQ(range_size(txn_kv.get(), versioned::table_stream_offset_key_prefix(
                                               std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                               kStreamDbId, kStreamId)),
              0);
    EXPECT_TRUE(key_exists(txn_kv.get(), unrelated_key));
}

TEST(TableStreamRecyclerTest, RecycleStreamResumesAfterLatestOffsetsWereDeleted) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);
    txn->remove(table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                         kStreamDbId, kStreamId, kPartitionId}));
    txn->remove(table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                         kStreamDbId, kStreamId, kPartitionId + 1}));
    put_stream_recycle_index(txn.get(), kStreamId, RecycleIndexPB::RECYCLING);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    EXPECT_EQ(range_size(txn_kv.get(),
                         table_stream_offset_key_prefix(std::string(kInstanceId), kBaseDbId,
                                                        kBaseTableId, kStreamDbId, kStreamId)),
              0);
    EXPECT_GT(range_size(txn_kv.get(), versioned::table_stream_offset_key_prefix(
                                               std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                               kStreamDbId, kStreamId)),
              0);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_indexes(), 0);

    EXPECT_FALSE(
            key_exists(txn_kv.get(), recycle_index_key({std::string(kInstanceId), kStreamId})));
    EXPECT_EQ(range_size(txn_kv.get(),
                         table_stream_offset_key_prefix(std::string(kInstanceId), kBaseDbId,
                                                        kBaseTableId, kStreamDbId, kStreamId)),
              0);
    EXPECT_EQ(range_size(txn_kv.get(), versioned::table_stream_offset_key_prefix(
                                               std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                               kStreamDbId, kStreamId)),
              0);
    EXPECT_EQ(recycler.recycle_indexes(), 0);
}

TEST(TableStreamRecyclerTest, RecycleStreamFinalizesWhenOffsetsWereAlreadyDeleted) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_stream_recycle_index(txn.get(), kStreamId, RecycleIndexPB::RECYCLING);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_indexes(), 0);

    EXPECT_FALSE(
            key_exists(txn_kv.get(), recycle_index_key({std::string(kInstanceId), kStreamId})));
    EXPECT_EQ(recycler.recycle_indexes(), 0);
}

TEST(TableStreamRecyclerTest, RecycleAbandonedPreparedStreamAndPartialOffsets) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);
    put_stream_recycle_index(txn.get(), kStreamId, RecycleIndexPB::PREPARED);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_indexes(), 0);

    EXPECT_FALSE(
            key_exists(txn_kv.get(), recycle_index_key({std::string(kInstanceId), kStreamId})));
    EXPECT_EQ(range_size(txn_kv.get(),
                         table_stream_offset_key_prefix(std::string(kInstanceId), kBaseDbId,
                                                        kBaseTableId, kStreamDbId, kStreamId)),
              0);
    EXPECT_EQ(range_size(txn_kv.get(), versioned::table_stream_offset_key_prefix(
                                               std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                               kStreamDbId, kStreamId)),
              0);
}

TEST(TableStreamRecyclerTest, StatisticsDispatchesStreamToOffsetScan) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);

    RecycleIndexPB recycle_index;
    recycle_index.set_db_id(kBaseDbId);
    recycle_index.set_table_id(kBaseTableId);
    recycle_index.set_creation_time(0);
    recycle_index.set_expiration(0);
    recycle_index.set_state(RecycleIndexPB::DROPPED);
    recycle_index.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    recycle_index.set_stream_db_id(kStreamDbId);
    txn->put(recycle_index_key({std::string(kInstanceId), kStreamId}),
             recycle_index.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.scan_and_statistics_indexes(), 0);

    EXPECT_EQ(g_bvar_recycler_instance_last_round_to_recycle_num.get(
                      {std::string(kInstanceId), "recycle_stream"}),
              6);
    EXPECT_EQ(g_bvar_recycler_instance_last_round_to_recycle_num.get(
                      {std::string(kInstanceId), "recycle_indexes"}),
              0);
}

TEST(TableStreamRecyclerTest, RecyclePartitionDeletesOnlyThatPartitionOffsets) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);

    RecyclePartitionPB recycle_partition;
    recycle_partition.set_db_id(kBaseDbId);
    recycle_partition.set_table_id(kBaseTableId);
    recycle_partition.add_index_id(2001);
    recycle_partition.set_creation_time(0);
    recycle_partition.set_expiration(0);
    recycle_partition.set_state(RecyclePartitionPB::DROPPED);
    TableStreamIdentityPB* identity = recycle_partition.add_table_streams();
    identity->set_base_db_id(kBaseDbId);
    identity->set_base_table_id(kBaseTableId);
    identity->set_stream_db_id(kStreamDbId);
    identity->set_stream_id(kStreamId);
    txn->put(recycle_partition_key({std::string(kInstanceId), kPartitionId}),
             recycle_partition.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_partitions(), 0);

    const auto dropped_latest =
            table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId, kStreamDbId,
                                     kStreamId, kPartitionId});
    const auto retained_latest =
            table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId, kStreamDbId,
                                     kStreamId, kPartitionId + 1});
    EXPECT_FALSE(key_exists(txn_kv.get(), dropped_latest));
    EXPECT_EQ(range_size(txn_kv.get(), versioned::table_stream_offset_key(
                                               {std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                                kStreamDbId, kStreamId, kPartitionId})),
              0);
    EXPECT_TRUE(key_exists(txn_kv.get(), retained_latest));
    EXPECT_GT(range_size(txn_kv.get(), versioned::table_stream_offset_key(
                                               {std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                                kStreamDbId, kStreamId, kPartitionId + 1})),
              0);
}

TEST(TableStreamRecyclerTest, DropPartitionRpcAndRecyclerRemoveStreamOffsets) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    auto txn_kv = std::make_shared<MemTxnKv>();
    txn_kv->update_commit_version(1000);
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(std::string(kInstanceId));
    instance.set_status(InstanceInfoPB::NORMAL);
    instance.set_multi_version_status(MULTI_VERSION_READ_WRITE);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put(instance_key({std::string(kInstanceId)}), instance.SerializeAsString());
    PartitionIndexPB partition_index;
    partition_index.set_db_id(kBaseDbId);
    partition_index.set_table_id(kBaseTableId);
    txn->put(versioned::partition_index_key({std::string(kInstanceId), kPartitionId}),
             partition_index.SerializeAsString());
    versioned_put(txn.get(),
                  versioned::meta_partition_key({std::string(kInstanceId), kPartitionId}),
                  Versionstamp(11, 0), "");
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId + 1);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto resource_mgr = std::make_shared<ResourceManager>(txn_kv);
    auto [refresh_code, refresh_message] =
            resource_mgr->refresh_instance(std::string(kInstanceId));
    ASSERT_EQ(refresh_code, MetaServiceCode::OK) << refresh_message;
    auto rate_limiter = std::make_shared<RateLimiter>();
    auto snapshot_manager = std::make_shared<SnapshotManager>(txn_kv);
    MetaServiceImpl meta_service(txn_kv, resource_mgr, rate_limiter, snapshot_manager);

    PartitionRequest request;
    request.set_cloud_unique_id("1:" + std::string(kInstanceId) + ":table_stream_recycler_test");
    request.set_db_id(kBaseDbId);
    request.set_table_id(kBaseTableId);
    request.add_index_ids(2001);
    request.add_partition_ids(kPartitionId);
    request.set_expiration(0);
    TableStreamIdentityPB* identity = request.add_table_streams();
    identity->set_base_db_id(kBaseDbId);
    identity->set_base_table_id(kBaseTableId);
    identity->set_stream_db_id(kStreamDbId);
    identity->set_stream_id(kStreamId);

    PartitionResponse response;
    brpc::Controller controller;
    meta_service.drop_partition(&controller, &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();

    EXPECT_FALSE(key_exists(
            txn_kv.get(), recycle_partition_key({std::string(kInstanceId), kPartitionId})));
    EXPECT_GT(range_size(txn_kv.get(), versioned::log_key({std::string(kInstanceId)})), 0);

    InstanceRecycler recycler(txn_kv, instance, RecyclerThreadPoolGroup {},
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.init(), 0);
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);
    EXPECT_TRUE(key_exists(
            txn_kv.get(), recycle_partition_key({std::string(kInstanceId), kPartitionId})));

    ASSERT_EQ(recycler.recycle_partitions(), 0);

    EXPECT_FALSE(key_exists(
            txn_kv.get(),
            table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                     kStreamDbId, kStreamId, kPartitionId})));
    EXPECT_EQ(range_size(txn_kv.get(),
                         versioned::table_stream_offset_key(
                                 {std::string(kInstanceId), kBaseDbId, kBaseTableId, kStreamDbId,
                                  kStreamId, kPartitionId})),
              0);
    EXPECT_TRUE(key_exists(
            txn_kv.get(),
            table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                     kStreamDbId, kStreamId, kPartitionId + 1})));
    EXPECT_FALSE(key_exists(
            txn_kv.get(), recycle_partition_key({std::string(kInstanceId), kPartitionId})));
    EXPECT_EQ(recycler.recycle_operation_logs(), 0);
    EXPECT_EQ(recycler.recycle_partitions(), 0);
}

TEST(TableStreamRecyclerTest, RecyclePartitionUsesPersistedStreamIdentity) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    constexpr std::string_view source_instance_id = "table_stream_recycler_source";
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

    InstanceInfoPB source_instance;
    source_instance.set_instance_id(std::string(source_instance_id));
    txn->put(instance_key({std::string(source_instance_id)}), source_instance.SerializeAsString());
    InstanceInfoPB child_instance;
    child_instance.set_instance_id(std::string(kInstanceId));
    child_instance.set_multi_version_status(MULTI_VERSION_READ_WRITE);
    child_instance.set_source_instance_id(std::string(source_instance_id));
    child_instance.set_source_snapshot_id(Versionstamp(20, 0).to_string());
    txn->put(instance_key({std::string(kInstanceId)}), child_instance.SerializeAsString());

    put_offset(txn.get(), kPartitionId);
    TableStreamOffsetPB source_offset;
    source_offset.set_partition_id(kPartitionId);
    source_offset.set_state(TableStreamOffsetStatePB::TABLE_STREAM_OFFSET_CONSUMED);
    source_offset.set_offset_tso(90);
    const auto source_latest =
            table_stream_offset_key({std::string(source_instance_id), kBaseDbId, kBaseTableId,
                                     kStreamDbId, kStreamId, kPartitionId});
    const auto source_versioned = versioned::table_stream_offset_key(
            {std::string(source_instance_id), kBaseDbId, kBaseTableId, kStreamDbId, kStreamId,
             kPartitionId});
    txn->put(source_latest, source_offset.SerializeAsString());
    versioned_put(txn.get(), source_versioned, Versionstamp(11, 0),
                  source_offset.SerializeAsString());

    RecyclePartitionPB recycle_partition;
    recycle_partition.set_db_id(kBaseDbId);
    recycle_partition.set_table_id(kBaseTableId);
    recycle_partition.add_index_id(2001);
    recycle_partition.set_creation_time(0);
    recycle_partition.set_expiration(0);
    recycle_partition.set_state(RecyclePartitionPB::DROPPED);
    TableStreamIdentityPB* identity = recycle_partition.add_table_streams();
    identity->set_base_db_id(kBaseDbId);
    identity->set_base_table_id(kBaseTableId);
    identity->set_stream_db_id(kStreamDbId);
    identity->set_stream_id(kStreamId);
    txn->put(recycle_partition_key({std::string(kInstanceId), kPartitionId}),
             recycle_partition.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler recycler(txn_kv, child_instance, RecyclerThreadPoolGroup {},
                              std::make_shared<TxnLazyCommitter>(txn_kv));
    ASSERT_EQ(recycler.recycle_partitions(), 0);

    const auto child_latest =
            table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId, kStreamDbId,
                                     kStreamId, kPartitionId});
    const auto child_versioned =
            versioned::table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                                kStreamDbId, kStreamId, kPartitionId});
    EXPECT_FALSE(key_exists(txn_kv.get(), child_latest));
    EXPECT_EQ(range_size(txn_kv.get(), child_versioned), 0);
    EXPECT_TRUE(key_exists(txn_kv.get(), source_latest));
    EXPECT_GT(range_size(txn_kv.get(), source_versioned), 0);
}

TEST(TableStreamRecyclerTest, RecyclePartitionResumesAfterPartialOffsetCleanup) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    const int32_t old_batch_size = config::recycler_max_tasks_per_batch;
    config::force_immediate_recycle = true;
    config::recycler_max_tasks_per_batch = 1;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
        config::recycler_max_tasks_per_batch = old_batch_size;
    };

    constexpr int64_t second_stream_id = kStreamId + 1;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_offset(txn.get(), kPartitionId);
    put_offset(txn.get(), kPartitionId, second_stream_id);

    txn->remove(table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                         kStreamDbId, kStreamId, kPartitionId}));
    versioned_remove_all(
            txn.get(),
            versioned::table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                                kStreamDbId, kStreamId, kPartitionId}));

    RecyclePartitionPB recycle_partition;
    recycle_partition.set_db_id(kBaseDbId);
    recycle_partition.set_table_id(kBaseTableId);
    recycle_partition.add_index_id(2001);
    recycle_partition.set_creation_time(0);
    recycle_partition.set_expiration(0);
    recycle_partition.set_state(RecyclePartitionPB::RECYCLING);
    for (int64_t stream_id : {kStreamId, second_stream_id}) {
        TableStreamIdentityPB* identity = recycle_partition.add_table_streams();
        identity->set_base_db_id(kBaseDbId);
        identity->set_base_table_id(kBaseTableId);
        identity->set_stream_db_id(kStreamDbId);
        identity->set_stream_id(stream_id);
    }
    txn->put(recycle_partition_key({std::string(kInstanceId), kPartitionId}),
             recycle_partition.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    InstanceRecycler recycler = make_recycler(txn_kv);
    ASSERT_EQ(recycler.recycle_partitions(), 0);

    for (int64_t stream_id : {kStreamId, second_stream_id}) {
        EXPECT_FALSE(key_exists(
                txn_kv.get(),
                table_stream_offset_key({std::string(kInstanceId), kBaseDbId, kBaseTableId,
                                         kStreamDbId, stream_id, kPartitionId})));
        EXPECT_EQ(range_size(
                          txn_kv.get(),
                          versioned::table_stream_offset_key(
                                  {std::string(kInstanceId), kBaseDbId, kBaseTableId, kStreamDbId,
                                   stream_id, kPartitionId})),
                  0);
    }
    EXPECT_FALSE(key_exists(
            txn_kv.get(), recycle_partition_key({std::string(kInstanceId), kPartitionId})));
    EXPECT_EQ(recycler.recycle_partitions(), 0);
}

} // namespace
} // namespace doris::cloud
