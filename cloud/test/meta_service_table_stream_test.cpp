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
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <initializer_list>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/defer.h"
#include "common/lexical_util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "recycler/recycler.h"
#include "resource-manager/resource_manager.h"
#include "snapshot/snapshot_manager.h"

namespace doris::cloud {

extern std::unique_ptr<MetaServiceProxy> get_meta_service(bool mock_resource_mgr);
extern TxnErrorCode read_operation_log(Transaction* txn, std::string_view log_key,
                                       Versionstamp* log_version, OperationLogPB* operation_log);

class MetaServiceTableStreamTest : public ::testing::Test {
protected:
    static constexpr int64_t kTargetDbId = 3001;
    static constexpr int64_t kTargetTableId = 3002;
    static constexpr int64_t kTargetIndexId = 3003;
    static constexpr int64_t kTargetPartitionId = 3004;
    static constexpr int64_t kTargetTabletId = 3005;
    static constexpr int64_t kBaseIndexId = 1005;

    void SetUp() override {
        service_ = get_meta_service(false);
        instance_id_ = "table_stream_read_state_instance";
        cloud_unique_id_ = "1:" + instance_id_ + ":test";
        identity_.set_base_db_id(1001);
        identity_.set_base_table_id(1002);
        identity_.set_stream_db_id(1003);
        identity_.set_stream_id(1004);
    }

    void set_multi_version_status(MultiVersionStatus mode) {
        multi_version_status_ = mode;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance;
        instance.set_instance_id(instance_id_);
        instance.set_multi_version_status(mode);
        txn->put(instance_key({instance_id_}), instance.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        auto [code, message] = service_->resource_mgr()->refresh_instance(instance_id_);
        ASSERT_EQ(code, MetaServiceCode::OK) << message;
    }

    void put_partition_mapping(Transaction* txn, int64_t partition_id) {
        PartitionIndexPB partition;
        partition.set_db_id(identity_.base_db_id());
        partition.set_table_id(identity_.base_table_id());
        txn->put(versioned::partition_index_key({instance_id_, partition_id}),
                 partition.SerializeAsString());
    }

    void put_latest_partition_state(Transaction* txn, int64_t partition_id, int64_t visible_version,
                                    int64_t visible_tso, std::optional<int64_t> offset_tso) {
        VersionPB version;
        version.set_version(visible_version);
        version.set_visible_tso(visible_tso);
        txn->put(partition_version_key({instance_id_, identity_.base_db_id(),
                                        identity_.base_table_id(), partition_id}),
                 version.SerializeAsString());
        if (offset_tso.has_value()) {
            TableStreamOffsetPB offset;
            offset.set_partition_id(partition_id);
            offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
            offset.set_offset_tso(*offset_tso);
            offset.set_last_consumption_time_ms(1234);
            txn->put(table_stream_offset_key({instance_id_, identity_.base_db_id(),
                                              identity_.base_table_id(), identity_.stream_db_id(),
                                              identity_.stream_id(), partition_id}),
                     offset.SerializeAsString());
        }
    }

    void put_versioned_partition_state(Transaction* txn, int64_t partition_id,
                                       int64_t visible_version, int64_t visible_tso,
                                       std::optional<int64_t> offset_tso) {
        put_partition_mapping(txn, partition_id);
        versioned_put(txn, versioned::meta_partition_key({instance_id_, partition_id}),
                      Versionstamp(41, 0), "");

        VersionPB version;
        version.set_version(visible_version);
        version.set_visible_tso(visible_tso);
        versioned_put(txn, versioned::partition_version_key({instance_id_, partition_id}),
                      Versionstamp(42, 0), version.SerializeAsString());
        if (offset_tso.has_value()) {
            TableStreamOffsetPB offset;
            offset.set_partition_id(partition_id);
            offset.set_state(TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING);
            offset.set_offset_tso(*offset_tso);
            versioned_put(txn,
                          versioned::table_stream_offset_key(
                                  {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
                                   identity_.stream_db_id(), identity_.stream_id(), partition_id}),
                          Versionstamp(43, 0), offset.SerializeAsString());
        }
    }

    GetTableStreamOffsetResponse get_read_state(const std::vector<int64_t>& partitions) {
        GetTableStreamOffsetRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        auto* binding = request.add_bindings();
        binding->mutable_identity()->CopyFrom(identity_);
        for (int64_t partition_id : partitions) {
            binding->add_partition_ids(partition_id);
        }
        GetTableStreamOffsetResponse response;
        brpc::Controller controller;
        service_->get_table_stream_offset(&controller, &request, &response, nullptr);
        return response;
    }

    GetTableStreamOffsetResponse get_read_state(std::initializer_list<int64_t> partitions) {
        return get_read_state(std::vector<int64_t>(partitions));
    }

    int64_t begin_target_transaction(const std::string& label) {
        BeginTxnRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        TxnInfoPB* txn_info = request.mutable_txn_info();
        txn_info->set_db_id(kTargetDbId);
        txn_info->set_label(label);
        txn_info->add_table_ids(kTargetTableId);
        txn_info->set_timeout_ms(36000);
        BeginTxnResponse response;
        brpc::Controller controller;
        service_->begin_txn(&controller, &request, &response, nullptr);
        EXPECT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
        return response.txn_id();
    }

    void create_target_tablet() {
        CreateTabletsRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        request.set_db_id(kTargetDbId);
        doris::TabletMetaCloudPB* tablet = request.add_tablet_metas();
        tablet->set_table_id(kTargetTableId);
        tablet->set_index_id(kTargetIndexId);
        tablet->set_partition_id(kTargetPartitionId);
        tablet->set_tablet_id(kTargetTabletId);
        doris::TabletSchemaCloudPB* schema = tablet->mutable_schema();
        schema->set_schema_version(0);
        doris::RowsetMetaCloudPB* first_rowset = tablet->add_rs_metas();
        first_rowset->set_rowset_id(0);
        first_rowset->set_rowset_id_v2("table_stream_target_initial");
        first_rowset->set_start_version(0);
        first_rowset->set_end_version(1);
        first_rowset->mutable_tablet_schema()->CopyFrom(*schema);

        CreateTabletsResponse response;
        brpc::Controller controller;
        service_->create_tablets(&controller, &request, &response, nullptr);
        ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    }

    void stage_target_rowset(int64_t txn_id) {
        doris::RowsetMetaCloudPB rowset;
        rowset.set_rowset_id(0);
        rowset.set_rowset_id_v2("table_stream_target_" + std::to_string(txn_id));
        rowset.set_tablet_id(kTargetTabletId);
        rowset.set_partition_id(kTargetPartitionId);
        rowset.set_index_id(kTargetIndexId);
        rowset.set_txn_id(txn_id);
        rowset.set_num_segments(1);
        rowset.set_num_rows(10);
        rowset.set_data_disk_size(100);
        rowset.set_total_disk_size(100);
        rowset.mutable_tablet_schema()->set_schema_version(0);
        rowset.set_txn_expiration(std::numeric_limits<int64_t>::max());

        CreateRowsetRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        request.mutable_rowset_meta()->CopyFrom(rowset);
        CreateRowsetResponse response;
        brpc::Controller prepare_controller;
        service_->prepare_rowset(&prepare_controller, &request, &response, nullptr);
        ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();

        response.Clear();
        brpc::Controller commit_controller;
        service_->commit_rowset(&commit_controller, &request, &response, nullptr);
        ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    }

    TxnInfoPB get_target_transaction(int64_t txn_id) {
        GetTxnRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        request.set_db_id(kTargetDbId);
        request.set_txn_id(txn_id);
        GetTxnResponse response;
        brpc::Controller controller;
        service_->get_txn(&controller, &request, &response, nullptr);
        EXPECT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
        return response.txn_info();
    }

    CommitTxnRequest make_consume_request(int64_t txn_id, int64_t partition_id,
                                          TableStreamOffsetStatePB expected_state,
                                          int64_t expected_tso, int64_t next_tso) {
        CommitTxnRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        request.set_db_id(kTargetDbId);
        request.set_txn_id(txn_id);
        TableStreamUpdatePB* stream_update = request.add_table_stream_updates();
        stream_update->mutable_identity()->CopyFrom(identity_);
        TableStreamPartitionUpdatePB* partition_update = stream_update->add_partition_updates();
        partition_update->set_partition_id(partition_id);
        partition_update->set_expected_state(expected_state);
        if (expected_state != TABLE_STREAM_OFFSET_UNKNOWN) {
            partition_update->set_expected_offset_tso(expected_tso);
        }
        partition_update->set_next_offset_tso(next_tso);
        return request;
    }

    CommitTxnResponse commit_transaction(const CommitTxnRequest& request) {
        CommitTxnResponse response;
        brpc::Controller controller;
        service_->commit_txn(&controller, &request, &response, nullptr);
        return response;
    }

    CommitTxnResponse consume_partition(int64_t txn_id, int64_t partition_id,
                                        TableStreamOffsetStatePB expected_state,
                                        int64_t expected_tso, int64_t next_tso) {
        return commit_transaction(
                make_consume_request(txn_id, partition_id, expected_state, expected_tso, next_tso));
    }

    PartitionResponse drop_base_partition(int64_t partition_id) {
        PartitionRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        request.set_db_id(identity_.base_db_id());
        request.set_table_id(identity_.base_table_id());
        request.add_index_ids(kBaseIndexId);
        request.add_partition_ids(partition_id);
        request.add_table_streams()->CopyFrom(identity_);
        request.set_expiration(0);
        PartitionResponse response;
        brpc::Controller controller;
        service_->drop_partition(&controller, &request, &response, nullptr);
        return response;
    }

    std::pair<CommitTxnResponse, CommitTxnResponse> commit_concurrently(
            const CommitTxnRequest& first_request, const CommitTxnRequest& second_request) {
        std::mutex mutex;
        std::condition_variable cv;
        int arrived = 0;
        auto* sync_point = SyncPoint::get_instance();
        DORIS_CLOUD_DEFER {
            sync_point->disable_processing();
            sync_point->clear_all_call_backs();
        };
        sync_point->set_call_back("commit_txn_immediately::before_commit", [&](auto&&) {
            std::unique_lock lock(mutex);
            ++arrived;
            cv.notify_all();
            EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(30), [&] { return arrived == 2; }));
        });
        sync_point->enable_processing();

        CommitTxnResponse first_response;
        CommitTxnResponse second_response;
        std::thread first_thread(
                [&] { first_response = commit_transaction(first_request); });
        std::thread second_thread(
                [&] { second_response = commit_transaction(second_request); });
        first_thread.join();
        second_thread.join();
        EXPECT_EQ(arrived, 2);
        return {std::move(first_response), std::move(second_response)};
    }

    TableStreamOffsetPB get_latest_offset(const TableStreamIdentityPB& identity,
                                          int64_t partition_id) {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        EXPECT_EQ(txn->get(table_stream_offset_key(
                                   {instance_id_, identity.base_db_id(), identity.base_table_id(),
                                    identity.stream_db_id(), identity.stream_id(), partition_id}),
                           &value),
                  TxnErrorCode::TXN_OK);
        TableStreamOffsetPB offset;
        EXPECT_TRUE(offset.ParseFromString(value));
        return offset;
    }

    TableStreamOffsetPB get_latest_offset(int64_t partition_id) {
        return get_latest_offset(identity_, partition_id);
    }

    void set_clone_source(const std::string& source_instance_id, Versionstamp snapshot_version) {
        multi_version_status_ = MULTI_VERSION_READ_WRITE;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        InstanceInfoPB instance;
        instance.set_instance_id(instance_id_);
        instance.set_multi_version_status(MULTI_VERSION_READ_WRITE);
        instance.set_source_instance_id(source_instance_id);
        instance.set_source_snapshot_id(SnapshotManager::serialize_snapshot_id(snapshot_version));
        txn->put(instance_key({instance_id_}), instance.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        auto [code, message] = service_->resource_mgr()->refresh_instance(instance_id_);
        ASSERT_EQ(code, MetaServiceCode::OK) << message;
    }

    Versionstamp put_auto_versioned_offset(const std::string& target_instance_id,
                                           const TableStreamIdentityPB& identity,
                                           int64_t partition_id, int64_t offset_tso,
                                           bool write_latest = false) {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        TableStreamOffsetPB offset;
        offset.set_partition_id(partition_id);
        offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
        offset.set_offset_tso(offset_tso);
        const TableStreamOffsetKeyInfo key_info {target_instance_id,       identity.base_db_id(),
                                                 identity.base_table_id(), identity.stream_db_id(),
                                                 identity.stream_id(),     partition_id};
        const std::string value = offset.SerializeAsString();
        if (write_latest) {
            txn->put(table_stream_offset_key(key_info), value);
        }
        txn->enable_get_versionstamp();
        versioned_put(txn.get(), versioned::table_stream_offset_key(key_info), value);
        EXPECT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        Versionstamp version;
        EXPECT_EQ(txn->get_versionstamp(&version), TxnErrorCode::TXN_OK);
        return version;
    }

    std::unique_ptr<MetaServiceProxy> service_;
    std::string instance_id_;
    std::string cloud_unique_id_;
    TableStreamIdentityPB identity_;
    MultiVersionStatus multi_version_status_ = MULTI_VERSION_DISABLED;
};

TEST_F(MetaServiceTableStreamTest, ReadLatestAndUnknownOffsets) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    put_latest_partition_state(txn.get(), 2002, 9, 140, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state({2001, 2002});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 2);

    const auto& consumed = response.bindings(0).partition_states(0);
    EXPECT_EQ(consumed.partition_id(), 2001);
    EXPECT_EQ(consumed.offset_state(), TABLE_STREAM_OFFSET_CONSUMED);
    EXPECT_EQ(consumed.offset_tso(), 100);
    EXPECT_EQ(consumed.end_tso(), 130);
    EXPECT_EQ(consumed.visible_version(), 8);
    EXPECT_EQ(consumed.last_consumption_time_ms(), 1234);

    const auto& unknown = response.bindings(0).partition_states(1);
    EXPECT_EQ(unknown.partition_id(), 2002);
    EXPECT_EQ(unknown.offset_state(), TABLE_STREAM_OFFSET_UNKNOWN);
    EXPECT_FALSE(unknown.has_offset_tso());
    EXPECT_EQ(unknown.end_tso(), 140);
    EXPECT_EQ(unknown.visible_version(), 9);
}

TEST_F(MetaServiceTableStreamTest, ReadVersionAndOffsetFromOneSnapshot) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    bool updated = false;
    sync_point->set_call_back("get_table_stream_offset::after_read_partition_versions",
                              [&](auto&&) {
                                  ASSERT_FALSE(updated);
                                  updated = true;
                                  std::unique_ptr<Transaction> update_txn;
                                  ASSERT_EQ(service_->txn_kv()->create_txn(&update_txn),
                                            TxnErrorCode::TXN_OK);
                                  put_latest_partition_state(update_txn.get(), 2001, 9, 140, 110);
                                  ASSERT_EQ(update_txn->commit(), TxnErrorCode::TXN_OK);
                              });
    sync_point->enable_processing();

    GetTableStreamOffsetResponse response = get_read_state({2001});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_TRUE(updated);
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 1);
    const TableStreamPartitionReadStatePB& state = response.bindings(0).partition_states(0);
    EXPECT_EQ(state.visible_version(), 8);
    EXPECT_EQ(state.end_tso(), 130);
    EXPECT_EQ(state.offset_tso(), 100);

    sync_point->disable_processing();
    sync_point->clear_all_call_backs();
    response = get_read_state({2001});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    const TableStreamPartitionReadStatePB& next_state =
            response.bindings(0).partition_states(0);
    EXPECT_EQ(next_state.visible_version(), 9);
    EXPECT_EQ(next_state.end_tso(), 140);
    EXPECT_EQ(next_state.offset_tso(), 110);
}

TEST_F(MetaServiceTableStreamTest, UnknownStreamIdReliesOnFeCatalogAuthority) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    identity_.set_stream_id(9999);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state({2001});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 1);
    EXPECT_EQ(response.bindings(0).partition_states(0).offset_state(),
              TABLE_STREAM_OFFSET_UNKNOWN);

    CommitTxnResponse commit_response =
            consume_partition(begin_target_transaction("consume-fe-authoritative-stream"), 2001,
                              TABLE_STREAM_OFFSET_UNKNOWN, 0, 120);
    ASSERT_EQ(commit_response.status().code(), MetaServiceCode::OK)
            << commit_response.status().msg();
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 120);
}

TEST_F(MetaServiceTableStreamTest, ReadVersionedState) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_versioned_partition_state(txn.get(), 2001, 18, 230, 200);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state({2001});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 1);
    const auto& state = response.bindings(0).partition_states(0);
    EXPECT_EQ(state.offset_state(), TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING);
    EXPECT_EQ(state.offset_tso(), 200);
    EXPECT_EQ(state.end_tso(), 230);
    EXPECT_EQ(state.visible_version(), 18);
}

TEST_F(MetaServiceTableStreamTest, ReadVersionedStateFromCloneChainInBatch) {
    const std::string source_instance_id = "table_stream_batch_read_source";
    set_clone_source(source_instance_id, Versionstamp(100, 0));
    const std::vector<int64_t> partition_ids = {2001, 2002, 2003};

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (size_t i = 0; i < partition_ids.size(); ++i) {
        int64_t partition_id = partition_ids[i];
        PartitionIndexPB partition;
        partition.set_db_id(identity_.base_db_id());
        partition.set_table_id(identity_.base_table_id());
        txn->put(versioned::partition_index_key({source_instance_id, partition_id}),
                 partition.SerializeAsString());
        versioned_put(txn.get(), versioned::meta_partition_key({source_instance_id, partition_id}),
                      Versionstamp(51 + i, 0), "");

        VersionPB version;
        version.set_version(10 + i);
        version.set_visible_tso(1000 + i);
        versioned_put(txn.get(),
                      versioned::partition_version_key({source_instance_id, partition_id}),
                      Versionstamp(60 + i, 0), version.SerializeAsString());
    }

    auto put_offset = [&](const std::string& target_instance_id, int64_t partition_id,
                          Versionstamp versionstamp, int64_t offset_tso) {
        TableStreamOffsetPB offset;
        offset.set_partition_id(partition_id);
        offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
        offset.set_offset_tso(offset_tso);
        versioned_put(
                txn.get(),
                versioned::table_stream_offset_key(
                        {target_instance_id, identity_.base_db_id(), identity_.base_table_id(),
                         identity_.stream_db_id(), identity_.stream_id(), partition_id}),
                versionstamp, offset.SerializeAsString());
    };
    put_offset(source_instance_id, 2001, Versionstamp(70, 0), 701);
    put_offset(source_instance_id, 2002, Versionstamp(71, 0), 702);
    put_offset(instance_id_, 2002, Versionstamp(200, 0), 802);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state(partition_ids);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings(0).partition_states_size(), partition_ids.size());
    EXPECT_EQ(response.bindings(0).partition_states(0).offset_tso(), 701);
    EXPECT_EQ(response.bindings(0).partition_states(1).offset_tso(), 802);
    EXPECT_EQ(response.bindings(0).partition_states(2).offset_state(), TABLE_STREAM_OFFSET_UNKNOWN);
}

TEST_F(MetaServiceTableStreamTest, ReadLargePartitionBatch) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    constexpr int kPartitionCount = 1201;
    std::vector<int64_t> partition_ids;
    partition_ids.reserve(kPartitionCount);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (int i = 0; i < kPartitionCount; ++i) {
        int64_t partition_id = 10000 + i;
        partition_ids.push_back(partition_id);
        put_latest_partition_state(txn.get(), partition_id, i + 1, 100000 + i, 90000 + i);
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state(partition_ids);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings(0).partition_states_size(), kPartitionCount);
    EXPECT_EQ(response.bindings(0).partition_states(0).partition_id(), partition_ids.front());
    EXPECT_EQ(response.bindings(0).partition_states(0).offset_tso(), 90000);
    EXPECT_EQ(response.bindings(0).partition_states(kPartitionCount - 1).partition_id(),
              partition_ids.back());
    EXPECT_EQ(response.bindings(0).partition_states(kPartitionCount - 1).offset_tso(),
              90000 + kPartitionCount - 1);
}

TEST_F(MetaServiceTableStreamTest, ReadMultipleBindingsInBatch) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    constexpr int kBindingCount = 101;
    constexpr int64_t kPartitionId = 2001;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    PartitionIndexPB partition;
    partition.set_db_id(identity_.base_db_id());
    partition.set_table_id(identity_.base_table_id());
    txn->put(versioned::partition_index_key({instance_id_, kPartitionId}),
             partition.SerializeAsString());
    versioned_put(txn.get(), versioned::meta_partition_key({instance_id_, kPartitionId}),
                  Versionstamp(20, 0), "");
    VersionPB version;
    version.set_version(8);
    version.set_visible_tso(130);
    versioned_put(txn.get(), versioned::partition_version_key({instance_id_, kPartitionId}),
                  Versionstamp(21, 0), version.SerializeAsString());

    GetTableStreamOffsetRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    for (int i = 0; i < kBindingCount; ++i) {
        int64_t stream_id = identity_.stream_id() + i;
        TableStreamPartitionSetPB* binding = request.add_bindings();
        binding->mutable_identity()->CopyFrom(identity_);
        binding->mutable_identity()->set_stream_id(stream_id);
        binding->add_partition_ids(kPartitionId);
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response;
    brpc::Controller controller;
    service_->get_table_stream_offset(&controller, &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings_size(), kBindingCount);
    for (const TableStreamReadBindingResultPB& binding : response.bindings()) {
        ASSERT_EQ(binding.partition_states_size(), 1);
        EXPECT_EQ(binding.partition_states(0).offset_state(), TABLE_STREAM_OFFSET_UNKNOWN);
    }
}

TEST_F(MetaServiceTableStreamTest, ReadWriteOnlyState) {
    set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 28, 330, 300);
    TableStreamOffsetPB stale_versioned_offset;
    stale_versioned_offset.set_partition_id(2001);
    stale_versioned_offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    stale_versioned_offset.set_offset_tso(250);
    const std::string versioned_offset_key = versioned::table_stream_offset_key(
            {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
             identity_.stream_db_id(), identity_.stream_id(), 2001});
    txn->enable_get_versionstamp();
    versioned_put(txn.get(), versioned_offset_key, stale_versioned_offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state({2001});
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    ASSERT_EQ(response.bindings_size(), 1);
    ASSERT_EQ(response.bindings(0).partition_states_size(), 1);
    const auto& state = response.bindings(0).partition_states(0);
    EXPECT_EQ(state.offset_state(), TABLE_STREAM_OFFSET_CONSUMED);
    EXPECT_EQ(state.offset_tso(), 300);
    EXPECT_EQ(state.end_tso(), 330);
    EXPECT_EQ(state.visible_version(), 28);

    CommitTxnResponse commit_response =
            consume_partition(begin_target_transaction("consume-write-only"), 2001,
                              TABLE_STREAM_OFFSET_CONSUMED, 300, 320);
    ASSERT_EQ(commit_response.status().code(), MetaServiceCode::OK)
            << commit_response.status().msg();
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 320);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    Versionstamp offset_version;
    std::string value;
    ASSERT_EQ(versioned_get(txn.get(), versioned_offset_key, &offset_version, &value),
              TxnErrorCode::TXN_OK);
    TableStreamOffsetPB versioned_offset;
    ASSERT_TRUE(versioned_offset.ParseFromString(value));
    EXPECT_EQ(versioned_offset.offset_tso(), 320);
}

TEST_F(MetaServiceTableStreamTest, RejectDuplicateBindingsAndPartitions) {
    GetTableStreamOffsetRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    auto* binding = request.add_bindings();
    binding->mutable_identity()->CopyFrom(identity_);
    binding->add_partition_ids(2001);
    binding->add_partition_ids(2001);
    GetTableStreamOffsetResponse response;
    brpc::Controller controller;
    service_->get_table_stream_offset(&controller, &request, &response, nullptr);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);

    request.mutable_bindings()->Clear();
    for (int i = 0; i < 2; ++i) {
        binding = request.add_bindings();
        binding->mutable_identity()->CopyFrom(identity_);
        binding->add_partition_ids(2001 + i);
    }
    response.Clear();
    brpc::Controller second_controller;
    service_->get_table_stream_offset(&second_controller, &request, &response, nullptr);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
}

TEST_F(MetaServiceTableStreamTest, RejectEnabledModeAndRecyclingStream) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    InstanceInfoPB instance;
    instance.set_instance_id(instance_id_);
    instance.set_multi_version_status(MULTI_VERSION_ENABLED);
    txn->put(instance_key({instance_id_}), instance.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    GetTableStreamOffsetResponse response = get_read_state({2001});
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);

    set_multi_version_status(MULTI_VERSION_DISABLED);
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    RecycleIndexPB recycle_index;
    recycle_index.set_table_id(identity_.base_table_id());
    recycle_index.set_state(RecycleIndexPB::RECYCLING);
    recycle_index.set_object_type(TABLE_STREAM);
    txn->put(recycle_index_key({instance_id_, identity_.stream_id()}),
             recycle_index.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    response = get_read_state({2001});
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
}

TEST_F(MetaServiceTableStreamTest, CommitUpdatesLatestOffsetAndIsIdempotent) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    put_latest_partition_state(txn.get(), 2002, 9, 140, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-existing-offset");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    TableStreamOffsetPB offset = get_latest_offset(2001);
    EXPECT_EQ(offset.state(), TABLE_STREAM_OFFSET_CONSUMED);
    EXPECT_EQ(offset.offset_tso(), 120);
    EXPECT_GT(offset.last_consumption_time_ms(), 0);

    response = consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 120);

    txn_id = begin_target_transaction("consume-unknown-offset");
    response = consume_partition(txn_id, 2002, TABLE_STREAM_OFFSET_UNKNOWN, 0, 140);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(2002).offset_tso(), 140);
}

TEST_F(MetaServiceTableStreamTest, CommitTargetRowsetAndOffsetAtomicallyAndIdempotently) {
    set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
    create_target_tablet();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    put_auto_versioned_offset(instance_id_, identity_, 2001, 100, true);
    const std::string versioned_offset_key = versioned::table_stream_offset_key(
            {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
             identity_.stream_db_id(), identity_.stream_id(), 2001});
    auto get_versioned_offset = [&]() {
        std::unique_ptr<Transaction> read_txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&read_txn), TxnErrorCode::TXN_OK);
        Versionstamp version;
        std::string value;
        EXPECT_EQ(versioned_get(read_txn.get(), versioned_offset_key, &version, &value),
                  TxnErrorCode::TXN_OK);
        TableStreamOffsetPB offset;
        EXPECT_TRUE(offset.ParseFromString(value));
        return offset;
    };

    const int64_t txn_id = begin_target_transaction("consume-target-rowset-atomically");
    stage_target_rowset(txn_id);
    const std::string target_rowset_key =
            meta_rowset_key({instance_id_, kTargetTabletId, 2});
    const std::string next_target_rowset_key =
            meta_rowset_key({instance_id_, kTargetTabletId, 3});
    const std::string tmp_rowset_key =
            meta_rowset_tmp_key({instance_id_, txn_id, kTargetTabletId});
    const std::string target_version_key = partition_version_key(
            {instance_id_, kTargetDbId, kTargetTableId, kTargetPartitionId});

    CommitTxnRequest stale_request =
            make_consume_request(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 99, 120);
    CommitTxnResponse response = commit_transaction(stale_request);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_target_transaction(txn_id).status(), TxnStatusPB::TXN_STATUS_PREPARED);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);
    EXPECT_EQ(get_versioned_offset().offset_tso(), 100);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(target_rowset_key, &value), TxnErrorCode::TXN_KEY_NOT_FOUND);
    EXPECT_EQ(txn->get(target_version_key, &value), TxnErrorCode::TXN_KEY_NOT_FOUND);
    EXPECT_EQ(txn->get(tmp_rowset_key, &value), TxnErrorCode::TXN_OK);

    CommitTxnRequest request =
            make_consume_request(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    response = commit_transaction(request);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_target_transaction(txn_id).status(), TxnStatusPB::TXN_STATUS_VISIBLE);
    const TableStreamOffsetPB committed_offset = get_latest_offset(2001);
    ASSERT_EQ(committed_offset.offset_tso(), 120);
    const TableStreamOffsetPB committed_versioned_offset = get_versioned_offset();
    ASSERT_EQ(committed_versioned_offset.offset_tso(), 120);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string committed_rowset;
    ASSERT_EQ(txn->get(target_rowset_key, &committed_rowset), TxnErrorCode::TXN_OK);
    EXPECT_EQ(txn->get(next_target_rowset_key, &value), TxnErrorCode::TXN_KEY_NOT_FOUND);
    EXPECT_EQ(txn->get(tmp_rowset_key, &value), TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(target_version_key, &value), TxnErrorCode::TXN_OK);
    VersionPB target_version;
    ASSERT_TRUE(target_version.ParseFromString(value));
    EXPECT_EQ(target_version.version(), 2);

    response = commit_transaction(request);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_target_transaction(txn_id).status(), TxnStatusPB::TXN_STATUS_VISIBLE);
    EXPECT_EQ(get_latest_offset(2001).SerializeAsString(), committed_offset.SerializeAsString());
    EXPECT_EQ(get_versioned_offset().SerializeAsString(),
              committed_versioned_offset.SerializeAsString());

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(target_rowset_key, &value), TxnErrorCode::TXN_OK);
    EXPECT_EQ(value, committed_rowset);
    EXPECT_EQ(txn->get(next_target_rowset_key, &value), TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(target_version_key, &value), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(target_version.ParseFromString(value));
    EXPECT_EQ(target_version.version(), 2);
}

TEST_F(MetaServiceTableStreamTest, CommitSourceVersionUsesSnapshotRead) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    create_target_tablet();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const int64_t txn_id = begin_target_transaction("consume-while-source-publishes");
    stage_target_rowset(txn_id);

    std::mutex mutex;
    std::condition_variable cv;
    bool commit_ready = false;
    bool resume_commit = false;
    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("commit_txn_immediately::before_commit", [&](auto&&) {
        std::unique_lock lock(mutex);
        commit_ready = true;
        cv.notify_all();
        cv.wait(lock, [&] { return resume_commit; });
    });
    sync_point->enable_processing();

    CommitTxnResponse response;
    std::thread commit_thread([&] {
        response = consume_partition(
                txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    });
    DORIS_CLOUD_DEFER {
        {
            std::lock_guard lock(mutex);
            resume_commit = true;
        }
        cv.notify_all();
        if (commit_thread.joinable()) {
            commit_thread.join();
        }
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };

    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(30), [&] { return commit_ready; }));
    }

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 9, 140, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    {
        std::lock_guard lock(mutex);
        resume_commit = true;
    }
    cv.notify_all();
    commit_thread.join();

    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 120);
    EXPECT_EQ(get_target_transaction(txn_id).status(), TxnStatusPB::TXN_STATUS_VISIBLE);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_OK);
}

TEST_F(MetaServiceTableStreamTest, CreatePartitionCommitConflictsWithPartitionRecycle) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    IndexRequest prepare_request;
    prepare_request.set_cloud_unique_id(cloud_unique_id_);
    prepare_request.set_db_id(identity_.base_db_id());
    prepare_request.set_table_id(identity_.base_table_id());
    prepare_request.set_stream_db_id(identity_.stream_db_id());
    prepare_request.add_index_ids(identity_.stream_id());
    prepare_request.set_object_type(TABLE_STREAM);
    prepare_request.set_expiration(0);
    IndexResponse prepare_response;
    brpc::Controller prepare_controller;
    service_->prepare_index(&prepare_controller, &prepare_request, &prepare_response, nullptr);
    ASSERT_EQ(prepare_response.status().code(), MetaServiceCode::OK)
            << prepare_response.status().msg();

    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    bool recycle_partition_written = false;
    sync_point->set_call_back("commit_table_stream_partition::before_commit", [&](auto&&) {
        ASSERT_FALSE(recycle_partition_written);
        PartitionResponse response = drop_base_partition(2001);
        ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
        recycle_partition_written = true;
    });
    sync_point->enable_processing();

    PartitionRequest partition_request;
    partition_request.set_cloud_unique_id(cloud_unique_id_);
    partition_request.set_db_id(identity_.base_db_id());
    partition_request.set_table_id(identity_.base_table_id());
    partition_request.set_stream_db_id(identity_.stream_db_id());
    partition_request.add_index_ids(identity_.stream_id());
    partition_request.set_object_type(TABLE_STREAM);
    partition_request.add_partition_ids(2001);
    TableStreamOffsetPB* offset = partition_request.add_table_stream_offsets();
    offset->set_partition_id(2001);
    offset->set_state(TABLE_STREAM_OFFSET_CONSUMED);
    offset->set_offset_tso(100);
    PartitionResponse partition_response;
    brpc::Controller partition_controller;
    service_->commit_partition(&partition_controller, &partition_request, &partition_response,
                               nullptr);

    EXPECT_NE(partition_response.status().code(), MetaServiceCode::OK);
    EXPECT_TRUE(recycle_partition_written);
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(table_stream_offset_key(
                               {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
                                identity_.stream_db_id(), identity_.stream_id(), 2001}),
                       &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    ASSERT_EQ(txn->get(recycle_index_key({instance_id_, identity_.stream_id()}), &value),
              TxnErrorCode::TXN_OK);
    RecycleIndexPB recycle_index;
    ASSERT_TRUE(recycle_index.ParseFromString(value));
    EXPECT_EQ(recycle_index.state(), RecycleIndexPB::PREPARED);
}

TEST_F(MetaServiceTableStreamTest, ConcurrentConsumersSerializeExistingOffset) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const int64_t first_txn_id = begin_target_transaction("consume-existing-offset-first");
    const int64_t second_txn_id = begin_target_transaction("consume-existing-offset-second");
    auto [first_response, second_response] = commit_concurrently(
            make_consume_request(first_txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120),
            make_consume_request(second_txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 130));

    const bool first_succeeded = first_response.status().code() == MetaServiceCode::OK;
    const bool second_succeeded = second_response.status().code() == MetaServiceCode::OK;
    EXPECT_NE(first_succeeded, second_succeeded);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), first_succeeded ? 120 : 130);
    EXPECT_EQ(get_target_transaction(first_txn_id).status(),
              first_succeeded ? TxnStatusPB::TXN_STATUS_VISIBLE
                              : TxnStatusPB::TXN_STATUS_PREPARED);
    EXPECT_EQ(get_target_transaction(second_txn_id).status(),
              second_succeeded ? TxnStatusPB::TXN_STATUS_VISIBLE
                               : TxnStatusPB::TXN_STATUS_PREPARED);
}

TEST_F(MetaServiceTableStreamTest, ConcurrentFirstConsumersSerializeMissingOffset) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, std::nullopt);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const int64_t first_txn_id = begin_target_transaction("consume-missing-offset-first");
    const int64_t second_txn_id = begin_target_transaction("consume-missing-offset-second");
    auto [first_response, second_response] = commit_concurrently(
            make_consume_request(first_txn_id, 2001, TABLE_STREAM_OFFSET_UNKNOWN, 0, 120),
            make_consume_request(second_txn_id, 2001, TABLE_STREAM_OFFSET_UNKNOWN, 0, 130));

    const bool first_succeeded = first_response.status().code() == MetaServiceCode::OK;
    const bool second_succeeded = second_response.status().code() == MetaServiceCode::OK;
    EXPECT_NE(first_succeeded, second_succeeded);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), first_succeeded ? 120 : 130);
    EXPECT_EQ(get_target_transaction(first_txn_id).status(),
              first_succeeded ? TxnStatusPB::TXN_STATUS_VISIBLE
                              : TxnStatusPB::TXN_STATUS_PREPARED);
    EXPECT_EQ(get_target_transaction(second_txn_id).status(),
              second_succeeded ? TxnStatusPB::TXN_STATUS_VISIBLE
                               : TxnStatusPB::TXN_STATUS_PREPARED);
}

TEST_F(MetaServiceTableStreamTest, CommitConflictsWithConcurrentPartitionRecycle) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    create_target_tablet();
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const int64_t txn_id = begin_target_transaction("consume-while-partition-recycled");
    stage_target_rowset(txn_id);
    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    bool recycle_partition_written = false;
    sync_point->set_call_back("commit_txn_immediately::before_commit", [&](auto&&) {
        ASSERT_FALSE(recycle_partition_written);
        PartitionResponse drop_response = drop_base_partition(2001);
        ASSERT_EQ(drop_response.status().code(), MetaServiceCode::OK)
                << drop_response.status().msg();
        recycle_partition_written = true;
    });
    sync_point->enable_processing();

    CommitTxnResponse response = consume_partition(
            txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    EXPECT_NE(response.status().code(), MetaServiceCode::OK);
    EXPECT_TRUE(recycle_partition_written);
    EXPECT_EQ(get_target_transaction(txn_id).status(), TxnStatusPB::TXN_STATUS_PREPARED);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    EXPECT_EQ(txn->get(meta_rowset_tmp_key({instance_id_, txn_id, kTargetTabletId}), &value),
              TxnErrorCode::TXN_OK);
}

TEST_F(MetaServiceTableStreamTest, CommitConflictsAfterStreamRecycleIndexIsCreated) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    create_target_tablet();
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const int64_t txn_id = begin_target_transaction("consume-while-stream-dropped");
    stage_target_rowset(txn_id);
    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    bool stream_dropped = false;
    sync_point->set_call_back("commit_txn_immediately::before_commit", [&](auto&&) {
        ASSERT_FALSE(stream_dropped);
        IndexRequest drop_request;
        drop_request.set_cloud_unique_id(cloud_unique_id_);
        drop_request.add_index_ids(identity_.stream_id());
        drop_request.set_db_id(identity_.base_db_id());
        drop_request.set_table_id(identity_.base_table_id());
        drop_request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
        drop_request.set_stream_db_id(identity_.stream_db_id());
        drop_request.set_expiration(0);
        IndexResponse drop_response;
        brpc::Controller drop_controller;
        service_->drop_index(&drop_controller, &drop_request, &drop_response, nullptr);
        ASSERT_EQ(drop_response.status().code(), MetaServiceCode::OK)
                << drop_response.status().msg();
        stream_dropped = true;
    });
    sync_point->enable_processing();

    CommitTxnResponse response = consume_partition(
            txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    EXPECT_NE(response.status().code(), MetaServiceCode::OK);
    EXPECT_TRUE(stream_dropped);
    EXPECT_EQ(get_target_transaction(txn_id).status(), TxnStatusPB::TXN_STATUS_PREPARED);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    EXPECT_EQ(txn->get(meta_rowset_key({instance_id_, kTargetTabletId, 2}), &value),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
    EXPECT_EQ(txn->get(meta_rowset_tmp_key({instance_id_, txn_id, kTargetTabletId}), &value),
              TxnErrorCode::TXN_OK);
}

TEST_F(MetaServiceTableStreamTest, CommitMultipleStreamsAtomically) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    TableStreamIdentityPB second_identity = identity_;
    second_identity.set_stream_db_id(1005);
    second_identity.set_stream_id(1006);

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    TableStreamOffsetPB second_offset;
    second_offset.set_partition_id(2001);
    second_offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    second_offset.set_offset_tso(105);
    second_offset.set_last_consumption_time_ms(1234);
    txn->put(table_stream_offset_key(
                     {instance_id_, second_identity.base_db_id(), second_identity.base_table_id(),
                      second_identity.stream_db_id(), second_identity.stream_id(), 2001}),
             second_offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    auto make_request = [&](int64_t txn_id, int64_t second_expected_tso) {
        CommitTxnRequest request =
                make_consume_request(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
        TableStreamUpdatePB* second_stream_update = request.add_table_stream_updates();
        second_stream_update->mutable_identity()->CopyFrom(second_identity);
        TableStreamPartitionUpdatePB* second_partition_update =
                second_stream_update->add_partition_updates();
        second_partition_update->set_partition_id(2001);
        second_partition_update->set_expected_state(TABLE_STREAM_OFFSET_CONSUMED);
        second_partition_update->set_expected_offset_tso(second_expected_tso);
        second_partition_update->set_next_offset_tso(125);
        return request;
    };

    CommitTxnResponse response = commit_transaction(
            make_request(begin_target_transaction("consume-multiple-streams-stale"), 104));
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(identity_, 2001).offset_tso(), 100);
    EXPECT_EQ(get_latest_offset(second_identity, 2001).offset_tso(), 105);

    response = commit_transaction(
            make_request(begin_target_transaction("consume-multiple-streams"), 105));
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(identity_, 2001).offset_tso(), 120);
    EXPECT_EQ(get_latest_offset(second_identity, 2001).offset_tso(), 125);
}

TEST_F(MetaServiceTableStreamTest, CommitLargePartitionBatch) {
    set_multi_version_status(MULTI_VERSION_WRITE_ONLY);
    constexpr int kPartitionCount = 1201;

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (int i = 0; i < kPartitionCount; ++i) {
        int64_t partition_id = 30000 + i;
        put_latest_partition_state(txn.get(), partition_id, i + 1, 40000 + i, std::nullopt);
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    CommitTxnRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.set_db_id(3001);
    request.set_txn_id(begin_target_transaction("consume-large-partition-batch"));
    TableStreamUpdatePB* stream_update = request.add_table_stream_updates();
    stream_update->mutable_identity()->CopyFrom(identity_);
    for (int i = 0; i < kPartitionCount; ++i) {
        int64_t partition_id = 30000 + i;
        TableStreamPartitionUpdatePB* update = stream_update->add_partition_updates();
        update->set_partition_id(partition_id);
        update->set_expected_state(TABLE_STREAM_OFFSET_UNKNOWN);
        update->set_next_offset_tso(40000 + i);
    }

    CommitTxnResponse response = commit_transaction(request);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(30000).offset_tso(), 40000);
    EXPECT_EQ(get_latest_offset(30000 + kPartitionCount / 2).offset_tso(),
              40000 + kPartitionCount / 2);
    EXPECT_EQ(get_latest_offset(30000 + kPartitionCount - 1).offset_tso(),
              40000 + kPartitionCount - 1);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsStaleAndInvalidOffsets) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-stale-offset");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 99, 120);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);

    txn_id = begin_target_transaction("consume-backward-offset");
    response = consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 90);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);

    txn_id = begin_target_transaction("consume-beyond-visible-tso");
    response = consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 131);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsDuplicatePartitionUpdates) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    const int64_t txn_id = begin_target_transaction("consume-duplicate-partition-update");
    CommitTxnRequest request =
            make_consume_request(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    TableStreamPartitionUpdatePB duplicate_update =
            request.table_stream_updates(0).partition_updates(0);
    request.mutable_table_stream_updates(0)->add_partition_updates()->CopyFrom(duplicate_update);

    CommitTxnResponse response = commit_transaction(request);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_target_transaction(txn_id).status(), TxnStatusPB::TXN_STATUS_PREPARED);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);
}

TEST_F(MetaServiceTableStreamTest, CommitUsesCloneEffectiveOffset) {
    const std::string source_instance_id = "table_stream_read_state_source";
    set_clone_source(source_instance_id, Versionstamp(100, 0));

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    PartitionIndexPB partition_index;
    partition_index.set_db_id(identity_.base_db_id());
    partition_index.set_table_id(identity_.base_table_id());
    txn->put(versioned::partition_index_key({source_instance_id, 2001}),
             partition_index.SerializeAsString());
    versioned_put(txn.get(), versioned::meta_partition_key({source_instance_id, 2001}),
                  Versionstamp(41, 0), "");
    VersionPB version;
    version.set_version(18);
    version.set_visible_tso(230);
    versioned_put(txn.get(), versioned::partition_version_key({source_instance_id, 2001}),
                  Versionstamp(42, 0), version.SerializeAsString());
    TableStreamOffsetPB inherited_offset;
    inherited_offset.set_partition_id(2001);
    inherited_offset.set_state(TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING);
    inherited_offset.set_offset_tso(200);
    versioned_put(txn.get(),
                  versioned::table_stream_offset_key(
                          {source_instance_id, identity_.base_db_id(), identity_.base_table_id(),
                           identity_.stream_db_id(), identity_.stream_id(), 2001}),
                  Versionstamp(43, 0), inherited_offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-inherited-offset-as-unknown");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_UNKNOWN, 0, 220);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);

    txn_id = begin_target_transaction("consume-inherited-offset");
    response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_INITIAL_SNAPSHOT_PENDING, 200, 220);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 220);

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    Versionstamp offset_version;
    ASSERT_EQ(
            versioned_get(txn.get(),
                          versioned::table_stream_offset_key(
                                  {instance_id_, identity_.base_db_id(), identity_.base_table_id(),
                                   identity_.stream_db_id(), identity_.stream_id(), 2001}),
                          &offset_version, &value),
            TxnErrorCode::TXN_OK);
    TableStreamOffsetPB versioned_offset;
    ASSERT_TRUE(versioned_offset.ParseFromString(value));
    EXPECT_EQ(versioned_offset.offset_tso(), 220);

    OperationLogPB operation_log;
    Versionstamp log_version;
    ASSERT_EQ(read_operation_log(txn.get(), versioned::log_key({instance_id_}), &log_version,
                                 &operation_log),
              TxnErrorCode::TXN_OK);
    ASSERT_TRUE(operation_log.has_commit_txn());
    ASSERT_EQ(operation_log.commit_txn().table_stream_offset_gc_size(), 1);
    const TableStreamPartitionSetPB& offset_gc =
            operation_log.commit_txn().table_stream_offset_gc(0);
    EXPECT_EQ(offset_gc.identity().SerializeAsString(), identity_.SerializeAsString());
    ASSERT_EQ(offset_gc.partition_ids_size(), 1);
    EXPECT_EQ(offset_gc.partition_ids(0), 2001);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsLatestOffsetWithoutVersionedOffset) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_versioned_partition_state(txn.get(), 2001, 8, 130, std::nullopt);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-inconsistent-local-offset");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_UNKNOWN, 0, 120);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsDifferentLatestAndVersionedOffsetValues) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_versioned_partition_state(txn.get(), 2001, 8, 130, std::nullopt);

    TableStreamOffsetPB latest_offset;
    latest_offset.set_partition_id(2001);
    latest_offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    latest_offset.set_offset_tso(100);
    latest_offset.set_last_consumption_time_ms(1234);
    TableStreamOffsetKeyInfo key_info {instance_id_,
                                       identity_.base_db_id(),
                                       identity_.base_table_id(),
                                       identity_.stream_db_id(),
                                       identity_.stream_id(),
                                       2001};
    txn->put(table_stream_offset_key(key_info), latest_offset.SerializeAsString());
    TableStreamOffsetPB versioned_offset = latest_offset;
    versioned_offset.set_last_consumption_time_ms(1235);
    versioned_put(txn.get(), versioned::table_stream_offset_key(key_info), Versionstamp(43, 0),
                  versioned_offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-inconsistent-offset-values");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).last_consumption_time_ms(), 1234);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsUnsupportedModeAndMissingVisibleTso) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    VersionPB version;
    version.set_version(8);
    txn->put(partition_version_key(
                     {instance_id_, identity_.base_db_id(), identity_.base_table_id(), 2001}),
             version.SerializeAsString());
    TableStreamOffsetPB offset;
    offset.set_partition_id(2001);
    offset.set_state(TABLE_STREAM_OFFSET_CONSUMED);
    offset.set_offset_tso(100);
    txn->put(table_stream_offset_key({instance_id_, identity_.base_db_id(),
                                      identity_.base_table_id(), identity_.stream_db_id(),
                                      identity_.stream_id(), 2001}),
             offset.SerializeAsString());
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    int64_t txn_id = begin_target_transaction("consume-missing-visible-tso");
    CommitTxnResponse response =
            consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 110);
    EXPECT_EQ(response.status().code(), MetaServiceCode::VERSION_NOT_FOUND);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);

    set_multi_version_status(MULTI_VERSION_ENABLED);
    txn_id = begin_target_transaction("consume-enabled-mode");
    response = consume_partition(txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 100, 110);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 100);
}

TEST_F(MetaServiceTableStreamTest, CommitRejectsUnsupportedTxnModesAndForcesImmediate) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    put_latest_partition_state(txn.get(), 2001, 8, 130, 100);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    CommitTxnRequest request = make_consume_request(begin_target_transaction("consume-2pc"), 2001,
                                                    TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    request.set_is_2pc(true);
    EXPECT_EQ(commit_transaction(request).status().code(), MetaServiceCode::INVALID_ARGUMENT);

    request = make_consume_request(begin_target_transaction("consume-txn-load"), 2001,
                                   TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    request.set_is_txn_load(true);
    EXPECT_EQ(commit_transaction(request).status().code(), MetaServiceCode::INVALID_ARGUMENT);

    request = make_consume_request(begin_target_transaction("consume-sub-txn"), 2001,
                                   TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    request.add_sub_txn_infos()->set_sub_txn_id(1);
    EXPECT_EQ(commit_transaction(request).status().code(), MetaServiceCode::INVALID_ARGUMENT);

    const int old_fuzzy_possibility = config::cloud_txn_lazy_commit_fuzzy_possibility;
    DORIS_CLOUD_DEFER {
        config::cloud_txn_lazy_commit_fuzzy_possibility = old_fuzzy_possibility;
    };
    config::cloud_txn_lazy_commit_fuzzy_possibility = 100;
    request = make_consume_request(begin_target_transaction("consume-immediate"), 2001,
                                   TABLE_STREAM_OFFSET_CONSUMED, 100, 120);
    request.set_enable_txn_lazy_commit(true);
    CommitTxnResponse response = commit_transaction(request);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 120);

    bool immediate_commit_hit = false;
    bool eventual_commit_hit = false;
    auto* sync_point = SyncPoint::get_instance();
    DORIS_CLOUD_DEFER {
        sync_point->disable_processing();
        sync_point->clear_all_call_backs();
    };
    sync_point->set_call_back("commit_txn_immediately::before_commit", [&](auto&& args) {
        *try_any_cast<TxnErrorCode*>(args[0]) = TxnErrorCode::TXN_BYTES_TOO_LARGE;
        *try_any_cast<MetaServiceCode*>(args[1]) = MetaServiceCode::INVALID_ARGUMENT;
        *try_any_cast<bool*>(args.back()) = true;
        immediate_commit_hit = true;
    });
    sync_point->set_call_back("commit_txn_eventually::finish",
                              [&](auto&&) { eventual_commit_hit = true; });
    sync_point->enable_processing();

    const int64_t too_large_txn_id = begin_target_transaction("consume-too-large");
    request = make_consume_request(too_large_txn_id, 2001, TABLE_STREAM_OFFSET_CONSUMED, 120, 130);
    request.set_enable_txn_lazy_commit(true);
    response = commit_transaction(request);
    EXPECT_EQ(response.status().code(), MetaServiceCode::INVALID_ARGUMENT);
    EXPECT_TRUE(immediate_commit_hit);
    EXPECT_FALSE(eventual_commit_hit);
    EXPECT_EQ(get_target_transaction(too_large_txn_id).status(),
              TxnStatusPB::TXN_STATUS_PREPARED);
    EXPECT_EQ(get_latest_offset(2001).offset_tso(), 120);
}

TEST_F(MetaServiceTableStreamTest, DropWritesTypedRecycleIndexInDisabledMode) {
    set_multi_version_status(MULTI_VERSION_DISABLED);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.add_index_ids(identity_.stream_id());
    request.set_db_id(identity_.base_db_id());
    request.set_table_id(identity_.base_table_id());
    request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    request.set_stream_db_id(identity_.stream_db_id());
    request.set_expiration(0);
    IndexResponse response;
    brpc::Controller controller;
    service_->drop_index(&controller, &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string value;
    ASSERT_EQ(txn->get(recycle_index_key({instance_id_, identity_.stream_id()}), &value),
              TxnErrorCode::TXN_OK);
    RecycleIndexPB recycle_index;
    ASSERT_TRUE(recycle_index.ParseFromString(value));
    EXPECT_EQ(recycle_index.state(), RecycleIndexPB::DROPPED);
    EXPECT_EQ(recycle_index.object_type(), IndexObjectTypePB::TABLE_STREAM);
    EXPECT_EQ(recycle_index.db_id(), identity_.base_db_id());
    EXPECT_EQ(recycle_index.table_id(), identity_.base_table_id());
    EXPECT_EQ(recycle_index.stream_db_id(), identity_.stream_db_id());
}

TEST_F(MetaServiceTableStreamTest, DropWritesTypedOperationLogInVersionedMode) {
    set_multi_version_status(MULTI_VERSION_READ_WRITE);
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.add_index_ids(identity_.stream_id());
    request.set_db_id(identity_.base_db_id());
    request.set_table_id(identity_.base_table_id());
    request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    request.set_stream_db_id(identity_.stream_db_id());
    request.set_expiration(0);
    IndexResponse response;
    brpc::Controller controller;
    service_->drop_index(&controller, &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();

    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    OperationLogPB operation_log;
    Versionstamp log_version;
    ASSERT_EQ(read_operation_log(txn.get(), versioned::log_key({instance_id_}), &log_version,
                                 &operation_log),
              TxnErrorCode::TXN_OK);
    ASSERT_TRUE(operation_log.has_drop_index());
    ASSERT_TRUE(operation_log.has_min_timestamp());
    EXPECT_EQ(operation_log.min_timestamp(),
              static_cast<uint64_t>(std::numeric_limits<int64_t>::max()));
    const DropIndexLogPB& drop_index = operation_log.drop_index();
    EXPECT_EQ(drop_index.object_type(), IndexObjectTypePB::TABLE_STREAM);
    EXPECT_EQ(drop_index.db_id(), identity_.base_db_id());
    EXPECT_EQ(drop_index.table_id(), identity_.base_table_id());
    EXPECT_EQ(drop_index.stream_db_id(), identity_.stream_db_id());
    ASSERT_EQ(drop_index.index_ids_size(), 1);
    EXPECT_EQ(drop_index.index_ids(0), identity_.stream_id());

    std::string value;
    EXPECT_EQ(txn->get(recycle_index_key({instance_id_, identity_.stream_id()}), &value, true),
              TxnErrorCode::TXN_KEY_NOT_FOUND);
}

TEST_F(MetaServiceTableStreamTest, DropUsesEarliestLocalVersionedOffset) {
    const std::string source_instance_id = "table_stream_drop_source";
    const Versionstamp source_version =
            put_auto_versioned_offset(source_instance_id, identity_, 2001, 10);
    set_clone_source(source_instance_id, Versionstamp(source_version.version() + 1, 0));
    // Partition 2002 sorts after partition 2001 but contains the earliest local version.
    const Versionstamp min_local_version =
            put_auto_versioned_offset(instance_id_, identity_, 2002, 30);
    put_auto_versioned_offset(instance_id_, identity_, 2001, 10);
    put_auto_versioned_offset(instance_id_, identity_, 2001, 20);

    IndexRequest request;
    request.set_cloud_unique_id(cloud_unique_id_);
    request.add_index_ids(identity_.stream_id());
    request.set_db_id(identity_.base_db_id());
    request.set_table_id(identity_.base_table_id());
    request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
    request.set_stream_db_id(identity_.stream_db_id());
    request.set_expiration(0);
    IndexResponse response;
    brpc::Controller controller;
    service_->drop_index(&controller, &request, &response, nullptr);
    ASSERT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();

    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
    OperationLogPB operation_log;
    Versionstamp log_version;
    ASSERT_EQ(read_operation_log(txn.get(), versioned::log_key({instance_id_}), &log_version,
                                 &operation_log),
              TxnErrorCode::TXN_OK);
    ASSERT_TRUE(operation_log.has_drop_index());
    ASSERT_TRUE(operation_log.has_min_timestamp());
    EXPECT_LT(source_version.version(), min_local_version.version());
    EXPECT_EQ(operation_log.min_timestamp(), min_local_version.version());
    EXPECT_LT(operation_log.min_timestamp(), log_version.version());
}

TEST_F(MetaServiceTableStreamTest, DropAndRecycleCloneChildOffsets) {
    const bool old_force_immediate_recycle = config::force_immediate_recycle;
    config::force_immediate_recycle = true;
    DORIS_CLOUD_DEFER {
        config::force_immediate_recycle = old_force_immediate_recycle;
    };

    const std::string source_instance_id = "table_stream_drop_recycle_source";
    TableStreamIdentityPB local_identity = identity_;
    TableStreamIdentityPB inherited_identity = identity_;
    inherited_identity.set_stream_id(identity_.stream_id() + 1);
    put_auto_versioned_offset(source_instance_id, local_identity, 2001, 90);
    const Versionstamp source_version =
            put_auto_versioned_offset(source_instance_id, inherited_identity, 2001, 80);
    const Versionstamp source_snapshot = Versionstamp::next(source_version);
    set_clone_source(source_instance_id, source_snapshot);
    put_auto_versioned_offset(instance_id_, local_identity, 2001, 100, true);

    auto drop_stream = [&](const TableStreamIdentityPB& identity) {
        IndexRequest request;
        request.set_cloud_unique_id(cloud_unique_id_);
        request.add_index_ids(identity.stream_id());
        request.set_db_id(identity.base_db_id());
        request.set_table_id(identity.base_table_id());
        request.set_object_type(IndexObjectTypePB::TABLE_STREAM);
        request.set_stream_db_id(identity.stream_db_id());
        request.set_expiration(0);
        IndexResponse response;
        brpc::Controller controller;
        service_->drop_index(&controller, &request, &response, nullptr);
        EXPECT_EQ(response.status().code(), MetaServiceCode::OK) << response.status().msg();
    };
    drop_stream(local_identity);
    drop_stream(inherited_identity);

    InstanceInfoPB child_instance;
    child_instance.set_instance_id(instance_id_);
    child_instance.set_status(InstanceInfoPB::NORMAL);
    child_instance.set_multi_version_status(MULTI_VERSION_READ_WRITE);
    child_instance.set_source_instance_id(source_instance_id);
    child_instance.set_source_snapshot_id(SnapshotManager::serialize_snapshot_id(source_snapshot));
    InstanceRecycler recycler(service_->txn_kv(), child_instance, RecyclerThreadPoolGroup {},
                              std::make_shared<TxnLazyCommitter>(service_->txn_kv()));
    ASSERT_EQ(recycler.init(), 0);
    ASSERT_EQ(recycler.recycle_operation_logs(), 0);

    auto key_exists = [&](std::string_view key) {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string value;
        return txn->get(key, &value, true) == TxnErrorCode::TXN_OK;
    };
    EXPECT_TRUE(key_exists(recycle_index_key({instance_id_, local_identity.stream_id()})));
    EXPECT_TRUE(key_exists(recycle_index_key({instance_id_, inherited_identity.stream_id()})));

    ASSERT_EQ(recycler.recycle_indexes(), 0);
    const auto offset_count = [&](const std::string& prefix) {
        std::unique_ptr<Transaction> txn;
        EXPECT_EQ(service_->txn_kv()->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        EXPECT_EQ(txn->get(prefix, lexical_end(prefix), &iter, true, 0), TxnErrorCode::TXN_OK);
        return iter->size();
    };
    EXPECT_EQ(offset_count(table_stream_offset_key_prefix(
                      instance_id_, local_identity.base_db_id(), local_identity.base_table_id(),
                      local_identity.stream_db_id(), local_identity.stream_id())),
              0);
    EXPECT_EQ(offset_count(versioned::table_stream_offset_key_prefix(
                      instance_id_, local_identity.base_db_id(), local_identity.base_table_id(),
                      local_identity.stream_db_id(), local_identity.stream_id())),
              0);
    EXPECT_EQ(offset_count(versioned::table_stream_offset_key_prefix(
                      instance_id_, inherited_identity.base_db_id(),
                      inherited_identity.base_table_id(), inherited_identity.stream_db_id(),
                      inherited_identity.stream_id())),
              0);
    EXPECT_FALSE(key_exists(recycle_index_key({instance_id_, local_identity.stream_id()})));
    EXPECT_FALSE(key_exists(recycle_index_key({instance_id_, inherited_identity.stream_id()})));
    EXPECT_GT(
            offset_count(versioned::table_stream_offset_key_prefix(
                    source_instance_id, local_identity.base_db_id(), local_identity.base_table_id(),
                    local_identity.stream_db_id(), local_identity.stream_id())),
            0);
    EXPECT_GT(offset_count(versioned::table_stream_offset_key_prefix(
                      source_instance_id, inherited_identity.base_db_id(),
                      inherited_identity.base_table_id(), inherited_identity.stream_db_id(),
                      inherited_identity.stream_id())),
              0);
}

} // namespace doris::cloud
