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

#include "cloud/cloud_cumulative_compaction_policy.h"

#include <fmt/core.h>

#include <algorithm>
#include <list>
#include <ostream>
#include <string>

#include "cloud/config.h"
#include "common/config.h"
#include "common/logging.h"
#include "cpp/sync_point.h"
#include "olap/olap_common.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"

namespace doris {

CloudSizeBasedCumulativeCompactionPolicy::CloudSizeBasedCumulativeCompactionPolicy(
        int64_t promotion_size, double promotion_ratio, int64_t promotion_min_size,
        int64_t compaction_min_size)
        : _promotion_size(promotion_size),
          _promotion_ratio(promotion_ratio),
          _promotion_min_size(promotion_min_size),
          _compaction_min_size(compaction_min_size) {}

int64_t CloudSizeBasedCumulativeCompactionPolicy::_level_size(const int64_t size) {
    if (size < 1024) return 0;
    int64_t max_level = (int64_t)1
                        << (sizeof(_promotion_size) * 8 - 1 - __builtin_clzl(_promotion_size / 2));
    if (size >= max_level) return max_level;
    return (int64_t)1 << (sizeof(size) * 8 - 1 - __builtin_clzl(size));
}

int32_t CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets(
        CloudTablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        const int64_t max_compaction_score, const int64_t min_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score, bool allow_delete) {
    LOG_WARNING("[lyk_debug]:")
            .tag("tablet id", tablet->tablet_id())
            .tag("candidate_rowsets", candidate_rowsets.size())
            .tag("start rowset", candidate_rowsets.begin()->get()->rowset_id())
            .tag("max comapction score", max_compaction_score)
            .tag("min comapction score", min_compaction_score)
            .tag("last delete version", last_delete_version->to_string())
            .tag("allow delete", allow_delete);
    size_t promotion_size = cloud_promotion_size(tablet);
    auto max_version = tablet->max_version().first;
    LOG_WARNING("[lyk_debug]: Cloud promotion size and max version")
            .tag("promotion_size", promotion_size)
            .tag("max_version", max_version);
    int transient_size = 0;
    *compaction_score = 0;
    int64_t total_size = 0;
    for (auto& rowset : candidate_rowsets) {
        // check whether this rowset is delete version
        LOG_WARNING("[lyk_debug]: Processing rowset")
                .tag("rowset_id", rowset->rowset_id())
                .tag("rowset_version", rowset->version().to_string())
                .tag("rowset_size", rowset->rowset_meta()->total_disk_size());

        if (!allow_delete && rowset->rowset_meta()->has_delete_predicate()) {
            *last_delete_version = rowset->version();
            LOG_WARNING("[lyk_debug]: Rowset has delete predicate, handling delete version")
                    .tag("last_delete_version", last_delete_version->to_string());
            if (!input_rowsets->empty()) {
                // we meet a delete version, and there were other versions before.
                // we should compact those version before handling them over to base compaction
                LOG_WARNING("[lyk_debug]: Break to compact earlier versions due to delete version");
                break;
            } else {
                // we meet a delete version, and no other versions before, skip it and continue
                LOG_WARNING(
                        "[lyk_debug]: Clear input rowsets and reset compaction score due to delete "
                        "version");
                input_rowsets->clear();
                *compaction_score = 0;
                transient_size = 0;
                continue;
            }
        }
        if (tablet->tablet_state() == TABLET_NOTREADY) {
            // If tablet under alter, keep latest 10 version so that base tablet max version
            // not merged in new tablet, and then we can copy data from base tablet
            LOG_WARNING("[lyk_debug]: Tablet is not ready, checking version constraint")
                    .tag("tablet_state", "TABLET_NOTREADY")
                    .tag("max_version", max_version);
            if (rowset->version().second < max_version - 10) {
                LOG_WARNING("[lyk_debug]: Skipping rowset due to version being too old")
                        .tag("rowset_version", rowset->version().to_string());
                continue;
            }
        }
        if (*compaction_score >= max_compaction_score) {
            LOG_WARNING("[lyk_debug]: Reached maximum compaction score")
                    .tag("compaction_score", *compaction_score)
                    .tag("max_compaction_score", max_compaction_score);
            // got enough segments
            break;
        }
        *compaction_score += rowset->rowset_meta()->get_compaction_score();
        total_size += rowset->rowset_meta()->total_disk_size();

        transient_size += 1;
        input_rowsets->push_back(rowset);
        LOG_WARNING("[lyk_debug]: Added rowset to compaction")
                .tag("current_compaction_score", *compaction_score)
                .tag("total_size", total_size);
    }

    if (total_size >= promotion_size) {
        LOG_WARNING("[lyk_debug]: Total size exceeds promotion size, returning transient size")
                .tag("total_size", total_size)
                .tag("promotion_size", promotion_size);
        return transient_size;
    }

    // if there is delete version, do compaction directly
    if (last_delete_version->first != -1) {
        LOG_WARNING("[lyk_debug]: Delete version detected, performing compaction directly")
                .tag("last_delete_version", last_delete_version->to_string());
        if (input_rowsets->size() == 1) {
            auto rs_meta = input_rowsets->front()->rowset_meta();
            LOG_WARNING("[lyk_debug]: Checking if rowset is overlapping")
                    .tag("rowset_id", input_rowsets->front()->rowset_id())
                    .tag("is_segments_overlapping", rs_meta->is_segments_overlapping());
            // if there is only one rowset and not overlapping,
            // we do not need to do cumulative compaction
            if (!rs_meta->is_segments_overlapping()) {
                input_rowsets->clear();
                *compaction_score = 0;
                LOG_WARNING(
                        "[lyk_debug]: Rowset not overlapping, clearing input rowsets and resetting "
                        "score");
            }
        }
        return transient_size;
    }

    auto rs_begin = input_rowsets->begin();
    size_t new_compaction_score = *compaction_score;
    LOG_WARNING("[lyk_debug]: Starting rowset compaction checks");
    while (rs_begin != input_rowsets->end()) {
        auto& rs_meta = (*rs_begin)->rowset_meta();
        int current_level = _level_size(rs_meta->total_disk_size());
        int remain_level = _level_size(total_size - rs_meta->total_disk_size());
        // Log the current level and remaining level
        LOG_WARNING("[lyk_debug]: Checking rowset compaction level")
                .tag("rowset_id", (*rs_begin)->rowset_id())
                .tag("current_level", current_level)
                .tag("remain_level", remain_level);
        // if current level less then remain level, input rowsets contain current rowset
        // and process return; otherwise, input rowsets do not contain current rowset.
        if (current_level <= remain_level) {
            LOG_WARNING(
                    "[lyk_debug]: Current level is less than or equal to remain level, breaking "
                    "the loop")
                    .tag("rowset_id", (*rs_begin)->rowset_id());
            break;
        }
        total_size -= rs_meta->total_disk_size();
        new_compaction_score -= rs_meta->get_compaction_score();
        ++rs_begin;
        LOG_WARNING("[lyk_debug]: Adjusting total size and compaction score")
                .tag("new_total_size", total_size)
                .tag("new_compaction_score", new_compaction_score)
                .tag("next_rowset_id", rs_begin != input_rowsets->end()
                                               ? (*rs_begin)->rowset_id().to_string()
                                               : "end");
    }
    if (rs_begin == input_rowsets->end()) { // No suitable level size found in `input_rowsets`
        LOG_WARNING("[lyk_debug]: No suitable level size found, checking configurations");
        if (config::prioritize_query_perf_in_compaction && tablet->keys_type() != DUP_KEYS) {
            // While tablet's key type is not `DUP_KEYS`, compacting rowset in such tablets has a significant
            // positive impact on queries and reduces space amplification, so we ignore level limitation and
            // pick candidate rowsets as input rowsets.
            LOG_WARNING("[lyk_debug]: Prioritizing query performance, bypassing level limitation")
                    .tag("tablet_keys_type", tablet->keys_type())
                    .tag("config_prioritize_query_perf",
                         config::prioritize_query_perf_in_compaction);
            return transient_size;
        } else if (*compaction_score >= max_compaction_score) {
            // Score of `input_rowsets` exceed max compaction score, which means `input_rowsets` will never change and
            // this tablet will never execute cumulative compaction. MUST execute compaction on these `input_rowsets`
            // to reduce compaction score.
            LOG_WARNING("[lyk_debug]: Compaction score exceeds max score, terminating compaction")
                    .tag("compaction_score", *compaction_score)
                    .tag("max_compaction_score", max_compaction_score);
            RowsetSharedPtr rs_with_max_score;
            uint32_t max_score = 1;
            for (auto& rs : *input_rowsets) {
                if (rs->rowset_meta()->get_compaction_score() > max_score) {
                    max_score = rs->rowset_meta()->get_compaction_score();
                    rs_with_max_score = rs;
                    LOG_WARNING("[lyk_debug]: Found rowset with higher compaction score")
                            .tag("rowset_id", rs->rowset_id())
                            .tag("compaction_score", max_score);
                }
            }
            if (rs_with_max_score) {
                input_rowsets->clear();
                input_rowsets->push_back(std::move(rs_with_max_score));
                *compaction_score = max_score;
                LOG_WARNING("[lyk_debug]: Chosen rowset for compaction with max score")
                        .tag("rowset_id", rs_with_max_score->rowset_id())
                        .tag("compaction_score", max_score);
                return transient_size;
            }
            // Exceeding max compaction score, do compaction on all candidate rowsets anyway
            return transient_size;
        }
    }
    input_rowsets->erase(input_rowsets->begin(), rs_begin);
    *compaction_score = new_compaction_score;
    LOG_WARNING("[lyk_debug]: After level size check, updated compaction score and total size")
            .tag("new_compaction_score", *compaction_score)
            .tag("total_size", total_size);

    VLOG_CRITICAL << "cumulative compaction size_based policy, compaction_score = "
                  << *compaction_score << ", total_size = " << total_size
                  << ", calc promotion size value = " << promotion_size
                  << ", tablet = " << tablet->tablet_id() << ", input_rowset size "
                  << input_rowsets->size();

    // empty return
    if (input_rowsets->empty()) {
        LOG_WARNING("[lyk_debug]: Input rowsets are empty, skipping compaction")
                .tag("transient_size", transient_size);
        return transient_size;
    }

    // if we have a sufficient number of segments, we should process the compaction.
    // otherwise, we check number of segments and total_size whether can do compaction.
    if (total_size < _compaction_min_size && *compaction_score < min_compaction_score) {
        LOG_WARNING("[lyk_debug]: Insufficient size and compaction score, clearing rowsets")
                .tag("total_size", total_size)
                .tag("compaction_score", *compaction_score)
                .tag("min_compaction_score", min_compaction_score);
        input_rowsets->clear();
        *compaction_score = 0;
    } else if (total_size >= _compaction_min_size && input_rowsets->size() == 1) {
        LOG_WARNING(
                "[lyk_debug]: Only one rowset and sufficient total size, checking for overlapping "
                "segments")
                .tag("total_size", total_size)
                .tag("compaction_min_size", _compaction_min_size)
                .tag("input_rowsets_size", input_rowsets->size());
        auto rs_meta = input_rowsets->front()->rowset_meta();
        // if there is only one rowset and not overlapping,
        // we do not need to do compaction
        if (!rs_meta->is_segments_overlapping()) {
            LOG_WARNING(
                    "[lyk_debug]: Rowset segments are not overlapping, clearing rowsets and "
                    "resetting compaction score");
            input_rowsets->clear();
            *compaction_score = 0;
        }
    }
    return transient_size;
}

int64_t CloudSizeBasedCumulativeCompactionPolicy::cloud_promotion_size(CloudTablet* t) const {
    int64_t promotion_size = int64_t(t->base_size() * _promotion_ratio);
    // promotion_size is between _size_based_promotion_size and _size_based_promotion_min_size
    return promotion_size > _promotion_size       ? _promotion_size
           : promotion_size < _promotion_min_size ? _promotion_min_size
                                                  : promotion_size;
}

int64_t CloudSizeBasedCumulativeCompactionPolicy::new_cumulative_point(
        CloudTablet* tablet, const RowsetSharedPtr& output_rowset, Version& last_delete_version,
        int64_t last_cumulative_point) {
    TEST_INJECTION_POINT_RETURN_WITH_VALUE("new_cumulative_point", int64_t(0), output_rowset.get(),
                                           last_cumulative_point);
    // for MoW table, if there's too many versions, the delete bitmap will grow to
    // a very big size, which may cause the tablet meta too big and the `save_meta`
    // operation too slow.
    // if the rowset should not promotion according to it's disk size, we should also
    // consider it's version count here.
    bool satisfy_promotion_version = tablet->enable_unique_key_merge_on_write() &&
                                     output_rowset->end_version() - output_rowset->start_version() >
                                             config::compaction_promotion_version_count;
    // if rowsets have delete version, move to the last directly.
    // if rowsets have no delete version, check output_rowset total disk size satisfies promotion size.
    return output_rowset->start_version() == last_cumulative_point &&
                           (last_delete_version.first != -1 ||
                            output_rowset->data_disk_size() >= cloud_promotion_size(tablet) ||
                            satisfy_promotion_version)
                   ? output_rowset->end_version() + 1
                   : last_cumulative_point;
}

int32_t CloudTimeSeriesCumulativeCompactionPolicy::pick_input_rowsets(
        CloudTablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        const int64_t max_compaction_score, const int64_t min_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score, bool allow_delete) {
    LOG_WARNING("[time lyk_debug]:")
            .tag("tablet id", tablet->tablet_id())
            .tag("candidate_rowsets", candidate_rowsets.size())
            .tag("start rowset", candidate_rowsets.begin()->get()->rowset_id())
            .tag("max comapction score", max_compaction_score)
            .tag("min comapction score", min_compaction_score)
            .tag("last delete version", last_delete_version->to_string())
            .tag("allow delete", allow_delete);
    if (tablet->tablet_state() == TABLET_NOTREADY) {
        LOG_WARNING("[time lyk_debug]: Tablet state is not ready, skipping compaction")
                .tag("tablet id", tablet->tablet_id())
                .tag("tablet_state", tablet->tablet_state());
        return 0;
    }

    input_rowsets->clear();
    int64_t compaction_goal_size_mbytes =
            tablet->tablet_meta()->time_series_compaction_goal_size_mbytes();

    int transient_size = 0;
    *compaction_score = 0;
    int64_t total_size = 0;

    for (const auto& rowset : candidate_rowsets) {
        LOG_WARNING("[time lyk_debug]: Processing rowset")
                .tag("rowset_id", rowset->rowset_id())
                .tag("tablet id", tablet->tablet_id())
                .tag("has_delete_predicate", rowset->rowset_meta()->has_delete_predicate());

        // check whether this rowset is delete version
        LOG_WARNING("[time lyk_debug]: ")
                .tag("tablet id", tablet->tablet_id())
                .tag("allow_delete", allow_delete)
                .tag("has_delete_predicate", rowset->rowset_meta()->has_delete_predicate());
        if (!allow_delete && rowset->rowset_meta()->has_delete_predicate()) {
            *last_delete_version = rowset->version();
            LOG_WARNING("[time lyk_debug]: Rowset has delete predicate, processing delete version")
                    .tag("tablet id", tablet->tablet_id())
                    .tag("last_delete_version", last_delete_version->to_string());
            if (!input_rowsets->empty()) {
                LOG_WARNING(
                        "[time lyk_debug]: Found delete version, and there were other versions "
                        "before. "
                        "Break compaction.")
                        .tag("tablet id", tablet->tablet_id());
                // we meet a delete version, and there were other versions before.
                // we should compact those version before handling them over to base compaction
                break;
            } else {
                // we meet a delete version, and no other versions before, skip it and continue
                LOG_WARNING(
                        "[time lyk_debug]: Found delete version, but no previous versions, "
                        "skipping it "
                        "and clearing rowsets")
                        .tag("tablet id", tablet->tablet_id());
                input_rowsets->clear();
                *compaction_score = 0;
                transient_size = 0;
                total_size = 0;
                continue;
            }
        }

        *compaction_score += rowset->rowset_meta()->get_compaction_score();
        total_size += rowset->rowset_meta()->total_disk_size();

        transient_size += 1;
        input_rowsets->push_back(rowset);
        LOG_WARNING("[time lyk_debug]: Updated compaction score and total size")
                .tag("tablet id", tablet->tablet_id())
                .tag("compaction_score", *compaction_score)
                .tag("total_size", total_size)
                .tag("transient_size", transient_size);

        // Condition 1: the size of input files for compaction meets the requirement of parameter compaction_goal_size
        if (total_size >= (compaction_goal_size_mbytes * 1024 * 1024)) {
            LOG_WARNING("[time lyk_debug]: Compaction goal size met, checking rowset overlap")
                    .tag("tablet id", tablet->tablet_id())
                    .tag("compaction_goal_size_mbytes", compaction_goal_size_mbytes)
                    .tag("total_size", total_size);
            if (input_rowsets->size() == 1 &&
                !input_rowsets->front()->rowset_meta()->is_segments_overlapping()) {
                LOG_WARNING(
                        "[time lyk_debug]: Only one non-overlapping rowset, skipping compaction")
                        .tag("tablet id", tablet->tablet_id())
                        .tag("rowset_id", input_rowsets->front()->rowset_id());
                // Only 1 non-overlapping rowset, skip it
                input_rowsets->clear();
                *compaction_score = 0;
                total_size = 0;
                continue;
            }
            return transient_size;
        } else if (
                *compaction_score >=
                config::compaction_max_rowset_count) { // If the number of rowsets is too large: FDB_ERROR_CODE_TXN_TOO_LARGE
            LOG_WARNING(
                    "[time lyk_debug]: Compaction score exceeds max rowset count, terminating "
                    "compaction")
                    .tag("tablet id", tablet->tablet_id())
                    .tag("compaction_score", *compaction_score)
                    .tag("compaction_max_rowset_count", config::compaction_max_rowset_count);
            return transient_size;
        }
    }

    // if there is delete version, do compaction directly
    LOG_WARNING("[time lyk_debug]: ")
            .tag("tablet id", tablet->tablet_id())
            .tag("last_delete_version->first", last_delete_version->first);
    if (last_delete_version->first != -1) {
        LOG_WARNING("[time lyk_debug]: Last delete version found, checking rowset compaction logic")
                .tag("tablet id", tablet->tablet_id())
                .tag("last_delete_version", last_delete_version->to_string());

        // if there is only one rowset and not overlapping,
        // we do not need to do cumulative compaction
        if (input_rowsets->size() == 1 &&
            !input_rowsets->front()->rowset_meta()->is_segments_overlapping()) {
            LOG_WARNING(
                    "[time lyk_debug]: Only one rowset and segments are not overlapping, clearing "
                    "rowsets")
                    .tag("tablet id", tablet->tablet_id())
                    .tag("input_rowsets_size", input_rowsets->size())
                    .tag("is_segments_overlapping",
                         input_rowsets->front()->rowset_meta()->is_segments_overlapping());
            input_rowsets->clear();
            *compaction_score = 0;
        }
        return transient_size;
    }
    LOG_WARNING("[time lyk_debug]: Checking compaction score against file count threshold")
            .tag("compaction_score", *compaction_score)
            .tag("tablet id", tablet->tablet_id())
            .tag("compaction_file_count_threshold",
                 tablet->tablet_meta()->time_series_compaction_file_count_threshold());

    // Condition 2: the number of input files reaches the threshold specified by parameter compaction_file_count_threshold
    LOG_WARNING("[time lyk_debug]: ")
            .tag("tablet id", tablet->tablet_id())
            .tag("compaction_score", *compaction_score)
            .tag("tablet->tablet_meta()->time_series_compaction_file_count_threshold()",
                 tablet->tablet_meta()->time_series_compaction_file_count_threshold());
    if (*compaction_score >= tablet->tablet_meta()->time_series_compaction_file_count_threshold()) {
        LOG_WARNING(
                "[time lyk_debug]: Compaction score exceeds or meets file count threshold, "
                "skipping "
                "compaction")
                .tag("tablet id", tablet->tablet_id())
                .tag("compaction_score", *compaction_score);
        return transient_size;
    }

    // Condition 3: level1 achieve compaction_goal_size
    std::vector<RowsetSharedPtr> level1_rowsets;
    LOG_WARNING("[time lyk_debug]: ")
            .tag("tablet id", tablet->tablet_id())
            .tag("tablet->tablet_meta()->time_series_compaction_level_threshold()",
                 tablet->tablet_meta()->time_series_compaction_level_threshold());
    if (tablet->tablet_meta()->time_series_compaction_level_threshold() >= 2) {
        LOG_WARNING("[time lyk_debug]: Checking if level 1 rowsets can meet compaction goal size")
                .tag("tablet id", tablet->tablet_id())
                .tag("level_threshold",
                     tablet->tablet_meta()->time_series_compaction_level_threshold())
                .tag("compaction_goal_size_mbytes", compaction_goal_size_mbytes);
        int64_t continuous_size = 0;
        for (const auto& rowset : candidate_rowsets) {
            const auto& rs_meta = rowset->rowset_meta();
            if (rs_meta->compaction_level() == 0) {
                LOG_WARNING("[time lyk_debug]: Encountered level 0 rowset, breaking out of loop")
                        .tag("tablet id", tablet->tablet_id())
                        .tag("rowset_id", rowset->rowset_id())
                        .tag("compaction_level", rs_meta->compaction_level());
                break;
            }
            level1_rowsets.push_back(rowset);
            continuous_size += rs_meta->total_disk_size();
            LOG_WARNING("[time lyk_debug]: Adding rowset to level1 rowsets")
                    .tag("tablet id", tablet->tablet_id())
                    .tag("rowset_id", rowset->rowset_id())
                    .tag("continuous_size", continuous_size);
            if (level1_rowsets.size() >= 2) {
                if (continuous_size >= compaction_goal_size_mbytes * 1024 * 1024) {
                    LOG_WARNING(
                            "[time lyk_debug]: Achieved compaction goal size, swapping level1 "
                            "rowsets")
                            .tag("tablet id", tablet->tablet_id())
                            .tag("continuous_size", continuous_size)
                            .tag("compaction_goal_size", compaction_goal_size_mbytes * 1024 * 1024);
                    input_rowsets->swap(level1_rowsets);
                    return input_rowsets->size();
                }
            }
        }
    }

    int64_t now = UnixMillis();
    int64_t last_cumu = tablet->last_cumu_compaction_success_time();
    LOG_WARNING("[time lyk_debug]: ")
            .tag("tablet id", tablet->tablet_id())
            .tag("now", now)
            .tag("last_cumu", last_cumu);
    if (last_cumu != 0) {
        int64_t cumu_interval = now - last_cumu;

        // Condition 4: the time interval between compactions exceeds the value specified by parameter compaction_time_threshold_second
        LOG_WARNING("[time lyk_debug]: Checking compaction time interval")
                .tag("tablet id", tablet->tablet_id())
                .tag("last_cumu", last_cumu)
                .tag("current_time", now)
                .tag("cumu_interval", cumu_interval)
                .tag("compaction_time_threshold",
                     tablet->tablet_meta()->time_series_compaction_time_threshold_seconds());
        if (cumu_interval >
            (tablet->tablet_meta()->time_series_compaction_time_threshold_seconds() * 1000)) {
            LOG_WARNING(
                    "[time lyk_debug]: Compaction time interval exceeded, checking level threshold "
                    "and "
                    "rowsets")
                    .tag("tablet id", tablet->tablet_id())
                    .tag("compaction_time_interval", cumu_interval);
            if (tablet->tablet_meta()->time_series_compaction_level_threshold() >= 2) {
                if (input_rowsets->empty() && level1_rowsets.size() >= 2) {
                    LOG_WARNING(
                            "[time lyk_debug]: Time threshold exceeded and enough level1 rowsets, "
                            "swapping")
                            .tag("tablet id", tablet->tablet_id())
                            .tag("level1_rowsets_size", level1_rowsets.size());
                    input_rowsets->swap(level1_rowsets);
                    return input_rowsets->size();
                }
            }
            return transient_size;
        }
    }

    input_rowsets->clear();
    // Condition 5: If their are many empty rowsets, maybe should be compacted
    LOG_WARNING("[time lyk_debug]: ")
            .tag("tablet id", tablet->tablet_id())
            .tag("tablet->tablet_meta()->time_series_compaction_empty_rowsets_threshold()",
                 tablet->tablet_meta()->time_series_compaction_empty_rowsets_threshold())
            .tag("candidate_rowsets", candidate_rowsets.size())
            .tag("input_rowsets", input_rowsets->size());
    tablet->calc_consecutive_empty_rowsets(
            input_rowsets, candidate_rowsets,
            tablet->tablet_meta()->time_series_compaction_empty_rowsets_threshold());
    LOG_WARNING("[time lyk_debug]: ")
            .tag("tablet id", tablet->tablet_id())
            .tag("input_rowsets", input_rowsets->size());
    if (!input_rowsets->empty()) {
        LOG_WARNING("[time lyk_debug]: Too many consecutive empty rowsets detected")
                .tag("tablet_id", tablet->tablet_id())
                .tag("empty_rowsets_size", input_rowsets->size());
        VLOG_NOTICE << "tablet is " << tablet->tablet_id()
                    << ", there are too many consecutive empty rowsets, size is "
                    << input_rowsets->size();
        return 0;
    }
    LOG_WARNING("[time lyk_debug]: Reset compaction score after empty rowsets check")
            .tag("compaction_score", *compaction_score)
            .tag("tablet id", tablet->tablet_id());
    *compaction_score = 0;

    return 0;
}

int64_t CloudTimeSeriesCumulativeCompactionPolicy::new_compaction_level(
        const std::vector<RowsetSharedPtr>& input_rowsets) {
    int64_t first_level = 0;
    for (size_t i = 0; i < input_rowsets.size(); i++) {
        int64_t cur_level = input_rowsets[i]->rowset_meta()->compaction_level();
        if (i == 0) {
            first_level = cur_level;
        } else {
            if (first_level != cur_level) {
                LOG(ERROR) << "Failed to check compaction level, first_level: " << first_level
                           << ", cur_level: " << cur_level;
            }
        }
    }
    return first_level + 1;
}

int64_t CloudTimeSeriesCumulativeCompactionPolicy::new_cumulative_point(
        CloudTablet* tablet, const RowsetSharedPtr& output_rowset, Version& last_delete_version,
        int64_t last_cumulative_point) {
    if (tablet->tablet_state() != TABLET_RUNNING || output_rowset->num_segments() == 0) {
        return last_cumulative_point;
    }

    if (tablet->tablet_meta()->time_series_compaction_level_threshold() >= 2 &&
        output_rowset->rowset_meta()->compaction_level() < 2) {
        return last_cumulative_point;
    }

    return output_rowset->end_version() + 1;
}

} // namespace doris
