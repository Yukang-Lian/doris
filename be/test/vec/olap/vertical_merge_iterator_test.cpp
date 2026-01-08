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

#include <gtest/gtest.h>

#include "common/config.h"
#include "util/simd/bits.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"

namespace doris::vectorized {

class SparseColumnOptimizationTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Enable sparse column optimization for tests
        config::enable_sparse_column_compaction_optimization = true;
    }

    void TearDown() override {
        // Reset to default
        config::enable_sparse_column_compaction_optimization = true;
    }

    // Helper function to create a nullable column with specific NULL pattern
    static ColumnNullable::MutablePtr create_nullable_column(const std::vector<Int64>& values,
                                                             const std::vector<bool>& null_flags) {
        EXPECT_EQ(values.size(), null_flags.size());

        auto nested_column = ColumnInt64::create();
        auto null_map = ColumnUInt8::create();

        for (size_t i = 0; i < values.size(); ++i) {
            nested_column->insert_value(values[i]);
            null_map->insert_value(null_flags[i] ? 1 : 0);
        }

        return ColumnNullable::create(std::move(nested_column), std::move(null_map));
    }

    // Helper to count non-NULL values using SIMD
    static size_t count_non_null(const ColumnNullable* col, size_t start, size_t count) {
        const auto& null_map = col->get_null_map_data();
        return simd::count_zero_num(reinterpret_cast<const int8_t*>(null_map.data() + start),
                                    count);
    }

    // Helper to check if all values in range are NULL
    static bool is_all_null(const ColumnNullable* col, size_t start, size_t count) {
        return count_non_null(col, start, count) == 0;
    }

    // Helper to check if all values in range are non-NULL
    static bool is_all_non_null(const ColumnNullable* col, size_t start, size_t count) {
        return count_non_null(col, start, count) == count;
    }

    // Simulate copy_rows logic for nullable columns with sparse optimization
    static void copy_rows_with_optimization(const ColumnNullable* src, size_t start, size_t count,
                                            IColumn* dst_col) {
        auto* dst_mut = dst_col->assume_mutable().get();

        const size_t non_null_count = count_non_null(src, start, count);

        if (non_null_count == 0) {
            // Case 1: All NULL - batch fill with defaults
            dst_mut->insert_many_defaults(count);
        } else if (non_null_count == count || non_null_count > count / 2) {
            // Case 2: All non-NULL or non-NULL ratio > 50% - direct copy
            dst_mut->insert_range_from(*src, start, count);
        } else {
            // Case 3: Sparse mixed (non-NULL < 50%) - fill NULL first, then replace
            const size_t dst_start = dst_mut->size();
            dst_mut->insert_many_defaults(count);

            auto* nullable_dst = assert_cast<ColumnNullable*>(dst_mut);
            const auto& null_map = src->get_null_map_data();

            for (size_t row = 0; row < count; row++) {
                if (null_map[start + row] == 0) { // 0 means non-NULL
                    nullable_dst->replace_column_data(*src, start + row, dst_start + row);
                }
            }
        }
    }

    // Original copy_rows logic (direct copy)
    static void copy_rows_original(const IColumn* src, size_t start, size_t count,
                                   IColumn* dst_col) {
        dst_col->assume_mutable()->insert_range_from(*src, start, count);
    }

    // Helper to compare two nullable columns
    static bool columns_equal(const ColumnNullable* col1, const ColumnNullable* col2) {
        if (col1->size() != col2->size()) {
            return false;
        }

        const auto& null_map1 = col1->get_null_map_data();
        const auto& null_map2 = col2->get_null_map_data();
        const auto& nested1 = col1->get_nested_column();
        const auto& nested2 = col2->get_nested_column();

        for (size_t i = 0; i < col1->size(); ++i) {
            if (null_map1[i] != null_map2[i]) {
                return false;
            }
            // Only compare nested data for non-NULL rows
            if (null_map1[i] == 0) {
                if (nested1.compare_at(i, i, nested2, 1) != 0) {
                    return false;
                }
            }
        }
        return true;
    }
};

TEST_F(SparseColumnOptimizationTest, AllNullColumn) {
    // Test Case 1: All NULL column
    std::vector<Int64> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<bool> null_flags = {true, true, true, true, true,
                                    true, true, true, true, true};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    // Create destination columns
    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    // Copy with optimization
    copy_rows_with_optimization(nullable_src, 0, 10, dst_optimized.get());
    // Copy with original method
    copy_rows_original(src.get(), 0, 10, dst_original.get());

    // Verify results are equal
    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    // Verify all are NULL
    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), 10);
    EXPECT_TRUE(is_all_null(result, 0, 10));
}

TEST_F(SparseColumnOptimizationTest, AllNonNullColumn) {
    // Test Case 2: All non-NULL column
    std::vector<Int64> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<bool> null_flags = {false, false, false, false, false,
                                    false, false, false, false, false};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    copy_rows_with_optimization(nullable_src, 0, 10, dst_optimized.get());
    copy_rows_original(src.get(), 0, 10, dst_original.get());

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    // Verify all are non-NULL
    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), 10);
    EXPECT_TRUE(is_all_non_null(result, 0, 10));

    // Verify values
    const auto& nested = assert_cast<const ColumnInt64&>(result->get_nested_column());
    for (size_t i = 0; i < 10; ++i) {
        EXPECT_EQ(nested.get_element(i), static_cast<Int64>(i + 1));
    }
}

TEST_F(SparseColumnOptimizationTest, SparseMixedColumn) {
    // Test Case 3: Sparse mixed column (< 50% non-NULL)
    // 20% non-NULL rate
    std::vector<Int64> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<bool> null_flags = {false, true, true, true, true,
                                    false, true, true, true, true};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    copy_rows_with_optimization(nullable_src, 0, 10, dst_optimized.get());
    copy_rows_original(src.get(), 0, 10, dst_original.get());

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    // Verify count
    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), 10);
    EXPECT_EQ(count_non_null(result, 0, 10), 2);

    // Verify specific values
    EXPECT_FALSE(result->is_null_at(0));
    EXPECT_TRUE(result->is_null_at(1));
    EXPECT_FALSE(result->is_null_at(5));

    const auto& nested = assert_cast<const ColumnInt64&>(result->get_nested_column());
    EXPECT_EQ(nested.get_element(0), 1);
    EXPECT_EQ(nested.get_element(5), 6);
}

TEST_F(SparseColumnOptimizationTest, DenseMixedColumn) {
    // Test Case: Dense mixed column (> 50% non-NULL, should use direct copy)
    // 80% non-NULL rate
    std::vector<Int64> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<bool> null_flags = {false, false, false, false, true,
                                    false, false, false, false, true};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    copy_rows_with_optimization(nullable_src, 0, 10, dst_optimized.get());
    copy_rows_original(src.get(), 0, 10, dst_original.get());

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(count_non_null(result, 0, 10), 8);
}

TEST_F(SparseColumnOptimizationTest, PartialRangeCopy) {
    // Test partial range copy
    std::vector<Int64> values = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<bool> null_flags = {false, true, true, true, true,
                                    true,  true, true, true, false};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    // Copy middle range (indices 2-7, all NULL)
    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    copy_rows_with_optimization(nullable_src, 2, 6, dst_optimized.get());
    copy_rows_original(src.get(), 2, 6, dst_original.get());

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), 6);
    EXPECT_TRUE(is_all_null(result, 0, 6));
}

TEST_F(SparseColumnOptimizationTest, LargeSparseCopy) {
    // Test with large sparse column (5% non-NULL rate, typical for sparse wide tables)
    constexpr size_t num_rows = 1024;
    std::vector<Int64> values(num_rows);
    std::vector<bool> null_flags(num_rows);

    for (size_t i = 0; i < num_rows; ++i) {
        values[i] = static_cast<Int64>(i);
        // Every 20th row is non-NULL (5% non-NULL rate)
        null_flags[i] = (i % 20 != 0);
    }

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    copy_rows_with_optimization(nullable_src, 0, num_rows, dst_optimized.get());
    copy_rows_original(src.get(), 0, num_rows, dst_original.get());

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), num_rows);
    // 1024 / 20 = 51.2 -> 52 non-NULL rows (0, 20, 40, ..., 1000, 1020)
    EXPECT_EQ(count_non_null(result, 0, num_rows), 52);
}

TEST_F(SparseColumnOptimizationTest, MultipleCopies) {
    // Test multiple sequential copies to the same destination
    std::vector<Int64> values = {1, 2, 3, 4, 5};
    std::vector<bool> null_flags = {false, true, true, true, false};

    auto src = create_nullable_column(values, null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    auto dst_optimized = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto dst_original = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    // Copy the same source multiple times
    for (int i = 0; i < 3; ++i) {
        copy_rows_with_optimization(nullable_src, 0, 5, dst_optimized.get());
        copy_rows_original(src.get(), 0, 5, dst_original.get());
    }

    EXPECT_TRUE(columns_equal(assert_cast<const ColumnNullable*>(dst_optimized.get()),
                              assert_cast<const ColumnNullable*>(dst_original.get())));

    const auto* result = assert_cast<const ColumnNullable*>(dst_optimized.get());
    EXPECT_EQ(result->size(), 15);
    EXPECT_EQ(count_non_null(result, 0, 15), 6); // 2 non-NULL per copy * 3 copies
}

TEST_F(SparseColumnOptimizationTest, DisabledOptimization) {
    // Test with optimization disabled
    config::enable_sparse_column_compaction_optimization = false;

    std::vector<Int64> values = {1, 2, 3, 4, 5};
    std::vector<bool> null_flags = {true, true, true, true, true};

    auto src = create_nullable_column(values, null_flags);

    auto dst = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());

    // When disabled, should still work correctly via direct copy path
    copy_rows_original(src.get(), 0, 5, dst.get());

    const auto* result = assert_cast<const ColumnNullable*>(dst.get());
    EXPECT_EQ(result->size(), 5);
    EXPECT_TRUE(is_all_null(result, 0, 5));
}

TEST_F(SparseColumnOptimizationTest, ReplaceColumnDataRange) {
    // Test replace_column_data_range functionality
    std::vector<Int64> src_values = {1, 2, 3, 4, 5};
    std::vector<bool> src_null_flags = {false, true, false, true, false};

    auto src = create_nullable_column(src_values, src_null_flags);
    const auto* nullable_src = assert_cast<const ColumnNullable*>(src.get());

    // Create destination with pre-filled NULLs
    auto dst = ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create());
    auto* nullable_dst = assert_cast<ColumnNullable*>(dst.get());

    // Pre-fill with NULLs
    nullable_dst->get_null_map_column().get_data().resize_fill(5, 1);
    nullable_dst->get_nested_column().resize(5);

    // Replace with source data
    nullable_dst->replace_column_data_range(*nullable_src, 0, 5, 0);

    // Verify results
    EXPECT_EQ(nullable_dst->size(), 5);
    const auto& null_map = nullable_dst->get_null_map_data();
    EXPECT_EQ(null_map[0], 0); // non-NULL
    EXPECT_EQ(null_map[1], 1); // NULL
    EXPECT_EQ(null_map[2], 0); // non-NULL
    EXPECT_EQ(null_map[3], 1); // NULL
    EXPECT_EQ(null_map[4], 0); // non-NULL

    // Verify values for non-NULL positions
    const auto& nested = assert_cast<const ColumnInt64&>(nullable_dst->get_nested_column());
    EXPECT_EQ(nested.get_element(0), 1);
    EXPECT_EQ(nested.get_element(2), 3);
    EXPECT_EQ(nested.get_element(4), 5);
}

TEST_F(SparseColumnOptimizationTest, CountZeroNumSIMD) {
    // Test SIMD count_zero_num function
    std::vector<int8_t> data(128);

    // All zeros
    std::fill(data.begin(), data.end(), 0);
    EXPECT_EQ(simd::count_zero_num(data.data(), static_cast<size_t>(data.size())), 128);

    // All ones
    std::fill(data.begin(), data.end(), 1);
    EXPECT_EQ(simd::count_zero_num(data.data(), static_cast<size_t>(data.size())), 0);

    // Mixed: every 4th is zero
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = (i % 4 == 0) ? 0 : 1;
    }
    EXPECT_EQ(simd::count_zero_num(data.data(), static_cast<size_t>(data.size())), 32);

    // Small sizes
    EXPECT_EQ(simd::count_zero_num(data.data(), static_cast<size_t>(1)), 1);  // single zero
    EXPECT_EQ(simd::count_zero_num(data.data() + 1, static_cast<size_t>(1)), 0);  // single one
}

} // namespace doris::vectorized
