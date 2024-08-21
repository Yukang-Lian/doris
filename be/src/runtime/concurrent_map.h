
#pragma once
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

template <typename KeyType, typename ValueType>
class ConcurrentMap {
public:
    ConcurrentMap(size_t num_buckets = 16);

    // Functionality
    size_t size();
    void clear();
    void erase(const KeyType& key);
    void insert(KeyType key, ValueType value);
    ValueType find(const KeyType& key);
    std::vector<ValueType> values();

private:
    size_t getBucketIndex(const KeyType& key) const;

    size_t num_buckets_;
    std::vector<std::unordered_map<KeyType, ValueType>> buckets_;
    std::vector<std::shared_mutex> locks_;
};