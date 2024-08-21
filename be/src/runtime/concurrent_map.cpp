#include "runtime/concurrent_map.h"

#include <mutex>

#include "runtime/plan_fragment_executor.h"

template <typename KeyType, typename ValueType>
ConcurrentMap<KeyType, ValueType>::ConcurrentMap(size_t num_buckets)
        : num_buckets_(num_buckets), buckets_(num_buckets), locks_(num_buckets) {}

template <typename KeyType, typename ValueType>
size_t ConcurrentMap<KeyType, ValueType>::size() {
    size_t total_size = 0;
    for (size_t i = 0; i < num_buckets_; ++i) {
        std::shared_lock<std::shared_mutex> lock(locks_[i]);
        total_size += buckets_[i].size();
    }
    return total_size;
}

template <typename KeyType, typename ValueType>
void ConcurrentMap<KeyType, ValueType>::clear() {
    for (size_t i = 0; i < num_buckets_; ++i) {
        std::unique_lock<std::shared_mutex> lock(locks_[i]);
        buckets_[i].clear();
    }
}

template <typename KeyType, typename ValueType>
void ConcurrentMap<KeyType, ValueType>::erase(const KeyType& key) {
    size_t index = getBucketIndex(key);
    std::unique_lock<std::shared_mutex> lock(locks_[index]);
    buckets_[index].erase(key);
}

template <typename KeyType, typename ValueType>
ValueType ConcurrentMap<KeyType, ValueType>::find(const KeyType& key) {
    size_t index = getBucketIndex(key);
    std::shared_lock<std::shared_mutex> lock(locks_[index]);
    auto it = buckets_[index].find(key);
    if (it != buckets_[index].end()) {
        return it->second;
    }
    return nullptr;
}

template <typename KeyType, typename ValueType>
void ConcurrentMap<KeyType, ValueType>::insert(KeyType key, ValueType value) {
    size_t index = getBucketIndex(key);
    std::unique_lock<std::shared_mutex> lock(locks_[index]);
    buckets_[index][key] = value;
}

template <typename KeyType, typename ValueType>
size_t ConcurrentMap<KeyType, ValueType>::getBucketIndex(const KeyType& key) const {
    return std::hash<KeyType> {}(key) % num_buckets_;
}

template <typename KeyType, typename ValueType>
std::vector<ValueType> ConcurrentMap<KeyType, ValueType>::values() {
    std::vector<ValueType> all_values;
    for (size_t i = 0; i < num_buckets_; ++i) {
        std::shared_lock<std::shared_mutex> lock(locks_[i]);
        for (const auto& pair : buckets_[i]) {
            all_values.push_back(pair.second);
        }
    }
    return all_values;
}

// Explicit template instantiation for the types you want to use
template class ConcurrentMap<doris::TUniqueId, std::shared_ptr<doris::PlanFragmentExecutor>>;
