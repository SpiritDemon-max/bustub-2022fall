//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  frame_time_.resize(num_frames + 1);  // 每个frame_id的vector进行初始化
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  // hist_list非空先删除小于k次的frame
  if (!hist_list_.empty()) {
    for (auto it = hist_list_.rbegin(); it != hist_list_.rend(); it++) {
      frame_id_t now_frame_id = (*it).second;
      auto &evi_frame = frame_map_[now_frame_id];

      if (evi_frame.evictable_) {
        *frame_id = now_frame_id;
        frame_time_[now_frame_id].clear();  // 对应frame的时间帧也要删除
        evi_frame.count_ = 0;               // 计数清0
        hist_list_.erase((++it).base());    // hist中删除该frame
        curr_size_--;
        return true;
      }
    }
  }

  // hist为空，且cache非空删除大于等于k次的frame
  if (!cache_list_.empty()) {
    for (auto it = cache_list_.begin(); it != cache_list_.end(); it++) {
      frame_id_t now_frame_id = (*it).second;
      auto &evi_frame = frame_map_[now_frame_id];

      if (evi_frame.evictable_) {
        *frame_id = now_frame_id;
        frame_time_[now_frame_id].clear();  // 对应frame的时间帧也要删除
        evi_frame.count_ = 0;               // 计数清0
        cache_list_.erase(it);              // cache中删除该frame
        curr_size_--;
        return true;
      }
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  // frame id 越界，抛出异常
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("Invalid frame_id ") + std::to_string(frame_id));
  }

  auto &acc_frame = frame_map_[frame_id];
  auto &acc_time = frame_time_[frame_id];

  // 每次当前frame计数增加
  size_t new_count = ++acc_frame.count_;
  // 时间帧压入vector
  acc_time.emplace_back(++current_timestamp_);

  // frame第一次加入hist_list，更新frame信息
  if (new_count == 1) {
    ++curr_size_;
    k_time new_cache(current_timestamp_, frame_id);
    hist_list_.emplace_front(new_cache);
    acc_frame.it_ = hist_list_.begin();

  } else {
    // 第k次从hist删除加入cache，大于k次删除原本位置，更新位置
    if (new_count == k_) {
      hist_list_.erase(acc_frame.it_);
    } else if (new_count > k_) {
      cache_list_.erase(acc_frame.it_);
    } else {
      return;  // 其余情况不用处理
    }

    acc_frame.kth_ = acc_time.front();  // 更新时间帧
    acc_time.erase(acc_time.begin());

    k_time new_cache(acc_frame.kth_, frame_id);
    // 二分维护当前frame在cache中的位置
    auto it = std::upper_bound(cache_list_.begin(), cache_list_.end(), new_cache);
    it = cache_list_.insert(it, new_cache);
    acc_frame.it_ = it;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("Invalid frame_id ") + std::to_string(frame_id));
  }

  auto &set_frame = frame_map_[frame_id];

  // frame的evictable_和函数参数不一致，更新curr_size
  if (set_frame.evictable_ && !set_evictable) {
    curr_size_--;
  } else if (!set_frame.evictable_ && set_evictable) {
    curr_size_++;
  }
  set_frame.evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  auto it = frame_map_.find(frame_id);
  auto &del_frame = it->second;
  // frame不存在直接return
  if (it == frame_map_.end() || del_frame.count_ == 0) {
    return;
  }

  // 页面不可删除抛出异常
  if (!del_frame.evictable_) {
    throw std::logic_error(std::string("Can't remove an inevictable frame ") + std::to_string(frame_id));
  }

  if (del_frame.count_ < k_ && del_frame.count_ > 0) {  // 小于k次说明frame在hist中
    hist_list_.erase(del_frame.it_);
  } else if (del_frame.count_ >= k_) {  // 否则在cache中
    cache_list_.erase(del_frame.it_);
  }

  // 更新删除frame的信息
  frame_time_[frame_id].clear();
  del_frame.count_ = 0;
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);

  return curr_size_;
}

}  // namespace bustub
