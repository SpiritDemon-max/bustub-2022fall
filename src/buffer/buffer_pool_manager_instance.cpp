//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;

  Page *page = pages_;

  // free队列不空表示有空闲的frame
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = page + frame_id;
  } else {
    // 判断是否有可驱除的frame
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    page = page + frame_id;
    // 有可驱除的frame判断是否为脏页，写回主存
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->page_id_, page->data_);
    }
    // 页表删除映射关系
    page_table_->Remove(page->page_id_);
  }

  // 更新page信息
  *page_id = AllocatePage();
  page->ResetMemory();
  page->page_id_ = *page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;

  // 更新lru_k中frame
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  // 更新页表
  page_table_->Insert(*page_id, frame_id);

  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  Page *page = pages_;

  // 判断页表是否有page的映射关系
  if (page_table_->Find(page_id, frame_id)) {
    page += frame_id;
    page->pin_count_++;
  } else {
    // 不在页表内则判断是否有空闲的frame
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
      page += frame_id;
    } else {
      // 没有空闲的frame则根据lru_k判断是否有可删除的frame
      if (!replacer_->Evict(&frame_id)) {
        return nullptr;
      }

      page += frame_id;
      // 页面为脏页写回磁盘
      if (page->IsDirty()) {
        disk_manager_->WritePage(page->page_id_, page->data_);
      }
      page_table_->Remove(page->page_id_);
    }

    // 更新信息
    page->ResetMemory();
    page->page_id_ = page_id;
    page->is_dirty_ = false;
    page->pin_count_ = 1;

    page_table_->Insert(page_id, frame_id);

    // 从磁盘读页面
    disk_manager_->ReadPage(page->page_id_, page->data_);
  }

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  // page不在frame或者page已经unpin返回false
  if (!page_table_->Find(page_id, frame_id) || pages_[frame_id].pin_count_ == 0) {
    return false;
  }

  // page的pin为0，frame设为可删除
  if (--pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  // 设置脏页
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = true;
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;

  // page不在frame或者无效返回false
  if (!page_table_->Find(page_id, frame_id) || page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto &page = pages_[frame_id];

  // 写回磁盘
  disk_manager_->WritePage(page_id, page.data_);
  page.is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;

  // 遍历所有frame，全部写回磁盘
  for (frame_id = 0; frame_id < static_cast<int>(GetPoolSize()); frame_id++) {
    auto &page = pages_[frame_id];

    if (page.page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page.page_id_, page.data_);
      page.is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  frame_id_t frame_id;
  // page不存在return true
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }

  auto &page = pages_[frame_id];
  // page不可以删除return false
  if (page.pin_count_ > 0) {
    return false;
  }

  // 脏页写回磁盘
  if (page.is_dirty_) {
    disk_manager_->WritePage(page.page_id_, page.data_);
    page.is_dirty_ = false;
  }

  // 更新信息
  replacer_->SetEvictable(frame_id, true);
  replacer_->Remove(frame_id);

  page_table_->Remove(page_id);
  page.ResetMemory();
  free_list_.emplace_back(frame_id);

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
