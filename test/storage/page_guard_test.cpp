//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, ReadSampleTest) {
  constexpr size_t buffer_pool_size = 5;
  constexpr size_t k = 2;

  const auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  const auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  auto guarded_page = ReadPageGuard(bpm.get(), page0);

  EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  EXPECT_EQ(1, page0->GetPinCount());

  {
    auto *page2 = bpm->NewPage(&page_id_temp);
    page2->RLatch();
    auto guard2 = ReadPageGuard(bpm.get(), page2);
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, WriteConcurrencySampleTest) {
  constexpr size_t buffer_pool_size = 5;
  constexpr size_t k = 2;

  const auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);
  DiskScheduler disk_scheduler(disk_manager.get());

  page_id_t write_page_id;
  { auto new_page = bpm->NewPageGuarded(&write_page_id); }

  std::vector<std::thread> threads;
  for (int tid = 0; tid < 4; tid++) {
    std::thread t([&bpm, &disk_scheduler, write_page_id] {
      auto write_page_guard = bpm->FetchPageWrite(write_page_id);
      char *data = write_page_guard.AsMut<char>();
      memcpy(data, "Hello world", 12);
      disk_scheduler.Schedule({true, data, write_page_id, DiskScheduler::CreatePromise()});
    });
    threads.push_back(std::move(t));
  }

  for (auto &t : threads) {
    t.join();
  }

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

}  // namespace bustub
