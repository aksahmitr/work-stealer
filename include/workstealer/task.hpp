#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

namespace workstealer {

class Scheduler;

namespace detail {
struct TaskNode;
}

class TaskHandle {
 public:
  TaskHandle() = default;

  bool valid() const { return static_cast<bool>(node_); }

 private:
  friend class Scheduler;
  friend struct detail::TaskNode;

  explicit TaskHandle(std::shared_ptr<detail::TaskNode> node) : node_(std::move(node)) {}

  std::shared_ptr<detail::TaskNode> node_;
};

namespace detail {

struct TaskNode {
  using Work = std::function<void()>;

  Work work;
  std::atomic<std::uint32_t> unfinished_deps{0};
  std::vector<std::shared_ptr<TaskNode>> successors;
  Scheduler* scheduler{nullptr};

  void register_successor(const std::shared_ptr<TaskNode>& succ) {
    succ->unfinished_deps.fetch_add(1, std::memory_order_relaxed);
    successors.push_back(succ);
  }

  void on_complete();
};

}  // namespace detail
}  // namespace workstealer
