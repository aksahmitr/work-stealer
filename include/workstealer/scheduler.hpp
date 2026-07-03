#pragma once

#include "workstealer/detail/worker_deque.hpp"
#include "workstealer/task.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace workstealer {

class Scheduler {
 public:
  using Task = std::function<void()>;

  explicit Scheduler(std::size_t threads = std::thread::hardware_concurrency()) {
    if (threads == 0) {
      threads = 4;
    }
    workers_.resize(threads);
    for (std::size_t i = 0; i < threads; ++i) {
      workers_[i].deque = std::make_unique<detail::WorkerDeque<Task>>();
    }
    for (std::size_t i = 0; i < threads; ++i) {
      workers_[i].thread = std::thread([this, i] { worker_loop(i); });
    }
  }

  ~Scheduler() { shutdown(); }

  Scheduler(const Scheduler&) = delete;
  Scheduler& operator=(const Scheduler&) = delete;

  std::size_t thread_count() const { return workers_.size(); }

  template <typename F>
  void submit_detached(F&& f) {
    static_assert(std::is_invocable_v<std::decay_t<F>>);
    enqueue_task(std::forward<F>(f));
  }

  template <typename F>
  TaskHandle submit(F&& f) {
    static_assert(std::is_invocable_v<std::decay_t<F>>);
    auto node = std::make_shared<detail::TaskNode>();
    node->scheduler = this;
    node->work = [node, fn = std::forward<F>(f)]() mutable {
      fn();
      node->on_complete();
    };
    enqueue_task(std::move(node->work));
    return TaskHandle{std::move(node)};
  }

  template <typename F>
  TaskHandle submit(F&& f, std::initializer_list<TaskHandle> deps) {
    static_assert(std::is_invocable_v<std::decay_t<F>>);
    auto node = std::make_shared<detail::TaskNode>();
    node->scheduler = this;
    node->work = [node, fn = std::forward<F>(f)]() mutable {
      fn();
      node->on_complete();
    };

    std::size_t dep_count = 0;
    for (const TaskHandle& d : deps) {
      if (!d.valid()) {
        continue;
      }
      d.node_->register_successor(node);
      ++dep_count;
    }
    node->unfinished_deps.store(static_cast<std::uint32_t>(dep_count),
                               std::memory_order_relaxed);

    if (dep_count == 0) {
      enqueue_task(node->work);
    }
    return TaskHandle{std::move(node)};
  }

  template <typename F>
  void parallel_for(std::size_t begin, std::size_t end, F&& body,
                    std::size_t grain = 0) {
    if (begin >= end) {
      return;
    }
    const std::size_t n = end - begin;
    if (grain == 0) {
      grain = std::max<std::size_t>(1, n / (thread_count() * 4));
    }

    struct Shared {
      std::atomic<std::size_t> remaining{0};
      std::mutex mu;
      std::condition_variable cv;
    };
    auto shared = std::make_shared<Shared>();

    auto body_fn = std::make_shared<std::decay_t<F>>(std::forward<F>(body));

    const std::size_t chunks =
        (n + grain - 1) / grain;
    shared->remaining.store(chunks, std::memory_order_relaxed);

    for (std::size_t c = 0; c < chunks; ++c) {
      const std::size_t chunk_begin = begin + c * grain;
      const std::size_t chunk_end = std::min(end, chunk_begin + grain);
      submit_detached([shared, chunk_begin, chunk_end, body_fn]() {
        for (std::size_t i = chunk_begin; i < chunk_end; ++i) {
          (*body_fn)(i);
        }
        if (shared->remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
          shared->cv.notify_one();
        }
      });
    }

    std::unique_lock lock(shared->mu);
    shared->cv.wait(lock, [&] {
      return shared->remaining.load(std::memory_order_acquire) == 0;
    });
  }

  void shutdown() {
    bool expected = false;
    if (!stopped_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      return;
    }
    for (auto& w : workers_) {
      w.deque->push([] {});
    }
    for (auto& w : workers_) {
      if (w.thread.joinable()) {
        w.thread.join();
      }
    }
  }

  void schedule_ready(detail::TaskNode* node) { enqueue_task(node->work); }

  void enqueue_task(Task task) {
    const std::size_t idx =
        next_victim_.fetch_add(1, std::memory_order_relaxed) % workers_.size();
    workers_[idx].deque->push(std::move(task));
  }

 private:
  struct Worker {
    std::unique_ptr<detail::WorkerDeque<Task>> deque;
    std::thread thread;
  };

  std::optional<Task> steal_from_others(std::size_t self, std::mt19937& rng) {
    const std::size_t n = workers_.size();
    if (n <= 1) {
      return std::nullopt;
    }
    std::uniform_int_distribution<std::size_t> dist(0, n - 2);
    for (std::size_t attempt = 0; attempt < n * 2; ++attempt) {
      std::size_t victim = dist(rng);
      if (victim >= self) {
        ++victim;
      }
      if (auto stolen = workers_[victim].deque->steal()) {
        return stolen;
      }
    }
    return std::nullopt;
  }

  void worker_loop(std::size_t index) {
    thread_local std::mt19937 rng{std::random_device{}() + static_cast<unsigned>(index)};

    while (!stopped_.load(std::memory_order_acquire)) {
      std::optional<Task> job = workers_[index].deque->pop();
      if (!job) {
        job = steal_from_others(index, rng);
      }
      if (job) {
        (*job)();
        continue;
      }
      std::this_thread::yield();
    }
  }

  std::vector<Worker> workers_;
  std::atomic<std::size_t> next_victim_{0};
  std::atomic<bool> stopped_{false};
};

namespace detail {

inline void task_node_on_complete(TaskNode* self) {
  for (const auto& succ : self->successors) {
    if (succ->unfinished_deps.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      self->scheduler->schedule_ready(succ.get());
    }
  }
}

inline void TaskNode::on_complete() { task_node_on_complete(this); }

}  // namespace detail

}  // namespace workstealer
