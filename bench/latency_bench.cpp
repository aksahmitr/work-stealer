#include "workstealer/scheduler.hpp"

#include <atomic>
#include <chrono>
#include <algorithm>
#include <cmath>
#include <cstdio>
#include <deque>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

static double percentile(std::vector<double> samples, double p) {
  std::sort(samples.begin(), samples.end());
  const std::size_t i =
      std::min(samples.size() - 1,
               static_cast<std::size_t>(std::ceil(p * samples.size())) - 1);
  return samples.empty() ? 0.0 : samples[i];
}

class CentralizedPool {
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> queue_;
  std::vector<std::thread> workers_;
  bool stop_{false};

 public:
  explicit CentralizedPool(std::size_t n) {
    for (std::size_t i = 0; i < n; ++i) {
      workers_.emplace_back([this] {
        for (;;) {
          std::function<void()> job;
          {
            std::unique_lock lock(mu_);
            cv_.wait(lock, [&] { return stop_ || !queue_.empty(); });
            if (stop_ && queue_.empty()) {
              return;
            }
            job = std::move(queue_.front());
            queue_.pop_front();
          }
          job();
        }
      });
    }
  }

  ~CentralizedPool() {
    {
      std::lock_guard lock(mu_);
      stop_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) {
      t.join();
    }
  }

  void submit(std::function<void()> f) {
    {
      std::lock_guard lock(mu_);
      queue_.push_back(std::move(f));
    }
    cv_.notify_one();
  }
};

static std::vector<double> measure(const std::function<void(std::function<void()>)>& submit,
                                   int slow_ms, int num_slow, std::size_t submitters,
                                   std::size_t fast_each) {
  const auto slow = [slow_ms] {
    std::this_thread::sleep_for(std::chrono::milliseconds(slow_ms));
  };
  const auto fast = [] {
    volatile int x = 0;
    for (int k = 0; k < 50; ++k) {
      x += k;
    }
    (void)x;
  };

  for (int i = 0; i < num_slow; ++i) {
    submit(slow);
  }

  const std::size_t total = submitters * fast_each;
  std::vector<double> us(total);
  std::atomic<std::size_t> done{0};

  std::vector<std::thread> threads;
  for (std::size_t s = 0; s < submitters; ++s) {
    threads.emplace_back([&, s] {
      for (std::size_t i = 0; i < fast_each; ++i) {
        const std::size_t idx = s * fast_each + i;
        const auto t0 = Clock::now();
        submit([&, idx, t0] {
          us[idx] = std::chrono::duration<double, std::micro>(Clock::now() - t0).count();
          fast();
          ++done;
        });
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  while (done.load() < total) {
    std::this_thread::yield();
  }
  return us;
}

static void print_row(const char* name, const std::vector<double>& us) {
  std::printf("%-22s  p50: %8.1f us   p99: %8.1f us\n", name, percentile(us, 0.5),
              percentile(us, 0.99));
}

int main() {
  const std::size_t workers = std::max(4u, std::thread::hardware_concurrency());
  const int slow_ms = static_cast<int>(workers / 2 + 1);
  const int num_slow = static_cast<int>(workers * (workers / 2 + 3));
  const std::size_t submitters = workers * (workers / 2 + 7);
  const std::size_t fast_each = (workers + 14) / 4;

  std::printf("task-start latency  workers=%zu  samples=%zu\n\n", workers,
              submitters * fast_each);

  CentralizedPool pool(workers);
  print_row("Centralized queue",
            measure([&](auto f) { pool.submit(std::move(f)); }, slow_ms, num_slow,
                    submitters, fast_each));

  workstealer::Scheduler sched(workers);
  print_row("Work-stealing",
            measure([&](auto f) { sched.submit_detached(std::move(f)); }, slow_ms,
                    num_slow, submitters, fast_each));
  sched.shutdown();
  return 0;
}
