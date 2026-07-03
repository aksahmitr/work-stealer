#pragma once

#include <deque>
#include <mutex>
#include <optional>
#include <utility>

namespace workstealer::detail {

template <typename T>
class WorkerDeque {
 public:
  void push(T item) {
    std::lock_guard lock(tail_mu_);
    items_.push_back(std::move(item));
  }

  std::optional<T> pop() {
    {
      std::lock_guard tail_lock(tail_mu_);
      if (items_.empty()) {
        return std::nullopt;
      }
      if (items_.size() > 1) {
        T item = std::move(items_.back());
        items_.pop_back();
        return item;
      }
    }
    std::scoped_lock both(head_mu_, tail_mu_);
    if (items_.empty()) {
      return std::nullopt;
    }
    T item = std::move(items_.back());
    items_.pop_back();
    return item;
  }

  std::optional<T> steal() {
    {
      std::lock_guard head_lock(head_mu_);
      if (items_.empty()) {
        return std::nullopt;
      }
      if (items_.size() > 1) {
        T item = std::move(items_.front());
        items_.pop_front();
        return item;
      }
    }
    std::scoped_lock both(head_mu_, tail_mu_);
    if (items_.empty()) {
      return std::nullopt;
    }
    T item = std::move(items_.front());
    items_.pop_front();
    return item;
  }

 private:
  std::mutex head_mu_;
  std::mutex tail_mu_;
  std::deque<T> items_;
};

}  // namespace workstealer::detail
