#pragma once

#include <exec/detail/atomic_intrusive_queue.hpp>
#include <exec/detail/bwos_lifo_queue.hpp>
#include <exec/detail/xorshift.hpp>

#include <mutex>
#include <optional>
#include <thread>

namespace __pool::_park_::inline __v1 {

struct __task_base {
    using __execute_fn [[__gnu__::__nodebug__]] =
        void (*)(__task_base *, std::uint32_t __tid) noexcept;
    __task_base       *__next_    = nullptr;
    const __execute_fn __execute_ = nullptr;

    explicit __task_base(__execute_fn __fn) noexcept : __execute_(__fn) {}
};

struct __remote_queue {
    explicit __remote_queue(std::size_t __n) noexcept : __queues_(__n) {}

    explicit __remote_queue(__remote_queue *__next, std::size_t __n) noexcept
        : __queues_(__n), __next_(__next) {}

    std::vector<exec::__atomic_intrusive_queue<&__task_base::__next_>>
                                 __queues_{};
    __remote_queue              *__next_{};
    const std::thread::id        __id_ = std::this_thread::get_id();
    std::optional<std::uint32_t> __index_{};
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic<std::uint64_t> __qsbr_seq_{0U};
};

struct __remote_queue_pool { // NOLINT
    std::atomic<__remote_queue *> __head_;
    __remote_queue               *__tail_;
    const std::size_t             __n_thread_;
    __remote_queue                __this_;

    explicit __remote_queue_pool(std::size_t __n) noexcept
        : __head_{&__this_}, __tail_{&__this_}, __n_thread_(__n), __this_(__n) {
    }

    ~__remote_queue_pool() noexcept {
        __remote_queue *__head = __head_.load(std::memory_order_acquire);
        while (__head != __tail_) {
            __remote_queue *tmp = std::exchange(__head, __head->__next_);
            delete tmp;
        }
    }

    [[__nodiscard__]] auto _M_pop_all_reversed(std::size_t __tid) noexcept
        -> STDEXEC::__intrusive_queue<&__task_base::__next_> {
        __remote_queue *__head = __head_.load(std::memory_order_acquire);
        STDEXEC::__intrusive_queue<&__task_base::__next_> __task{};
        while (__head != nullptr) {
            __task.append(__head->__queues_[__tid].pop_all_reversed());
            __head = __head->__next_;
        }
        return __task;
    }

    [[__nodiscard__]] auto _M_get() -> __remote_queue * {
        thread_local std::thread::id _S_this_id = std::this_thread::get_id();
        __remote_queue *__head  = __head_.load(std::memory_order_acquire);
        __remote_queue *__queue = __head;
        while (__queue != __tail_) {
            if (__queue->__id_ == _S_this_id) { return __queue; }
            __queue = __queue->__next_;
        }
        auto *__new_head = new __remote_queue{__head, __n_thread_};
        while (!__head_.compare_exchange_weak(
            __head, __new_head, std::memory_order_acq_rel)) {
            __new_head->__next_ = __head;
        }
        return __new_head;
    }
};

struct __parschd;

struct __thread_state {
    struct __pop_result {
        __task_base  *__task_;
        std::uint32_t __src_;
    };

    explicit __thread_state(__parschd *__schd, const std::uint32_t __i) noexcept
        : __pool_(__schd), __index_(__i) {
        std::random_device __rd;
        __rng_.seed(__rd);
    }

    enum class _Sp : unsigned char { _S_run, _S_sleep, _S_retire, _S_notify };

    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic_bool __stop_requested_{false};
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic_bool __retire_requested_{false};
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic<_Sp> __state_{_Sp::_S_run};

    exec::bwos::lifo_queue<__task_base *, std::allocator<__task_base *>>
                                                      __local_{32, 8};
    STDEXEC::__intrusive_queue<&__task_base::__next_> __pending_;

    mutable std::mutex              __mut_;
    mutable std::condition_variable __cv_;
    __parschd                      *__pool_;
    exec::xorshift                  __rng_;
    const std::uint32_t             __index_;

    [[__nodiscard__]] auto _M_pop() noexcept -> __pop_result;
    void                   _M_start_retire() noexcept;
    void                   _M_push_local(__task_base *__task) noexcept {
        if (!__local_.push_back(__task)) { __pending_.push_back(__task); }
    }
    void _M_push_local(
        STDEXEC::__intrusive_queue<&__task_base::__next_> &&__tasks) noexcept {
        __pending_.prepend(std::move(__tasks));
    }

    // 唤醒一个可能在 _S_sleep 的线程。
    // - 对 _S_retire 短路:绝不 exchange 污染 retire 线程的 state_。
    // - 对 _S_notify 幂等:已 notify 过,直接返回。
    // - 仅当确实把 _S_sleep -> _S_notify 时才敲 cv(它在 cv_.wait 里)。
    // 线程能否前进由 wait 谓词从持久标志判定,这里只负责"敲一下"。
    void _M_notify_remote() noexcept {
        _Sp __old = __state_.load(std::memory_order_relaxed);
        for (;;) {
            if (__old == _Sp::_S_retire || __old == _Sp::_S_notify) { return; }
            if (__state_.compare_exchange_weak(
                    __old, _Sp::_S_notify, std::memory_order_relaxed)) {
                break;
            }
        }
        if (__old == _Sp::_S_sleep) {
            { std::lock_guard __l{__mut_}; }
            __cv_.notify_one();
        }
    }

    // 仅当 state_ 恰为 _S_sleep 时把它转 _S_notify 并唤醒,返回是否命中。
    // _M_notify_one_sleep 依赖精确返回值跳过非 sleeper。
    auto _M_notify_sleep() noexcept -> bool {
        _Sp __exp = _Sp::_S_sleep;
        if (!__state_.compare_exchange_strong(
                __exp, _Sp::_S_notify, std::memory_order_relaxed)) {
            return false;
        }
        { std::lock_guard __l{__mut_}; }
        __cv_.notify_one();
        return true;
    }

    void _M_request_stop() noexcept {
        __stop_requested_.store(true, std::memory_order_release);
        { std::lock_guard __l{__mut_}; } // 与两处 wait 配对,防丢唤醒
        __cv_.notify_one();
    }

    void _M_request_retire() noexcept {
        bool __exp = false;
        if (__retire_requested_.compare_exchange_strong(
                __exp, true, std::memory_order_relaxed)) {
            _M_notify_remote(); // 把可能在 _S_sleep 的它捅到 pop 安全点观察请求
        }
    }

    void _M_request_resume() noexcept {
        bool __exp = true;
        if (__retire_requested_.compare_exchange_strong(
                __exp, false, std::memory_order_relaxed)) {
            { std::lock_guard __l{__mut_}; } // 与 retire 的 wait 配对
            __cv_.notify_one();
        }
    }

    [[__nodiscard__]] auto _M_try_pop() -> __pop_result {
        __pop_result __result{.__task_ = nullptr, .__src_ = __index_};
        __result.__task_ = __local_.pop_back();
        if (__result.__task_ != nullptr) [[__likely__]] { return __result; }
        return _M_try_remote();
    }
    [[__nodiscard__]] auto _M_try_remote() -> __pop_result;
    [[__nodiscard__]] auto _M_try_steal_any() -> __pop_result;

    void _M_notify_one_sleep() noexcept;
    void _M_set_steal() const noexcept;
    void _M_clear_steal() noexcept;
    void _M_set_sleep() const noexcept;
    void _M_clear_sleep() noexcept;
};

struct __parschd { // NOLINT
    [[__nodiscard__]] static auto _S_hardware_concurrency() noexcept
        -> unsigned int {
        unsigned int const __n = std::thread::hardware_concurrency();
        return __n == 0 ? 1 : __n;
    }

    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic<std::uint32_t> __active_{};
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic<std::uint64_t> __epoch_{1U};
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic_bool __wait_{false};
    alignas(__GCC_DESTRUCTIVE_SIZE) __remote_queue_pool __remote_;

    std::uint32_t                              __n_thread_;
    std::vector<std::thread>                   __thread_;
    std::vector<std::optional<__thread_state>> __state_;
    std::vector<std::atomic<std::uint32_t>>    __access_;
    std::vector<std::uint32_t>                 __offset_;
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic<std::uint32_t> __size_;
    mutable std::mutex __qsbr_mut_;

    auto _M_get_remote_queue() noexcept -> __remote_queue * {
        __remote_queue *__queue = __remote_._M_get();
        if (__queue->__index_.has_value()) [[__likely__]] { return __queue; }
        __queue->__index_.emplace((std::numeric_limits<std::size_t>::max)());
        std::size_t __i{};
        for (std::thread &__t : __thread_) {
            if (__t.get_id() == __queue->__id_) {
                __queue->__index_.emplace(__i);
                break;
            }
            ++__i;
        }
        return __queue;
    }

    void _M_qsbr_lock(__remote_queue *__rq) noexcept {
        __rq->__qsbr_seq_.store(
            __epoch_.load(std::memory_order_relaxed),
            std::memory_order_seq_cst);
    }
    void _M_qsbr_unlock(__remote_queue *__rq) noexcept {
        __rq->__qsbr_seq_.store(0, std::memory_order_release);
        if (__wait_.load(std::memory_order_acquire)) {
            __wait_.store(false, std::memory_order_release);
            __wait_.notify_all();
        }
    }
    [[__nodiscard__]] auto _M_qsbr_test(const std::uint64_t __ep) noexcept
        -> bool {
        for (__remote_queue *__head =
                 __remote_.__head_.load(std::memory_order_acquire);
             __head != nullptr; __head = __head->__next_) {
            const auto __tmp =
                __head->__qsbr_seq_.load(std::memory_order_seq_cst);
            if (__tmp != 0 && __tmp < __ep) { return false; }
        }
        return true;
    }
    void _M_qsbr_synchronize() noexcept {
        const std::uint64_t __ep =
            __epoch_.fetch_add(1U, std::memory_order_seq_cst) + 1U;
        if (_M_qsbr_test(__ep)) { return; }
        while (true) {
            __wait_.store(true, std::memory_order_seq_cst);
            if (_M_qsbr_test(__ep)) [[__unlikely__]] { break; }
            __wait_.wait(true);
            if (_M_qsbr_test(__ep)) [[__likely__]] { break; }
        }
    }

    void _M_request_stop() noexcept {
        for (auto &__s : __state_) {
            __s->__stop_requested_.store(true, std::memory_order_release);
        }
        for (auto &__s : __state_) { __s->_M_request_stop(); }
    }
    void _M_request_resume() noexcept {
        for (auto &__s : __state_) { __s->_M_request_resume(); }
    }
    void _M_join() noexcept {
        for (auto &__t : __thread_) { __t.join(); }
    }
    auto _M_enqueue(__task_base *__task) noexcept -> bool {
        __remote_queue   *__queue = _M_get_remote_queue();
        const std::size_t __idx   = __queue->__index_.value();
        if (__idx < __state_.size()) {
            __state_[__idx]->_M_push_local(__task);
            return true;
        }

        thread_local std::uint64_t __start =
            std::uint64_t(std::random_device{}());
        ++__start;
        _M_qsbr_lock(__queue);
        const std::uint32_t __k = __size_.load(std::memory_order_acquire);
        if (__k == 0) [[__unlikely__]] {
            _M_qsbr_unlock(__queue);
            return false;
        }
        const std::size_t __target =
            __access_[__start % __k].load(std::memory_order_acquire);
        __queue->__queues_[__target].push_front(__task);
        __state_[__target]->_M_notify_remote();
        _M_qsbr_unlock(__queue);
        return true;
    }
    inline void _M_run(const std::uint32_t __i) noexcept {
        while (true) {
            auto [__task, __src] = __state_[__i]->_M_pop();
            if (__task == nullptr) [[__unlikely__]] { return; }
            (*__task->__execute_)(__task, __src);
        }
    }
    explicit __parschd(
        const std::uint32_t __n = _S_hardware_concurrency()) noexcept // NOLINT
        : __remote_(__n), __n_thread_(__n), __state_(__n), __access_(__n),
          __offset_(__n), __size_(__n) {
        for (std::uint32_t __i{}; __i < __n; ++__i) {
            __state_[__i].emplace(this, __i);
            __access_[__i].store(__i, std::memory_order_relaxed);
            __offset_[__i] = __i;
        }

        __thread_.reserve(__n);
        __active_.store(__n << 16U, std::memory_order_relaxed);
        for (std::uint32_t __index{}; __index < __n; ++__index) {
            __thread_.emplace_back(
                [this, __index] noexcept -> void { _M_run(__index); });
        }
    }
    ~__parschd() noexcept {
        // 先全员置 stop(release),再 resume 把 retire 线程拉出 wait,
        // 最后统一 stop-notify。stop 优先级最高,谓词含 __stop_requested_。
        for (auto &__s : __state_) {
            __s->__stop_requested_.store(true, std::memory_order_release);
        }
        _M_request_resume();
        _M_request_stop();
        _M_join();
    }
};

inline void _S_move_pending_to_local(
    STDEXEC::__intrusive_queue<&__task_base::__next_> &__pending,
    exec::bwos::lifo_queue<__task_base *, std::allocator<__task_base *>>
        &__local) {
    auto const __last = __local.push_back(__pending.begin(), __pending.end());
    STDEXEC::__intrusive_queue<&__task_base::__next_> __tmp{};
    __tmp.splice(__tmp.begin(), __pending, __pending.begin(), __last);
    __tmp.clear();
}

[[__nodiscard__]] inline auto __thread_state::_M_try_remote() -> __pop_result {
    __pop_result __result{.__task_ = nullptr, .__src_ = __index_};
    STDEXEC::__intrusive_queue<&__task_base::__next_> __remote =
        __pool_->__remote_._M_pop_all_reversed(__index_);
    __pending_.append(std::move(__remote));
    if (!__pending_.empty()) {
        _S_move_pending_to_local(__pending_, __local_);
        __result.__task_ = __local_.pop_back();
    }
    return __result;
}
[[__nodiscard__]] inline auto __thread_state::_M_try_steal_any()
    -> __pop_result {
    std::uniform_int_distribution<std::uint32_t> __dist(
        0, static_cast<std::uint32_t>(__pool_->__n_thread_ - 2));
    std::uint32_t __idx = __dist(__rng_);
    __idx += std::uint32_t(__idx >= __index_);
    return {
        .__task_ = __pool_->__state_[__idx]->__local_.steal_front(),
        .__src_  = __idx};
}
inline void __thread_state::_M_notify_one_sleep() noexcept {
    std::uniform_int_distribution<std::uint32_t> __dist(
        0, __pool_->__n_thread_ - 1);
    const std::uint32_t __start = __dist(__rng_);
    for (std::uint32_t __i{}; __i < __pool_->__n_thread_; ++__i) {
        std::uint32_t __idx = (__start + __i) % __pool_->__n_thread_;
        if (__idx == __index_) { continue; }
        if (__pool_->__state_[__idx]->_M_notify_sleep()) { return; }
    }
}
inline void __thread_state::_M_set_steal() const noexcept {
    std::uint32_t const __n = 1U - (1U << 16U);
    __pool_->__active_.fetch_add(__n, std::memory_order_relaxed);
}
inline void __thread_state::_M_clear_steal() noexcept {
    constexpr std::uint32_t diff = 1 - (1U << 16U);
    std::uint32_t const     __active =
        __pool_->__active_.fetch_sub(diff, std::memory_order_relaxed);
    std::uint32_t const __n_victim = __active >> 16U;
    std::uint32_t const __n_thief  = __active & 0xffffU;
    if (__n_thief == 1 && __n_victim != 0) { _M_notify_one_sleep(); }
}
inline void __thread_state::_M_set_sleep() const noexcept {
    __pool_->__active_.fetch_sub(1U << 16U, std::memory_order_relaxed);
}
inline void __thread_state::_M_clear_sleep() noexcept {
    std::uint32_t const __active =
        __pool_->__active_.fetch_add(1U << 16U, std::memory_order_relaxed);
    if (__active == 0) { _M_notify_one_sleep(); }
}
[[__nodiscard__]] inline auto __thread_state::_M_pop() noexcept
    -> __pop_result {
    __pop_result __result = _M_try_pop();
    while (__result.__task_ == nullptr) {
        _M_set_steal();
        for (std::size_t __i{}; __i < __pool_->__n_thread_ + 1; ++__i) {
            __result = _M_try_steal_any();
            if (__result.__task_ != nullptr) {
                _M_clear_steal();
                return __result;
            }
        }
#if defined(__x86_64__) || defined(__i386__)
        __builtin_ia32_pause();
#else
        std::this_thread::yield();
#endif
        _M_clear_steal();

        if (__stop_requested_.load(std::memory_order_acquire))
            [[__unlikely__]] {
            return __result;
        }
        if (__retire_requested_.load(std::memory_order_acquire))
            [[__unlikely__]] {
            _M_start_retire();
            if (__stop_requested_.load(std::memory_order_acquire)) {
                return {.__task_ = nullptr, .__src_ = __index_};
            }
            __state_.store(_Sp::_S_run, std::memory_order_relaxed);
            __result = _M_try_pop();
            continue;
        }
        _Sp              __exp = _Sp::_S_run;
        std::unique_lock __lock{__mut_};
        if (__state_.compare_exchange_weak(
                __exp, _Sp::_S_sleep, std::memory_order_relaxed)) {
            __result = _M_try_remote();
            if (__result.__task_ != nullptr) [[__unlikely__]] {
                __lock.unlock();
                __state_.store(_Sp::_S_run, std::memory_order_relaxed);
                return __result;
            }
            _M_set_sleep();
            __cv_.wait(__lock, [this] noexcept -> bool {
                return __state_.load(std::memory_order_relaxed) ==
                           _Sp::_S_notify ||
                       __stop_requested_.load(std::memory_order_relaxed) ||
                       __retire_requested_.load(std::memory_order_relaxed);
            });
            __lock.unlock();
            _M_clear_sleep();
        }
        if (__lock.owns_lock()) { __lock.unlock(); }
        __state_.store(_Sp::_S_run, std::memory_order_relaxed);
        __result = _M_try_pop();
    }
    return __result;
}
inline void __thread_state::_M_start_retire() noexcept {
    {
        std::lock_guard __lock{__pool_->__qsbr_mut_};

        const std::uint32_t __j = __pool_->__offset_[__index_];
        const std::uint32_t __last =
            __pool_->__size_.load(std::memory_order_relaxed) - 1U;
        const std::uint32_t __w =
            __pool_->__access_[__last].load(std::memory_order_relaxed);

        __pool_->__access_[__j].store(__w, std::memory_order_release);
        __pool_->__offset_[__w] = __j;

        __pool_->__access_[__last].store(__index_, std::memory_order_relaxed);
        __pool_->__offset_[__index_] = __last;

        __pool_->__size_.store(__last, std::memory_order_release);
    }
    __pool_->_M_qsbr_synchronize();

    STDEXEC::__intrusive_queue<&__task_base::__next_> __tmp{};
    for (__task_base *__t = __local_.pop_back(); __t != nullptr;
         __t              = __local_.pop_back()) {
        __tmp.push_back(__t);
    }
    __tmp.prepend(__pool_->__remote_._M_pop_all_reversed(__index_));
    __tmp.prepend(std::move(__pending_));

    if (!__tmp.empty()) {
        __remote_queue *__queue = __pool_->_M_get_remote_queue();

        thread_local std::uint64_t __start =
            std::uint64_t(std::random_device{}());
        ++__start;
        __pool_->_M_qsbr_lock(__queue);
        const std::uint32_t __k =
            __pool_->__size_.load(std::memory_order_acquire);
        if (__k == 0) [[__unlikely__]] {
            __pool_->_M_qsbr_unlock(__queue);
            // 全员挂起:不在 retire 中途执行用户任务,
            // 留回自己 __pending_,resume 后 _M_try_pop 自取,不丢不重。
            __pending_.prepend(std::move(__tmp));
        } else {
            const std::size_t __target = __pool_->__access_[__start % __k].load(
                std::memory_order_acquire);
            __queue->__queues_[__target].prepend(std::move(__tmp));
            __pool_->__state_[__target]->_M_notify_remote();
            __pool_->_M_qsbr_unlock(__queue);
        }
    }

    std::unique_lock __lock{__mut_};
    __state_.store(_Sp::_S_retire, std::memory_order_relaxed); // 持锁内写
    _M_set_sleep();
    __cv_.wait(__lock, [this] noexcept -> bool {
        return !__retire_requested_.load(std::memory_order_relaxed) ||
               __stop_requested_.load(std::memory_order_relaxed);
    });
    __lock.unlock();
    _M_clear_sleep();

    if (__stop_requested_.load(std::memory_order_acquire)) {
        // 析构击穿:线程即将退出,不重新加入可见集。
        return;
    }

    {
        std::lock_guard     __lock{__pool_->__qsbr_mut_};
        const std::uint32_t __k =
            __pool_->__size_.load(std::memory_order_relaxed);
        __pool_->__access_[__k].store(__index_, std::memory_order_release);
        __pool_->__offset_[__index_] = __k;
        __pool_->__size_.store(__k + 1U, std::memory_order_release);
    }
}
} // namespace __pool::_park_::inline __v1
