#pragma once

#include <exec/detail/atomic_intrusive_queue.hpp>
#include <exec/detail/bwos_lifo_queue.hpp>
#include <exec/detail/xorshift.hpp>
#include <stdexec/__detail/__manual_lifetime.hpp>
#include <stdexec/__detail/__utility.hpp>

#include <linux/futex.h>
#include <linux/membarrier.h>
#include <mutex>
#include <sys/syscall.h>
#include <thread>
#include <unistd.h>

#if defined(__SANITIZE_THREAD__) ||                                            \
    (defined(__has_feature) && __has_feature(thread_sanitizer))
extern "C" {
void __tsan_acquire(void *__addr);
void __tsan_release(void *__addr);
}
    #define _TSAN_ACQUIRE(__addr)                                              \
        __tsan_acquire(                                                        \
            const_cast<void *>(static_cast<const volatile void *>(__addr)))
    #define _TSAN_RELEASE(__addr)                                              \
        __tsan_release(                                                        \
            const_cast<void *>(static_cast<const volatile void *>(__addr)))
#else
    #define _TSAN_ACQUIRE(__addr) ((void)(__addr))
    #define _TSAN_RELEASE(__addr) ((void)(__addr))
#endif

#ifndef _GR_max_n_parschd_obj
    #define _GR_max_n_parschd_obj 8
#endif
#ifndef _Gr_max_n_concurrency
    #define _Gr_max_n_concurrency 512
#endif

namespace __gr::__resumable__::inline __v1 {

template <std::default_initializable _Ty, std::size_t _Np> struct __vector {
    _Ty         __data_[_Np];
    std::size_t __size_{};
    template <typename... _Args>
    [[__gnu__::__always_inline__, __gnu__::__artificial__]] //
    inline auto _M_resize_emplace(std::size_t __pos, _Args &&...__args) noexcept
        -> _Ty & {
        if (__pos >= __size_) [[__unlikely__]] { __size_ = __pos + 1; }
        // NOLINTNEXTLINE
        return *::new (&__data_[__pos]) _Ty{std::forward<_Args>(__args)...};
    }
};

[[__gnu__::__always_inline__, __gnu__::__artificial__]] //
inline auto _S_syscall_memory_barrier(const int __cmd) noexcept -> long {
    return ::syscall(__NR_membarrier, __cmd, 0, 0);     // NOLINT
}
[[__gnu__::__always_inline__, __gnu__::__artificial__]] //
inline auto _S_syscall_futex_wait_private(void *__addr, const int __x) noexcept
    -> long {
    // NOLINTNEXTLINE
    return ::syscall(
        __NR_futex, __addr, FUTEX_WAIT_PRIVATE, __x, nullptr, nullptr, 0);
}
[[__gnu__::__always_inline__, __gnu__::__artificial__]] //
inline auto _S_syscall_futex_wake_private(void *__addr, const int __x) noexcept
    -> long {
    // NOLINTNEXTLINE
    return ::syscall(
        __NR_futex, __addr, FUTEX_WAKE_PRIVATE, __x, nullptr, nullptr, 0);
}

struct __task_base {
    using __execute_fn [[__gnu__::__nodebug__]] =
        void (*)(__task_base *, std::uint32_t __tid) noexcept;
    __task_base       *__next_    = nullptr;
    const __execute_fn __execute_ = nullptr;

    explicit __task_base(__execute_fn __fn) noexcept : __execute_(__fn) {}
};

struct __remote_queue {
    explicit __remote_queue() noexcept = default;
    explicit __remote_queue(__remote_queue *__next) noexcept
        : __next_(__next) {}

    exec::__atomic_intrusive_queue<&__task_base::__next_>
                          __queues_[_Gr_max_n_concurrency]{};
    __remote_queue       *__next_ = nullptr;
    const std::thread::id __id_   = std::this_thread::get_id();
    std::uint32_t __index_        = (std::numeric_limits<std::uint32_t>::max)();
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic<std::uint64_t> __rcu_seq_{0U};
};

struct __remote_queue_sink { // NOLINT
    std::atomic<__remote_queue *> __head_;
    __remote_queue               *__tail_;
    __remote_queue                __this_{};

    explicit __remote_queue_sink() noexcept
        : __head_{&__this_}, __tail_{&__this_} {}

    ~__remote_queue_sink() noexcept {
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
            // NOLINTNEXTLINE
            __task.append(__head->__queues_[__tid].pop_all_reversed());
            __head = __head->__next_;
        }
        return __task;
    }

    [[__nodiscard__]] auto _M_get() -> __remote_queue * {
        thread_local std::thread::id _S_tid = std::this_thread::get_id();
        __remote_queue *__head  = __head_.load(std::memory_order_acquire);
        __remote_queue *__queue = __head;
        while (__queue != __tail_) {
            if (__queue->__id_ == _S_tid) { return __queue; }
            __queue = __queue->__next_;
        }
        auto *__new_head = new __remote_queue{__head};
        while (!__head_.compare_exchange_weak(
            __head, __new_head, std::memory_order_acq_rel)) {
            __new_head->__next_ = __head;
        }
        return __new_head;
    }
};

struct __parschd;
struct __task_receiver {
    struct __pop_result {
        __task_base  *__task_;
        std::uint32_t __src_ = (std::numeric_limits<std::uint32_t>::max)();
    };

    explicit __task_receiver(
        __parschd *__schd, const std::uint32_t __i) noexcept
        : __pool_(__schd), __index_(__i) {
        std::random_device __rd;
        __rng_.seed(__rd);
    }

    enum class __exec_stage : unsigned char { _S_run, _S_sleep, _S_suspend };
    enum class __message : unsigned char {
        _S_none,
        _S_retry,
        _S_retire,
        _S_resume,
        _S_stop
    };
    struct __state_mask {
#if __BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__
    #error "__BYTE_ORDER__ != __ORDER_LITTLE_ENDIAN__"
#endif
        __attribute__((__preferred_type__(__exec_stage))) __exec_stage __stage_
            : 8 = __exec_stage::_S_run;
        __attribute__((__preferred_type__(__exec_stage))) __message __message_
            : 8 = __message::_S_none;
        friend auto operator==(__state_mask, __state_mask) noexcept
            -> bool = default;
    } __attribute__((__trivial_abi__));

    [[__gnu__::__always_inline__, __gnu__::__artificial__, __nodiscard__]]
    inline static constexpr auto _S_rank(const __message __n) noexcept
        -> unsigned {
        constexpr unsigned __r[]{0U, 1U, 2U, 2U, 3U};
        return __r[static_cast<unsigned char>(__n)]; // NOLINT
    }

    struct __trap_notify_result {
        bool         __delivered_;
        __exec_stage __stage_;
    } __attribute__((__trivial_abi__));

    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic<__state_mask> __mask_{};

    exec::bwos::lifo_queue<__task_base *, std::allocator<__task_base *>>
                                                      __local_{32, 8};
    STDEXEC::__intrusive_queue<&__task_base::__next_> __pending_;

    mutable std::mutex              __mut_;
    mutable std::condition_variable __cv_;
    __parschd                      *__pool_;
    exec::xorshift                  __rng_;
    const std::uint32_t             __index_;

    auto _M_trap_notify(const __message __x) noexcept -> __trap_notify_result {
        const unsigned __rank = _S_rank(__x);
        __state_mask   __old  = __mask_.load(std::memory_order_relaxed);
        for (;;) {
            if (__old.__message_ == __x || _S_rank(__old.__message_) > __rank) {
                return {false, __old.__stage_};
            }
            if (__mask_.compare_exchange_weak(
                    __old, {__old.__stage_, __x}, std::memory_order_relaxed))
                [[__likely__]] {
                return {true, __old.__stage_};
            }
        }
    }

    [[__gnu__::__always_inline__, __gnu__::__artificial__]]
    inline void _M_wake_private() noexcept {
        { std::lock_guard __l{__mut_}; }
        __cv_.notify_one();
    }

    void _M_consume(const __message __x) noexcept {
        __state_mask __old = __mask_.load(std::memory_order_relaxed);
        while (__old.__message_ == __x) {
            if (__mask_.compare_exchange_weak(
                    __old, {__old.__stage_, __message::_S_none},
                    std::memory_order_relaxed)) {
                return;
            }
        }
    }

    void _M_transfer_to(const __exec_stage __x) noexcept {
        __state_mask __old = __mask_.load(std::memory_order_relaxed);
        while (__old.__stage_ != __x) {
            if (__mask_.compare_exchange_weak(
                    __old, {__x, __old.__message_},
                    std::memory_order_relaxed)) {
                return;
            }
        }
    }
    [[__gnu__::__always_inline__, __gnu__::__artificial__, __nodiscard__]]
    inline auto _M_stop_requested() const noexcept -> bool {
        return __mask_.load(std::memory_order_relaxed).__message_ ==
               __message::_S_stop;
    }

    void _M_notify_retry() noexcept {
        const auto [__ok, __y] = _M_trap_notify(__message::_S_retry);
        if (__ok && __y == __exec_stage::_S_sleep) { _M_wake_private(); }
    }

    [[__nodiscard__]] auto _M_try_notify_retry_sleep() noexcept -> bool {
        __state_mask __exp{__exec_stage::_S_sleep, __message::_S_none};
        if (!__mask_.compare_exchange_strong(
                __exp, {__exec_stage::_S_sleep, __message::_S_retry},
                std::memory_order_relaxed)) {
            return false;
        }
        _M_wake_private();
        return true;
    }

    void _M_notify_stop() noexcept {
        const auto [__ok, __y] = _M_trap_notify(__message::_S_stop);
        if (__ok && (__y == __exec_stage::_S_sleep ||
                     __y == __exec_stage::_S_suspend)) {
            _M_wake_private();
        }
    }

    void _M_notify_retire() noexcept {
        const auto [__ok, __y] = _M_trap_notify(__message::_S_retire);
        if (__ok && __y == __exec_stage::_S_sleep) { _M_wake_private(); }
    }

    void _M_notify_retire_sync() noexcept {
        _M_notify_retire();
        while (__mask_.load(std::memory_order_relaxed).__stage_ !=
               __exec_stage::_S_suspend) {
#if defined(__x86_64__) || defined(__i386__)
            __builtin_ia32_pause();
#else
            std::this_thread::yield();
#endif
        }
        { std::lock_guard __lock{__mut_}; }
    }

    void _M_notify_resume() noexcept {
        const auto [__ok, __y] = _M_trap_notify(__message::_S_resume);
        if (__ok && __y == __exec_stage::_S_suspend) { _M_wake_private(); }
    }

    void _M_notify_resume_sync() noexcept {
        _M_notify_resume();
        { std::lock_guard __lock{__mut_}; }
        while (__mask_.load(std::memory_order_relaxed).__stage_ !=
               __exec_stage::_S_run) {
#if defined(__x86_64__) || defined(__i386__)
            __builtin_ia32_pause();
#else
            std::this_thread::yield();
#endif
        }
    }

    void _M_push_local(__task_base *__task) noexcept {
        if (!__local_.push_back(__task)) { __pending_.push_back(__task); }
    }
    void _M_push_local(
        STDEXEC::__intrusive_queue<&__task_base::__next_> &&__tasks) noexcept {
        __pending_.prepend(std::move(__tasks));
    }

    [[__nodiscard__]] auto _M_try_pop() -> __pop_result {
        __pop_result __result{.__task_ = nullptr, .__src_ = __index_};
        __result.__task_ = __local_.pop_back();
        if (__result.__task_ != nullptr) [[__likely__]] { return __result; }
        return _M_try_remote();
    }
    [[__nodiscard__]] auto _M_try_remote() -> __pop_result;
    [[__nodiscard__]] auto _M_try_steal() -> __pop_result;

    [[__nodiscard__]] auto _M_pop() noexcept -> __pop_result;
    void                   _M_retire_and_resume() noexcept;

    void _M_notify_one_sleep() noexcept;

    [[__gnu__::__always_inline__, __gnu__::__artificial__]] //
    inline void _M_set_steal() const noexcept;
    [[__gnu__::__always_inline__, __gnu__::__artificial__]] //
    inline void _M_clear_steal() noexcept;
    [[__gnu__::__always_inline__, __gnu__::__artificial__]] //
    inline void _M_set_sleep() const noexcept;
    [[__gnu__::__always_inline__, __gnu__::__artificial__]] //
    inline void _M_clear_sleep() noexcept;
};

struct __parschd { // NOLINT
    [[__nodiscard__]] static auto _S_hardware_concurrency() noexcept
        -> unsigned int {
        const unsigned __n = std::thread::hardware_concurrency();
        return __n == 0 ? 1 : __n;
    }

    inline static std::atomic_uint64_t _S_n{0U};

    alignas(__GCC_DESTRUCTIVE_SIZE) const std::uint64_t __nid_ =
        _S_n.fetch_add(1U, std::memory_order_relaxed);
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic<std::uint32_t> __active_{};
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic<std::uint64_t> __epoch_{1U};
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic_int __wait_{0};
    alignas(__GCC_DESTRUCTIVE_SIZE) __remote_queue_sink __remote_;

    std::uint32_t                                            __n_thread_;
    std::vector<std::thread>                                 __thread_;
    std::vector<STDEXEC::__manual_lifetime<__task_receiver>> __receiver_;
    alignas(__GCC_DESTRUCTIVE_SIZE)
        std::vector<std::atomic<std::uint32_t>> __access_;
    std::vector<std::uint32_t> __offset_;
    alignas(__GCC_DESTRUCTIVE_SIZE) std::atomic<std::uint32_t> __size_;
    mutable std::mutex __modify_mut_;

    [[__gnu__::__always_inline__, __gnu__::__artificial__]] //
    static inline auto _S_remote_queue_vec() noexcept
        -> __vector<__remote_queue *, _GR_max_n_parschd_obj> & {
        thread_local __vector<__remote_queue *, _GR_max_n_parschd_obj> _S_vec;
        return _S_vec;
    }

    void _M_construct_remote_queue(const std::uint32_t __index) noexcept {
        _M_get_remote_queue()->__index_ = __index;
    }

    [[__nodiscard__]] auto _M_get_remote_queue() noexcept -> __remote_queue * {
        auto &__vec = _S_remote_queue_vec();
        if (__nid_ < __vec.__size_) [[__likely__]] {
            return __vec.__data_[__nid_]; // NOLINT
        }
        return __vec._M_resize_emplace(__nid_, __remote_._M_get());
    }

    void _M_rcu_lock(__remote_queue &__rq) noexcept {
        __rq.__rcu_seq_.store(
            __epoch_.load(std::memory_order_acquire),
            std::memory_order_relaxed);
        __asm__ __volatile__("" ::: "memory"); // NOLINT
    }
    void _M_rcu_unlock(__remote_queue &__rq) noexcept {
        __rq.__rcu_seq_.store(0, std::memory_order_relaxed);
        _TSAN_RELEASE(&__rq.__rcu_seq_);
        __asm__ __volatile__("" ::: "memory"); // NOLINT
        if ((bool)__wait_.load(std::memory_order_acquire)) {
            __wait_.store(0, std::memory_order_release);
            _S_syscall_futex_wake_private(&__wait_, 0);
        }
    }
    [[__nodiscard__]] auto
    _M_rcu_can_progress(const std::uint64_t __ep) noexcept -> bool {
        _S_syscall_memory_barrier(MEMBARRIER_CMD_GLOBAL);
        for (__remote_queue *__head =
                 __remote_.__head_.load(std::memory_order_acquire);
             __head != nullptr; __head = __head->__next_) {
            _TSAN_ACQUIRE(&__head->__rcu_seq_);
            const auto __tmp =
                __head->__rcu_seq_.load(std::memory_order_relaxed);
            if (__tmp != 0 && __tmp < __ep) { return false; }
        }
        return true;
    }
    void _M_rcu_synchronize() noexcept {
        const std::uint64_t __ep =
            __epoch_.fetch_add(1U, std::memory_order_acq_rel) + 1U;
        if (_M_rcu_can_progress(__ep)) { return; }
        while (true) {
            __wait_.store(1, std::memory_order_release);
            _S_syscall_memory_barrier(MEMBARRIER_CMD_GLOBAL);
            if (_M_rcu_can_progress(__ep)) [[__unlikely__]] { break; }
            _S_syscall_futex_wait_private(&__wait_, 1);
        }
    }

    void _M_request_stop() noexcept {
        for (auto &__rcvr : __receiver_) { __rcvr->_M_notify_stop(); }
    }
    // void _M_request_resume() noexcept {
    //     for (auto &__q : __queue_) { __q->_M_request_resume(); }
    // }
    void _M_join() noexcept {
        for (std::thread &__t : __thread_) { __t.join(); }
    }
    auto _M_enqueue(__task_base *__task) noexcept -> bool {
        thread_local __remote_queue *__queue = _M_get_remote_queue();
        const std::uint32_t          __idx   = __queue->__index_;
        if (__idx < __receiver_.size()) {
            __receiver_[__idx]->_M_push_local(__task);
            return true;
        }

        thread_local std::uint64_t __start =
            std::uint64_t(std::random_device{}());
        ++__start;
        _M_rcu_lock(*__queue);
        _TSAN_ACQUIRE(&__size_);
        const std::uint32_t __k = __size_.load(std::memory_order_relaxed);
        if (__k == 0) [[__unlikely__]] {
            _M_rcu_unlock(*__queue);
            return false;
        }
        __asm__ __volatile__("" ::: "memory");           // NOLINT
        const std::uint32_t __target =
            __access_[__start % __k].load(std::memory_order_relaxed);
        __queue->__queues_[__target].push_front(__task); // NOLINT
        __receiver_[__target]->_M_notify_retry();
        _M_rcu_unlock(*__queue);
        return true;
    }
    inline void _M_run(const std::uint32_t __i) noexcept {
        while (true) {
            auto [__task, __src] = __receiver_[__i]->_M_pop();
            if (__task == nullptr) [[__unlikely__]] { return; }
            (*__task->__execute_)(__task, __src);
        }
    }
    explicit __parschd(
        const std::uint32_t __n = _S_hardware_concurrency()) noexcept // NOLINT
        : __n_thread_(__n), __receiver_(__n), __access_(__n), __offset_(__n),
          __size_(__n) {
        const long __r = _S_syscall_memory_barrier(MEMBARRIER_CMD_QUERY);
        // NOLINTNEXTLINE
        assert(__r > 0 && (__r & MEMBARRIER_CMD_GLOBAL));
        for (std::uint32_t __i{}; __i < __n; ++__i) {
            __receiver_[__i].__construct(this, __i);
        }
#pragma clang loop vectorize(enable) interleave(enable)
        for (std::uint32_t __i{}; __i < __n; ++__i) {
            __access_[__i].store(__i, std::memory_order_relaxed);
            __offset_[__i] = __i;
        }

        __thread_.reserve(__n);
        __active_.store(__n << 16U, std::memory_order_relaxed);
        for (std::uint32_t __index{}; __index < __n; ++__index) {
            __thread_.emplace_back([this, __index] noexcept -> void {
                _M_construct_remote_queue(__index);
                _M_run(__index);
            });
        }
    }
    ~__parschd() noexcept {
        _M_request_stop();
        _M_join();
        for (std::uint32_t __i{}; __i < __n_thread_; ++__i) {
            __receiver_[__i].__destroy();
        }
    }
};

inline void _S_move_pending_to_local(
    STDEXEC::__intrusive_queue<&__task_base::__next_> &__pending,
    exec::bwos::lifo_queue<__task_base *, std::allocator<__task_base *>>
        &__local) {
    const auto __last = __local.push_back(__pending.begin(), __pending.end());
    STDEXEC::__intrusive_queue<&__task_base::__next_> __tmp{};
    __tmp.splice(__tmp.begin(), __pending, __pending.begin(), __last);
    __tmp.clear();
}

[[__nodiscard__]] inline auto __task_receiver::_M_try_remote() -> __pop_result {
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
[[__nodiscard__]] inline auto __task_receiver::_M_try_steal() -> __pop_result {
    __remote_queue *__queue = __pool_->_M_get_remote_queue();
    __pool_->_M_rcu_lock(*__queue);
    _TSAN_ACQUIRE(&__pool_->__size_);
    const std::uint32_t __k = __pool_->__size_.load(std::memory_order_relaxed);
    if (__k < 2) [[__unlikely__]] {
        __pool_->_M_rcu_unlock(*__queue);
        return {.__task_ = nullptr};
    }
    std::uniform_int_distribution<std::uint32_t> __d(0, __k - 1);
    for (unsigned __cnt{}; __cnt < __k - 1;) {
        const std::uint32_t __idx =
            __pool_->__access_[__d(__rng_)].load(std::memory_order_relaxed);
        if (__idx == __index_) [[__unlikely__]] { continue; }
        if (__task_base *__task =
                __pool_->__receiver_[__idx]->__local_.steal_front()) {
            __pool_->_M_rcu_unlock(*__queue);
            return {.__task_ = __task, .__src_ = __idx};
        }
        ++__cnt;
    }
    __pool_->_M_rcu_unlock(*__queue);
    return {.__task_ = nullptr};
}
inline void __task_receiver::_M_notify_one_sleep() noexcept {
    __remote_queue *__queue = __pool_->_M_get_remote_queue();
    __pool_->_M_rcu_lock(*__queue);
    _TSAN_ACQUIRE(&__pool_->__size_);
    const std::uint32_t __k = __pool_->__size_.load(std::memory_order_relaxed);
    if (__k < 2) [[__unlikely__]] {
        __pool_->_M_rcu_unlock(*__queue);
        return;
    }

    std::uniform_int_distribution<std::uint32_t> __d(0, __k - 1);
    const std::uint32_t                          __begin = __d(__rng_);
    for (std::uint32_t __i{}; __i < __k; ++__i) {
        const std::uint32_t __idx =
            __pool_->__access_[(__begin + __i) % __k].load(
                std::memory_order_relaxed);
        if (__idx == __index_) [[__unlikely__]] { continue; }
        if (__pool_->__receiver_[__idx]->_M_try_notify_retry_sleep()) {
            __pool_->_M_rcu_unlock(*__queue);
            return;
        }
    }
    __pool_->_M_rcu_unlock(*__queue);
}
inline void __task_receiver::_M_set_steal() const noexcept {
    std::uint32_t const __n = 1U - (1U << 16U);
    __pool_->__active_.fetch_add(__n, std::memory_order_relaxed);
}
inline void __task_receiver::_M_clear_steal() noexcept {
    constexpr std::uint32_t diff = 1 - (1U << 16U);
    const std::uint32_t     __active =
        __pool_->__active_.fetch_sub(diff, std::memory_order_relaxed);
    const std::uint32_t __n_victim = __active >> 16U;
    const std::uint32_t __n_thief  = __active & 0xffffU;
    if (__n_thief == 1 && __n_victim != 0) { _M_notify_one_sleep(); }
}
inline void __task_receiver::_M_set_sleep() const noexcept {
    __pool_->__active_.fetch_sub(1U << 16U, std::memory_order_relaxed);
}
inline void __task_receiver::_M_clear_sleep() noexcept {
    const std::uint32_t __active =
        __pool_->__active_.fetch_add(1U << 16U, std::memory_order_relaxed);
    if (__active == 0) { _M_notify_one_sleep(); }
}

[[__nodiscard__]] inline auto __task_receiver::_M_pop() noexcept
    -> __pop_result {
    __pop_result __result = _M_try_pop();
    while (__result.__task_ == nullptr) {
        _M_set_steal();
        __result = _M_try_steal();
        if (__result.__task_ != nullptr) {
            _M_clear_steal();
            return __result;
        }
#if defined(__x86_64__) || defined(__i386__)
        __builtin_ia32_pause();
#else
        std::this_thread::yield();
#endif
        _M_clear_steal();

        const __state_mask __x = __mask_.load(std::memory_order_relaxed);
        switch (static_cast<__message>(__x.__message_)) {
        case __message::_S_stop: return __result;
        case __message::_S_retire:
            _M_retire_and_resume();
            if (_M_stop_requested()) {
                return {.__task_ = nullptr, .__src_ = __index_};
            }
            _M_transfer_to(__exec_stage::_S_run);
            __result = _M_try_pop();
            continue;
        case __message::_S_retry:
        case __message::_S_resume:
            _M_consume(__x.__message_);
            __result = _M_try_pop();
            continue;
        case __message::_S_none: break;
        }

        std::unique_lock __lock{__mut_};
        __state_mask     __exp{__exec_stage::_S_run, __message::_S_none};
        if (__mask_.compare_exchange_strong(
                __exp, {__exec_stage::_S_sleep, __message::_S_none},
                std::memory_order_relaxed)) {
            __result = _M_try_remote();
            if (__result.__task_ != nullptr) [[__unlikely__]] {
                __lock.unlock();
                _M_transfer_to(__exec_stage::_S_run);
                return __result;
            }
            _M_set_sleep();
            __cv_.wait(__lock, [this] noexcept -> bool {
                return __mask_.load(std::memory_order_relaxed).__message_ !=
                       __message::_S_none;
            });
            __lock.unlock();
            _M_clear_sleep();
        }
        if (__lock.owns_lock()) { __lock.unlock(); }
        _M_transfer_to(__exec_stage::_S_run);
        __result = _M_try_pop();
    }
    return __result;
}

inline void __task_receiver::_M_retire_and_resume() noexcept {
    {
        std::lock_guard __lock{__pool_->__modify_mut_};

        const std::uint32_t __j = __pool_->__offset_[__index_];
        const std::uint32_t __last =
            __pool_->__size_.load(std::memory_order_relaxed) - 1U;
        const std::uint32_t __w =
            __pool_->__access_[__last].load(std::memory_order_relaxed);

        __pool_->__access_[__j].store(__w, std::memory_order_relaxed);
        __pool_->__offset_[__w] = __j;

        __pool_->__access_[__last].store(__index_, std::memory_order_relaxed);
        __pool_->__offset_[__index_] = __last;

        __pool_->__size_.store(__last, std::memory_order_relaxed);
        _TSAN_RELEASE(&__pool_->__size_);
    }
    __pool_->_M_rcu_synchronize();

    STDEXEC::__intrusive_queue<&__task_base::__next_> __tmp{};
#pragma clang loop vectorize(enable) interleave(enable)
    for (__task_base *__t = __local_.pop_back(); __t != nullptr;
         __t              = __local_.pop_back()) {
        __tmp.push_back(__t);
    }
    __tmp.prepend(__pool_->__remote_._M_pop_all_reversed(__index_));
    __tmp.prepend(std::move(__pending_));

    if (!__tmp.empty()) {
        __remote_queue *__queue = __pool_->_M_get_remote_queue();

        __pool_->_M_rcu_lock(*__queue);
        _TSAN_ACQUIRE(&__pool_->__size_);
        const std::uint32_t __k =
            __pool_->__size_.load(std::memory_order_relaxed);
        if (__k == 0) [[__unlikely__]] {
            __pool_->_M_rcu_unlock(*__queue);
            __pending_.prepend(std::move(__tmp));
        } else {
            std::uniform_int_distribution<std::uint32_t> __d(0, __k - 1);
            const std::uint32_t                          __idx =
                __pool_->__access_[__d(__rng_)].load(std::memory_order_relaxed);
            __queue->__queues_[__idx].prepend(std::move(__tmp)); // NOLINT
            __pool_->__receiver_[__idx]->_M_notify_retry();
            __pool_->_M_rcu_unlock(*__queue);
        }
    }

    std::unique_lock __lock{__mut_};
    _M_transfer_to(__exec_stage::_S_suspend);
    _M_set_sleep();
    __cv_.wait(__lock, [this] noexcept -> bool {
        const __message __n =
            __mask_.load(std::memory_order_relaxed).__message_;
        return __n == __message::_S_resume || __n == __message::_S_stop;
    });
    __lock.unlock();
    _M_clear_sleep();
    _M_consume(__message::_S_resume);

    if (_M_stop_requested()) { return; }
    {
        std::lock_guard     __lock{__pool_->__modify_mut_};
        const std::uint32_t __k =
            __pool_->__size_.load(std::memory_order_relaxed);

        __pool_->__access_[__k].store(__index_, std::memory_order_relaxed);
        __pool_->__offset_[__index_] = __k;

        __pool_->__size_.store(__k + 1U, std::memory_order_relaxed);
        _TSAN_RELEASE(&__pool_->__size_);
        _S_syscall_memory_barrier(MEMBARRIER_CMD_GLOBAL);
    }
}
} // namespace __gr::__resumable__::inline __v1
