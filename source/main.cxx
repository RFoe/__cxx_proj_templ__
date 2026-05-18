#include <__pool/__v3/__park/__parschd.hpp>

#include <atomic>
#include <chrono>
#include <print>
#include <thread>

using namespace __pool::_park_; // NOLINT

namespace {

template <typename _Fn, typename _Ty = std::remove_cvref_t<_Fn>>
inline auto _S_task(_Fn &&__func) -> __task_base * {
    struct __task : public __task_base {
        static void
        _S_execute(__task_base *__self, std::uint32_t /*__tid*/) noexcept {
            (static_cast<__task *>(__self)->__func_)();
            delete static_cast<__task *>(__self);
        }
        explicit __task(_Fn &&__fn) noexcept // NOLINT
            : __task_base(&_S_execute), __func_(std::forward<_Fn>(__fn)) {}
        _Ty __func_;
    };
    return new (std::nothrow) __task{std::forward<_Fn>(__func)};
}

} // namespace

using std::chrono::operator""s;
using __clock = std::chrono::steady_clock;

auto main() -> int {
    constexpr unsigned __share = 16;
    const unsigned     __n     = __parschd::_S_hardware_concurrency() * __share;

    __parschd __schd{__n};

    std::atomic_uint32_t __remaining{0};

    auto __enqueue_batch = [&](unsigned __count) noexcept -> void {
        __remaining.store(__count, std::memory_order_relaxed);
        for (unsigned __i{}; __i < __count; ++__i) {
            __schd._M_enqueue(_S_task([&__remaining]() noexcept -> void {
                // std::this_thread::sleep_for(1s);
                if (__remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                    __remaining.notify_all();
                }
            }));
        }
    };
    auto __wait_batch = [&]() noexcept -> void {
        for (unsigned __v  = __remaining.load(std::memory_order_acquire);
             __v != 0; __v = __remaining.load(std::memory_order_acquire)) {
            __remaining.wait(__v, std::memory_order_acquire);
        }
    };

    // ---- 等可调度线程数稳定 ------------------------------------------
    // __size_ 是可见集大小,正好等于当前能接活的线程数:
    // retire 把自己移出可见集(size--),resume 重新加入(size++)。
    // _M_request_retire/resume 是异步信号,enqueue 前必须等 __size_
    // 收敛到位,否则任务会落到尚未真正 retire 的线程上,计时失真。
    // __size_ 上没有 notify,只能轮询(测试夹具,可接受)。
    auto __wait_size = [&](std::uint32_t __want) noexcept -> void {
        while (__schd.__size_.load(std::memory_order_acquire) < __want) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    };

    auto __phase = [&](const char *__name, std::uint32_t __threads) -> void {
        __wait_size(__threads);
        std::println(
            "--- {}: {} thread(s), {} tasks @1s ---", __name, __threads, __n);
        const auto __t0 = __clock::now();
        __enqueue_batch(__n);
        __wait_batch();
        const auto __dt =
            std::chrono::duration<double>(__clock::now() - __t0).count();
        std::println("--- {} done in {:.4f}s ---\n", __name, __dt);
    };

    for (std::size_t __i{}; __i < __n; ++__i) {
        __schd.__state_[__i]->_M_request_retire();
    }
    std::size_t __i{};
    for (std::size_t __j{}; __j < __share; ++__j) {
        __schd.__state_[__i + __j]->_M_request_resume();
    }
    __i += __share;
    __phase("phase 1", __i); // 16 tasks / 1 thread  -> ~16.00s
    for (std::size_t __j{}; __j < __share; ++__j) {
        __schd.__state_[__i + __j]->_M_request_resume();
    }
    __i += __share;
    __phase("phase 2", __i); // 16 / 2               -> ~ 8.00s
    for (std::size_t __j{}; __j < __share; ++__j) {
        __schd.__state_[__i + __j]->_M_request_resume();
    }
    __i += __share;
    __phase("phase 3", __i); // ceil(16/3) waves     -> ~ 6.00s
    for (std::size_t __j{}; __j < __share; ++__j) {
        __schd.__state_[__i + __j]->_M_request_resume();
    }
    __i += __share;
    __phase("phase 4", __i); // 16 / 4               -> ~ 4.00s
}
