#pragma once

#include <__algorithm/min.h>
#include <__bit/bit_width.h>
#include <atomic>

namespace __gr::__core__::inline __v1 {

template <typename _Ty, std::size_t __max_size_>
struct __atomic_seg_array { // NOLINT
    using __atomic                         = std::atomic<_Ty>;
    static constexpr unsigned _S_n_segment = std::bit_width(__max_size_);

    std::atomic<__atomic *> __segment_[_S_n_segment]{};
    std::atomic_size_t      __size_{0U};

    ~__atomic_seg_array() noexcept {
        for (auto &__p : __segment_) {
            delete[] __p.load(std::memory_order_acquire);
        }
    }

    struct __locate_result {
        unsigned __segment_;
        unsigned __index_;
    };

    [[__gnu__::__always_inline__, __gnu__::__artificial__]] //
    inline static constexpr auto _S_locate(const unsigned __i) noexcept
        -> __locate_result {
        const unsigned __seg = 63U - __builtin_clzll(__i + 1U);
        return {__seg, __i - ((1U << __seg) - 1U)};
    }

    [[__nodiscard__]] auto _M_segment(const unsigned __seg) noexcept
        -> __atomic * {
        __atomic *__p =
            __segment_[__seg].load(std::memory_order_acquire); // NOLINT
        if (__p != nullptr) [[__likely__]] { return __p; }
        auto *__new = new __atomic[1U << __seg]{};
        // NOLINTNEXTLINE
        if (__segment_[__seg].compare_exchange_strong(
                __p, __new, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            return __new;
        }
        delete[] __new;
        return __p;
    }

    auto _M_push_back(_Ty __v) noexcept -> std::size_t {
        const std::size_t __i =
            __size_.fetch_add(1U, std::memory_order_relaxed);
        const auto [__seg, __idx] = _S_locate(__i);
        __atomic *__p             = _M_segment(__seg);
        __p[__idx].store(__v, std::memory_order_release);
        return __i;
    }

    [[__nodiscard__]] auto _M_size() const noexcept -> std::size_t {
        return __size_.load(std::memory_order_acquire);
    }

    template <typename _Fn> void _M_ro_iter(_Fn &&__fn) const noexcept {
        const std::size_t __n   = _M_size();
        unsigned          __it  = 0;
        unsigned          __seg = 0;
        while (__it < __n) {
            // NOLINTNEXTLINE
            __atomic *__p = __segment_[__seg].load(std::memory_order_acquire);
            const std::size_t __m = 1U << __seg;
            const std::size_t __u = std::min(__m, __n - __it);
            for (std::size_t __idx{}; __idx < __u; ++__idx) {
                // NOLINTNEXTLINE
                const _Ty __v = __p[__idx].load(std::memory_order_acquire);
                if (__v == _Ty{}) [[__unlikely__]] { continue; }
                if (__fn(__v)) [[__unlikely__]] { return; }
            }
            __it += __u;
            ++__seg;
        }
    }
};

} // namespace __gr::__core__::inline __v1
