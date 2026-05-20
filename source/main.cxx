#include <__gr/__v4/__resumable/__parschd.hpp>

#include <atomic>
#include <chrono>
#include <latch>
#include <print>
#include <thread>

using namespace __gr::__resumable__; // NOLINT

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
using std::chrono::operator""ms;
using __clock = std::chrono::steady_clock;

namespace {

struct __stress_ctx {
    __parschd                 *__sched_;
    std::atomic<std::uint64_t> __pending_{0};   // outstanding tasks
    std::atomic<std::uint64_t> __submitted_{0}; // total submissions
    std::atomic<std::uint64_t> __executed_{0};  // total completions
    std::atomic<std::uint64_t> __spawn_failed_{
        0};                                     // enqueue rejected, ran inline
    std::atomic<bool> __stop_producers_{false};
};

inline void _S_submit(__stress_ctx &__ctx, __task_base *__t) noexcept {
    __ctx.__pending_.fetch_add(1, std::memory_order_relaxed);
    __ctx.__submitted_.fetch_add(1, std::memory_order_relaxed);
    if (!__ctx.__sched_->_M_enqueue(__t)) [[__unlikely__]] {
        // All workers retired right now; no one can accept. Run in-place;
        // the task body still decrements __pending_.
        __ctx.__spawn_failed_.fetch_add(1, std::memory_order_relaxed);
        (*__t->__execute_)(__t, 0);
    }
}

inline auto
_S_make_fork_task(__stress_ctx &__ctx, int __depth, std::uint64_t __seed)
    -> __task_base *;

inline void _S_run_fork_task(
    __stress_ctx &__ctx, int __depth, std::uint64_t __seed) noexcept {
    std::mt19937 __rng(__seed);

    // Synthetic work — a small integer mix, kept from being optimised away.
    std::uint64_t       __sink  = __seed;
    const std::uint32_t __nspin = static_cast<std::uint32_t>(__rng() & 0x7F);
    for (std::uint32_t __i = 0; __i < __nspin; ++__i) {
        __sink = __sink * 6364136223846793005ULL + 1442695040888963407ULL;
    }
    asm volatile("" : "+r"(__sink)); // NOLINT

    // Maybe spawn 0..3 children. Stop spawning once producers have signalled
    // shutdown so the drain phase actually converges.
    if (__depth > 0 &&
        !__ctx.__stop_producers_.load(std::memory_order_relaxed)) {
        const std::uint32_t __nchildren =
            static_cast<std::uint32_t>(__rng() & 0x3);
        for (std::uint32_t __i = 0; __i < __nchildren; ++__i) {
            _S_submit(
                __ctx,
                _S_make_fork_task(
                    __ctx, __depth - 1, static_cast<std::uint64_t>(__rng())));
        }
    }

    __ctx.__executed_.fetch_add(1, std::memory_order_relaxed);
    __ctx.__pending_.fetch_sub(1, std::memory_order_release);
}

inline auto
_S_make_fork_task(__stress_ctx &__ctx, int __depth, std::uint64_t __seed)
    -> __task_base * {
    return _S_task([&__ctx, __depth, __seed]() noexcept {
        _S_run_fork_task(__ctx, __depth, __seed);
    });
}

} // namespace

int main(int __argc, char *__argv[]) {
    const std::uint64_t __seed =
        (__argc >= 2) ? std::strtoull(__argv[1], nullptr, 10)
                      : static_cast<std::uint64_t>(std::random_device{}());
    const auto __duration =
        (__argc >= 3) ? std::chrono::seconds(std::atoi(__argv[2])) : 10s;
    const unsigned __nthreads =
        (__argc >= 4) ? static_cast<unsigned>(std::atoi(__argv[3]))
                      : std::max(2U, std::thread::hardware_concurrency());
    constexpr unsigned __nproducers = 6;

    std::fprintf(
        stderr,
        "parschd stress: seed=%llu nthreads=%u nproducers=%u "
        "duration=%llds\n",
        static_cast<unsigned long long>(__seed), __nthreads, __nproducers,
        static_cast<long long>(__duration.count()));
    std::fflush(stderr);

    __parschd    __sched{__nthreads};
    __stress_ctx __ctx;
    __ctx.__sched_ = &__sched;

    std::mt19937_64 __master{__seed};

    // ----- Producer threads -------------------------------------------------
    std::vector<std::thread> __producers;
    __producers.reserve(__nproducers);
    for (unsigned __pi = 0; __pi < __nproducers; ++__pi) {
        const std::uint64_t __pseed = __master();
        __producers.emplace_back([&__ctx, __pseed]() noexcept {
            std::mt19937_64 __rng{__pseed};
            while (!__ctx.__stop_producers_.load(std::memory_order_relaxed)) {
                const std::uint32_t __burst =
                    1U + static_cast<std::uint32_t>(__rng() & 0x1F);
                for (std::uint32_t __i = 0; __i < __burst; ++__i) {
                    const int __depth = static_cast<int>(__rng() & 0x3);
                    _S_submit(
                        __ctx, _S_make_fork_task(
                                   __ctx, __depth,
                                   static_cast<std::uint64_t>(__rng())));
                }
                if ((__rng() & 0xFF) == 0) {
                    std::this_thread::sleep_for(
                        std::chrono::microseconds(
                            static_cast<int>(__rng() & 0x1FF)));
                }
            }
        });
    }

    // ----- Controller: random retire / resume churn -------------------------
    const std::uint64_t __cseed = __master();
    std::thread         __controller([&__ctx, &__sched, __nthreads,
                                      __cseed]() noexcept {
        std::mt19937_64 __rng{__cseed};
        while (!__ctx.__stop_producers_.load(std::memory_order_relaxed)) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(1 + static_cast<int>(__rng() % 25U)));
            const std::uint32_t __action =
                static_cast<std::uint32_t>(__rng() & 0x7);
            const std::uint32_t __n =
                1U +
                static_cast<std::uint32_t>(__rng() % std::max(1U, __nthreads));
            switch (__action) {
            case 0: __sched._M_request_retire_n(__n); break;
            case 1: __sched._M_request_resume_n(__n); break;
            case 2: __sched._M_request_retire_n_sync(__n); break;
            case 3: __sched._M_request_resume_n_sync(__n); break;
            case 4: // tight retire-then-resume churn
                __sched._M_request_retire_n(__n);
                __sched._M_request_resume_n(__n);
                break;
            case 5: // retire absolutely everyone
                __sched._M_request_retire_n_sync(__nthreads);
                break;
            case 6: // resume absolutely everyone
                __sched._M_request_resume_n_sync(__nthreads);
                break;
            case 7: // back-to-back retire bursts, no resume in between
                __sched._M_request_retire_n(__n);
                std::this_thread::sleep_for(std::chrono::microseconds(50));
                __sched._M_request_retire_n(__n);
                break;
            }
        }
        // Wake everyone currently retired so the drain phase can make progress.
        __sched._M_request_resume_n_sync(__nthreads);
    });

    // ----- Run for __duration with periodic progress ------------------------
    const auto __t0 = __clock::now();
    while (__clock::now() - __t0 < __duration) {
        std::this_thread::sleep_for(500ms);
        const auto __elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            __clock::now() - __t0);
        std::fprintf(
            stderr,
            "  [%4llds] submitted=%llu executed=%llu pending=%llu "
            "inline=%llu\n",
            static_cast<long long>(__elapsed.count()),
            static_cast<unsigned long long>(
                __ctx.__submitted_.load(std::memory_order_relaxed)),
            static_cast<unsigned long long>(
                __ctx.__executed_.load(std::memory_order_relaxed)),
            static_cast<unsigned long long>(
                __ctx.__pending_.load(std::memory_order_relaxed)),
            static_cast<unsigned long long>(
                __ctx.__spawn_failed_.load(std::memory_order_relaxed)));
        std::fflush(stderr);
    }

    // ----- Shut down producers / controller ---------------------------------
    __ctx.__stop_producers_.store(true, std::memory_order_release);
    for (auto &__t : __producers) { __t.join(); }
    __controller.join();

    // ----- Drain: keep nudging retired workers awake until pending hits 0 ---
    const auto __drain_t0 = __clock::now();
    while (__ctx.__pending_.load(std::memory_order_acquire) != 0) {
        // A late retire could still race in here; periodically resume so any
        // newly-retired worker can pick up tasks that were migrated to it.
        __sched._M_request_resume_n(__nthreads);
        std::this_thread::sleep_for(5ms);
        if (__clock::now() - __drain_t0 > 60s) {
            std::fprintf(
                stderr,
                "DRAIN TIMEOUT (seed=%llu): submitted=%llu executed=%llu "
                "pending=%llu inline=%llu\n",
                static_cast<unsigned long long>(__seed),
                static_cast<unsigned long long>(__ctx.__submitted_.load()),
                static_cast<unsigned long long>(__ctx.__executed_.load()),
                static_cast<unsigned long long>(__ctx.__pending_.load()),
                static_cast<unsigned long long>(__ctx.__spawn_failed_.load()));
            std::abort();
        }
    }

    // ----- Verify invariants -----------------------------------------------
    const std::uint64_t __sub =
        __ctx.__submitted_.load(std::memory_order_relaxed);
    const std::uint64_t __exec =
        __ctx.__executed_.load(std::memory_order_relaxed);
    if (__sub != __exec) {
        std::fprintf(
            stderr,
            "INVARIANT VIOLATED (seed=%llu): submitted=%llu != executed=%llu\n",
            static_cast<unsigned long long>(__seed),
            static_cast<unsigned long long>(__sub),
            static_cast<unsigned long long>(__exec));
        std::abort();
    }

    std::fprintf(
        stderr, "PASS (seed=%llu): submitted=%llu executed=%llu inline=%llu\n",
        static_cast<unsigned long long>(__seed),
        static_cast<unsigned long long>(__sub),
        static_cast<unsigned long long>(__exec),
        static_cast<unsigned long long>(__ctx.__spawn_failed_.load()));
    return 0;
}
