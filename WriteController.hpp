#pragma once
#include <atomic>
#include <chrono>
#include <thread>
#include <cstdint>
#include <algorithm>

class WriteController {
public:
    static constexpr int kL0SlowdownTrigger = 4;
    static constexpr int kL0StopTrigger = 12;
    static constexpr int kMemTableSlowdownTrigger = 2;
    static constexpr int kMemTableStopTrigger = 4;

    enum class State : uint8_t { NORMAL = 0, DELAY = 1, STOP = 2 };

    static WriteController& instance() {
        static WriteController wc;
        return wc;
    }

    void ApplyBackpressure() {
        uint8_t state = fast_state_.load(std::memory_order_relaxed);
        if (state == static_cast<uint8_t>(State::NORMAL)) [[likely]] {
            return;
        }
        DoBackpressure(state);
    }

    void UpdateState(int l0_count, int memtable_count) {
        State current_state = static_cast<State>(fast_state_.load(std::memory_order_relaxed));
        State new_state = State::NORMAL;
        uint32_t delay_us = 0;

        if (l0_count >= kL0StopTrigger || memtable_count >= kMemTableStopTrigger) {
            new_state = State::STOP;
        } else if (l0_count >= kL0SlowdownTrigger || memtable_count >= kMemTableSlowdownTrigger) {
            new_state = State::DELAY;
            int over_l0 = std::max(0, l0_count - kL0SlowdownTrigger);
            int over_mem = std::max(0, memtable_count - kMemTableSlowdownTrigger);
            int over = std::max(over_l0, over_mem);
            delay_us = std::min(2000u, 100u + static_cast<uint32_t>(over) * 200u);
        }

        // ✅ 优化：增加状态迟滞（Hysteresis），防止在阈值边缘频繁抖动
        // 如果当前是 STOP，只有当指标降到 Slowdown 阈值以下才降级为 DELAY
        // 如果当前是 DELAY，只有当指标降到正常才降级为 NORMAL
        if (current_state == State::STOP && new_state == State::DELAY) {
            if (l0_count < kL0SlowdownTrigger && memtable_count < kMemTableSlowdownTrigger) {
                new_state = State::NORMAL; // 直接降到底
            }
        }

        uint32_t packed = (static_cast<uint32_t>(new_state) << 24) | (delay_us & 0x00FFFFFF);
        packed_state_.store(packed, std::memory_order_release);
        fast_state_.store(static_cast<uint8_t>(new_state), std::memory_order_release);
    }

    State GetState() const {
        return static_cast<State>(fast_state_.load(std::memory_order_relaxed));
    }

private:
    WriteController() = default;

    void DoBackpressure(uint8_t state) {
        if (state == static_cast<uint8_t>(State::STOP)) {
            while (fast_state_.load(std::memory_order_acquire) == static_cast<uint8_t>(State::STOP)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        } else if (state == static_cast<uint8_t>(State::DELAY)) {
            uint32_t packed = packed_state_.load(std::memory_order_acquire);
            uint32_t delay_us = packed & 0x00FFFFFF;
            if (delay_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(delay_us));
            }
        }
    }

    alignas(64) std::atomic<uint8_t> fast_state_{static_cast<uint8_t>(State::NORMAL)};
    alignas(64) std::atomic<uint32_t> packed_state_{0};
};