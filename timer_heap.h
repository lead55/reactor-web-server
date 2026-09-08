#ifndef TIMER_HEAP_H
#define TIMER_HEAP_H

#include <queue>
#include <vector>
#include <cstddef>    // std::size_t
#include <cstdint>
#include <functional>

// -------- 小根堆定时器节点 --------
// expire: 绝对超时时刻（CLOCK_MONOTONIC 秒）
// seq:    版本号，配合 m_seqs[fd] 实现"懒删除"——refresh/invalidate 只递增 seq，
//         旧节点留在堆里，tick 时 seq 不匹配则跳过，避免 O(n) 查找删除。
struct TimerNode {
    int64_t  expire;   // 绝对超时（秒，CLOCK_MONOTONIC）
    int      fd;
    uint64_t seq;      // 版本号
};

// 小根堆比较器：expire 小的优先（priority_queue 默认大根堆，用 > 反转）
struct TimerNodeCmp {
    bool operator()(const TimerNode& a, const TimerNode& b) const noexcept {
        return a.expire > b.expire;
    }
};

// 基于小根堆的定时器，用于清理空闲连接。
//   - add:       新连接注册定时器
//   - refresh:   有 I/O 活动时续期（递增 seq + push 新节点，旧节点 tick 时自动失效）
//   - invalidate: 连接关闭时使所有 pending 节点失效
//   - tick:      弹出所有已超时的有效节点，回调关闭对应连接
//   - next_expire: 返回最早超时时刻，用于设置 timerfd
class TimerHeap {
public:
    explicit TimerHeap(std::size_t max_fd);

    // 注册新连接的超时定时器
    void add(int fd, int timeout_sec, int64_t now_sec);
    // 续期（有 I/O 活动时调用）
    void refresh(int fd, int timeout_sec, int64_t now_sec);
    // 使 fd 的所有 pending 定时器节点失效（关闭连接时调用）
    void invalidate(int fd);

    // 弹出所有已超时的有效节点，对每个调用 cb(fd)
    void tick(int64_t now_sec, const std::function<void(int)>& cb);

    // 返回最早超时时刻（秒），堆空返回 -1
    // 注意：堆顶可能含失效节点，返回值可能偏早——tick 时会跳过失效节点，无害
    int64_t next_expire() const noexcept;

private:
    std::priority_queue<TimerNode, std::vector<TimerNode>, TimerNodeCmp> m_heap;
    std::vector<uint64_t> m_seqs;   // 按 fd 索引，当前有效 seq；0 = 无效
};

#endif // TIMER_HEAP_H
