#include "timer_heap.h"

TimerHeap::TimerHeap(std::size_t max_fd)
    : m_seqs(max_fd, 0)
{
}

void TimerHeap::add(int fd, int timeout_sec, int64_t now_sec) {
    if (fd < 0 || static_cast<std::size_t>(fd) >= m_seqs.size()) return;
    uint64_t seq = ++m_seqs[fd];
    m_heap.push({now_sec + timeout_sec, fd, seq});
}

void TimerHeap::refresh(int fd, int timeout_sec, int64_t now_sec) {
    // refresh 与 add 语义完全相同（懒删除：递增 seq 使旧节点失效，push 新节点）
    add(fd, timeout_sec, now_sec);
}

void TimerHeap::invalidate(int fd) {
    if (fd >= 0 && static_cast<std::size_t>(fd) < m_seqs.size()) {
        m_seqs[fd] = 0;
    }
}

void TimerHeap::tick(int64_t now_sec, const std::function<void(int)>& cb) {
    while (!m_heap.empty()) {
        const TimerNode& top = m_heap.top();
        if (top.expire > now_sec) break;

        TimerNode node = top;
        m_heap.pop();

        if (static_cast<std::size_t>(node.fd) < m_seqs.size() && m_seqs[node.fd] == node.seq) {
            m_seqs[node.fd] = 0;
            cb(node.fd);
        }
    }
}

int64_t TimerHeap::next_expire() const noexcept {
    if (m_heap.empty()) return -1;
    return m_heap.top().expire;
}
