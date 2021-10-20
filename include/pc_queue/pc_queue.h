#pragma once

#include <map>
#include <queue>
#include <shared_mutex>
#include <thread>
#include <stdexcept>
#include <condition_variable>

#include "detail/single_queue.h"

//-------------------------------------------------------------------------------
//                             IConsumer
//-------------------------------------------------------------------------------
template<typename Key, typename Value>
struct IConsumer
{
    virtual void Consume(Key id, const Value& value) = 0;
};

//-------------------------------------------------------------------------------
//                             PCQueue
//-------------------------------------------------------------------------------
template<typename Key, typename Value>
class PCQueue
{
public:
    PCQueue();

    PCQueue(const PCQueue& other) = delete;
    PCQueue& operator= (const PCQueue& other) = delete;

    PCQueue(PCQueue&& other) = delete;
    PCQueue& operator= (PCQueue&& other) = delete;

    void Subscribe(Key queueId, IConsumer<Key, Value>* consumer);
    void Unsubscribe(Key queueId);

    void Enqueue(Key queueId, Value value);
    std::optional<Value> Dequeue(Key queueId);

    void StopProcessing(bool waitConsume = true);
    ~PCQueue();

private:
    void Process();

private:
    std::map<Key, SingleQueue<Key, Value>> m_queues;
    std::shared_mutex m_queuesMutex;
    std::condition_variable_any m_cv;

    std::atomic_bool m_running;
    std::thread m_worker;
};

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
PCQueue<Key, Value>::PCQueue()
    : m_running(true)
    , m_worker([&]() { Process(); })
{}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void PCQueue<Key, Value>::Process()
{
    auto queuesEmpty = [&]()
    {
        auto itFound = std::find_if(m_queues.begin(), m_queues.end(),
            [](const std::pair<const Key, SingleQueue<Key, Value>>& q) { return !q.second.Empty(); }
        );
        return itFound == m_queues.end();
    };

    while (m_running)
    {
        std::shared_lock<std::shared_mutex> lock(m_queuesMutex);

        if (queuesEmpty())
        {
            m_cv.wait(lock, [&] {
                    return !m_running || !queuesEmpty();
                }
            );
        }

        if (!m_running)
            return;

        for (auto& queue : m_queues)
        {
            queue.second.ConsumeAll();
        }
    }
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void PCQueue<Key, Value>::Subscribe(Key queueId, IConsumer<Key, Value>* consumer)
{
    {
        std::unique_lock<std::shared_mutex> lock(m_queuesMutex);
        auto it = m_queues.find(queueId);
        if (it == m_queues.end())
        {
            m_queues.try_emplace(queueId, queueId, consumer);
        }
        else
        {
            it->second.Subscribe(consumer);
        }
    }
    m_cv.notify_one();
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void PCQueue<Key, Value>::Unsubscribe(Key queueId)
{
    std::unique_lock<std::shared_mutex> lock(m_queuesMutex);
    auto it = m_queues.find(queueId);
    if (it != m_queues.end())
    {
        SingleQueue<Key, Value>& queue = it->second;
        queue.Unsubscribe();
    }
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void PCQueue<Key, Value>::Enqueue(Key queueId, Value value)
{
    std::shared_lock<std::shared_mutex> lock(m_queuesMutex);
    auto it = m_queues.find(queueId);
    if (it != m_queues.end())
    {
        SingleQueue<Key, Value>& queue = it->second;
        queue.Push(value);
    }
    else
    {
        lock.unlock();
        std::unique_lock<std::shared_mutex> uniqueLock(m_queuesMutex);
        auto secondIt = m_queues.find(queueId);
        if (secondIt == m_queues.end())
        {
            secondIt = m_queues.try_emplace(queueId, queueId, nullptr).first;
        }

        SingleQueue<Key, Value>& queue = secondIt->second;
        queue.Push(value);
    }
    m_cv.notify_one();
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
std::optional<Value> PCQueue<Key, Value>::Dequeue(Key queueId)
{
    std::shared_lock<std::shared_mutex> lock(m_queuesMutex);
    auto it = m_queues.find(queueId);
    if (it != m_queues.end())
    {
        SingleQueue<Key, Value>& queue = it->second;

        return queue.Pop();
    }
    return std::nullopt;
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void PCQueue<Key, Value>::StopProcessing(bool waitConsume /* = true */)
{
    m_running = false;
    m_cv.notify_one();
    if (m_worker.joinable())
        m_worker.join();

    if (waitConsume)
    {
        std::unique_lock<std::shared_mutex> lock(m_queuesMutex);
        for (auto& queue : m_queues)
        {
            queue.second.StopProcessing(true);
        }
    }
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
PCQueue<Key, Value>::~PCQueue()
{
    try
    {
        StopProcessing(false);
    }
    catch (...) {}
}
