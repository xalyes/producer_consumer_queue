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
    std::map<Key, SingleQueue<Key, Value>> m_queues;
    std::shared_mutex m_queuesMutex;
    std::condition_variable_any m_cv;
};

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
PCQueue<Key, Value>::PCQueue()
{}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void PCQueue<Key, Value>::Subscribe(Key queueId, IConsumer<Key, Value>* consumer)
{
    {
        std::unique_lock<std::shared_mutex> lock(m_queuesMutex);
        auto it = m_queues.find(queueId);
        if (it == m_queues.end())
        {
            m_queues.try_emplace(queueId, queueId, consumer, 2);
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
        queue.Push(std::move(value));
    }
    else
    {
        lock.unlock();
        std::unique_lock<std::shared_mutex> uniqueLock(m_queuesMutex);
        auto secondIt = m_queues.find(queueId);
        if (secondIt == m_queues.end())
        {
            secondIt = m_queues.try_emplace(queueId, queueId, nullptr, 2).first;
        }

        SingleQueue<Key, Value>& queue = secondIt->second;
        queue.Push(std::move(value));
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
