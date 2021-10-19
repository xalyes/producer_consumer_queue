#pragma once

#include <map>
#include <queue>
#include <shared_mutex>
#include <thread>
#include <stdexcept>
#include <condition_variable>

template<typename Key, typename Value>
struct IConsumer
{
    virtual void Consume(Key id, const Value& value) = 0;
};

template<typename Key, typename Value>
class SingleQueue
{
public:
    SingleQueue(Key id, IConsumer<Key, Value>* consumer)
        : m_id(id)
        , m_consumer(consumer)
    {}

    SingleQueue(const SingleQueue& other) = delete;
    SingleQueue& operator= (const SingleQueue& other) = delete;

    SingleQueue(SingleQueue&& other)
        : m_id(other.m_id)
    {
        m_consumer = other.m_consumer;
        m_queue = std::move(other.m_queue);
    }

    SingleQueue& operator= (SingleQueue&& other)
    {
        m_id = other.m_id;
        m_consumer = other.m_consumer;
        m_queue = std::move(other.m_queue);
    }

    bool Empty() const
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        return m_queue.empty();
    }

    void ConsumeAll()
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        while (!m_queue.empty())
        {
            m_consumer->Consume(m_id, m_queue.front());
            m_queue.pop();
        }
    }

    void Push(const Value& v)
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_queue.push(v);
    }

private:
    const Key m_id;
    IConsumer<Key, Value>* m_consumer;
    mutable std::mutex m_mutex;
    std::queue<Value> m_queue;
};

template<typename Key, typename Value>
class PCQueue
{
public:
    PCQueue();

    void Subscribe(Key queueId, IConsumer<Key, Value>* consumer);
    void Unsubscribe(Key queueId);
    void Enqueue(Key queueId, Value value);

    void StopProcessing(bool waitConsume = true)
    {
        m_running = false;
        m_cv.notify_one();
        m_worker.join();

        if (waitConsume)
        {
            std::unique_lock<std::shared_mutex> lock(m_queuesMutex);
            for (auto& queue : m_queues)
            {
                queue.second.ConsumeAll();
            }
        }
    }

    ~PCQueue()
    {
        try
        {
            StopProcessing(false);
        }
        catch (...) {}
    }

private:
    void Process();

private:
    std::map<Key, SingleQueue<Key, Value>> m_queues;
    std::shared_mutex m_queuesMutex;
    std::condition_variable_any m_cv;

    std::atomic_bool m_running;
    std::thread m_worker;
};

template<typename Key, typename Value>
PCQueue<Key, Value>::PCQueue()
    : m_running(true)
    , m_worker([&]() { Process(); })
{}

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

template<typename Key, typename Value>
void PCQueue<Key, Value>::Subscribe(Key queueId, IConsumer<Key, Value>* consumer)
{
    std::unique_lock<std::shared_mutex> lock(m_queuesMutex);
    auto it = m_queues.find(queueId);
    if (it == m_queues.end())
        m_queues.emplace(queueId, SingleQueue<Key, Value>(queueId, consumer));
    else
        throw std::invalid_argument("Queue with that id already exists");
}

template<typename Key, typename Value>
void PCQueue<Key, Value>::Unsubscribe(Key queueId)
{
    std::unique_lock<std::shared_mutex> lock(m_queuesMutex);
    auto it = m_queues.find(queueId);
    if (it != m_queues.end())
        m_queues.erase(it);
}

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
        throw std::invalid_argument("Queue with that id doesn't exists");
    }
    m_cv.notify_one();
}
