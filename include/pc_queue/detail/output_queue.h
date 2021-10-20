#pragma once

#include <map>
#include <mutex>
#include <queue>
#include <vector>
#include <thread>
#include <stdexcept>
#include <shared_mutex>
#include <condition_variable>

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
struct IConsumer;

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
class OutputQueue
{
public:
    OutputQueue(Key id, size_t threadCount);
    ~OutputQueue();

    OutputQueue(const OutputQueue& other) = delete;
    OutputQueue& operator= (const OutputQueue& other) = delete;

    OutputQueue(OutputQueue&& other);
    OutputQueue& operator= (OutputQueue&& other);

    void Push(Value&& value);
    void Start();
    void StopProcessing(bool waitConsume = true);

private:
    void Process();

private:
    const Key m_id;
    cosnt size_t m_threadCount;

    std::shared_mutex m_consumerMutex;
    IConsumer<Key, Value>* m_consumer;

    std::queue<Value> m_queue;

    std::mutex m_mutex;
    std::condition_variable m_cv;

    bool m_running;
    std::vector<std::thread> m_threadPool;
};

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
OutputQueue<Key, Value>::OutputQueue(Key id, size_t threadCount)
    : m_id(id)
    , m_threadCount(threadCount)
    , m_running(false)
{
    if (!m_threadCount)
        throw std::invalid_argument("Thread count is zero");
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
OutputQueue<Key, Value>::OutputQueue(OutputQueue&& other)
    : m_id(other.m_id)
{

}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void OutputQueue<Key, Value>::Start()
{
    m_threadPool.reserve(m_threadCount);
    m_running = true;
    for (size_t i = 0; i < m_threadCount; ++i)
    {
        m_threadPool.emplace([&]() { Process(); });
    }
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void OutputQueue<Key, Value>::Process()
{
    auto condition = [&]()
    {
        return !m_running || (!m_queue.empty() && m_consumer != nullptr);
    };

    while (true)
    {
        std::vector<Value> m_buffer;
        {
            std::unique_lock<std::mutex> lock(m_mutex);

            if (!condition())
            {
                m_cv.wait(lock, [&] {
                        return condition();
                    }
                );
            }

            if (!m_running)
                return;

            m_buffer.reserve(m_queue.size());

            while (!m_queue.empty())
            {
                m_buffer.emplace_back(std::move(m_queue.front()));
                m_queue.pop();
            }
        }

        std::shared_lock<std::shared_mutex> lock(m_consumerMutex);
        for (auto& item : m_buffer)
        {
            try
            {
                m_consumer->Consume(m_id, item);
            }
            catch (...)
            {
                // Drop item
            }
        }
    }
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void OutputQueue<Key, Value>::Push(Value&& value)
{
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_queue.push(value);
    }
    m_cv.notify_one();
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void OutputQueue<Key, Value>::StopProcessing(bool waitConsume /* = true */)
{
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_running = false;
    }

    m_cv.notify_all();

    for (auto& t : m_threadPool)
    {
        if (t.joinable())
            t.join();
    }

    m_threadPool.clear();

    if (waitConsume)
    {
        std::shared_lock<std::shared_mutex> lock(m_consumerMutex);
        while (!m_queue.empty())
        {
            m_consumer->Consume(m_id, m_queue.front());
            m_queue.pop();
        }
    }
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
OutputQueue<Key, Value>::~OutputQueue()
{
    try
    {
        StopProcessing(false);
    }
    catch (...)
    {}
}
