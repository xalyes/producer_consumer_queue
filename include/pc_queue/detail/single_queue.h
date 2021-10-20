#pragma once

#include <mutex>
#include <optional>

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
struct IConsumer;

// ------------------------------------------------------------------------------ -
template<typename Key, typename Value>
class SingleQueue
{
public:
    SingleQueue(Key id, IConsumer<Key, Value>* consumer, size_t threadCount);
    ~SingleQueue();

    SingleQueue(const SingleQueue& other) = delete;
    SingleQueue& operator= (const SingleQueue& other) = delete;

    SingleQueue(SingleQueue&& other) = delete;
    SingleQueue& operator= (SingleQueue&& other) = delete;

    std::optional<Value> Pop();
    void Push(Value&& value);
    void StopProcessing(bool waitConsume = true);

    void Subscribe(IConsumer<Key, Value>* consumer);
    void Unsubscribe();

private:
    void Process();

private:
    const Key m_id;

    std::shared_mutex m_processMutex;

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
SingleQueue<Key, Value>::SingleQueue(Key id, IConsumer<Key, Value>* consumer, size_t threadCount)
    : m_id(id)
    , m_consumer(consumer)
{
    if (!threadCount)
        throw std::invalid_argument("Thread count is zero");

    m_threadPool.reserve(threadCount);
    for (size_t i = 0; i < threadCount; ++i)
    {
        m_threadPool.emplace_back([&]() { Process(); });
    }
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void SingleQueue<Key, Value>::Process()
{
    auto condition = [&]()
    {
        return !m_running || (!m_queue.empty() && m_consumer != nullptr);
    };

    while (true)
    {
        std::vector<Value> buffer;
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

            buffer.reserve(m_queue.size());

            while (!m_queue.empty())
            {
                buffer.emplace_back(std::move(m_queue.front()));
                m_queue.pop();
            }
        }

        std::shared_lock<std::shared_mutex> lock(m_consumerMutex);
        for (auto& item : buffer)
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
void SingleQueue<Key, Value>::Push(Value&& value)
{
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_queue.push(value);
    }
    m_cv.notify_one();
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
std::optional<Value> SingleQueue<Key, Value>::Pop()
{
    return std::nullopt;
    /*std::unique_lock<std::mutex> lock(m_mutex);
    if (m_queue.empty())
        return std::nullopt;

    Value v = std::move(m_queue.front());
    m_queue.pop();
    return v;*/
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void SingleQueue<Key, Value>::Subscribe(IConsumer<Key, Value>* consumer)
{
    {
        std::unique_lock<std::shared_mutex> lock(m_consumerMutex);
        m_consumer = consumer;
    }
    m_cv.notify_one();
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void SingleQueue<Key, Value>::Unsubscribe()
{
    std::unique_lock<std::shared_mutex> lock(m_consumerMutex);
    m_consumer = nullptr;
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void SingleQueue<Key, Value>::StopProcessing(bool waitConsume /* = true */)
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
SingleQueue<Key, Value>::~SingleQueue()
{
    try
    {
        StopProcessing(false);
    }
    catch (...)
    {
    }
}

