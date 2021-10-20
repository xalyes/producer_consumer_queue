#pragma once

#include <mutex>
#include <optional>

template<typename Key, typename Value>
struct IConsumer;

//-------------------------------------------------------------------------------
//                             SingleQueue
//-------------------------------------------------------------------------------
template<typename Key, typename Value>
class SingleQueue
{
public:
    SingleQueue(Key id, IConsumer<Key, Value>* consumer, size_t maxCapacity);

    SingleQueue(const SingleQueue& other) = delete;
    SingleQueue& operator= (const SingleQueue& other) = delete;

    SingleQueue(SingleQueue&& other) = delete;
    SingleQueue& operator= (SingleQueue&& other) = delete;

    bool Empty() const;
    void ConsumeAll();
    std::optional<Value> Pop();
    bool Push(Value&& v);

    void Subscribe(IConsumer<Key, Value>* consumer);
    void Unsubscribe();

    void StopProcessing(bool waitConsume = true);
    ~SingleQueue();

private:
    void Process();

private:
    const Key m_id;
    const size_t m_maxCapacity;
    IConsumer<Key, Value>* m_consumer;
    mutable std::mutex m_mutex;
    std::queue<Value> m_queue;

    std::condition_variable m_cv;
    bool m_running;
    std::thread m_worker;
};

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
SingleQueue<Key, Value>::SingleQueue(Key id, IConsumer<Key, Value>* consumer, size_t maxCapacity)
    : m_id(id)
    , m_maxCapacity(maxCapacity)
    , m_consumer(consumer)
    , m_running(true)
    , m_worker([&]() { Process(); })
{}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void SingleQueue<Key, Value>::Process()
{
    auto condition = [&]()
    {
        return !m_running || (!m_queue.empty() && m_consumer);
    };

    while (true)
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        if (!condition())
        {
            m_cv.wait(lock, [&]() {
                    return condition();
                }
            );
        }

        if (!m_running)
            return;

        while (!m_queue.empty())
        {
            try
            {
                m_consumer->Consume(m_id, std::move(m_queue.front()));
            }
            catch (...)
            {
                // Drop
            }

            m_queue.pop();
        }
    }
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
bool SingleQueue<Key, Value>::Empty() const
{
    std::unique_lock<std::mutex> lock(m_mutex);
    return m_queue.empty();
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void SingleQueue<Key, Value>::ConsumeAll()
{
    m_cv.notify_one();
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
std::optional<Value> SingleQueue<Key, Value>::Pop()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_queue.empty())
        return std::nullopt;

    Value v = m_queue.front();
    m_queue.pop();
    return v;
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
bool SingleQueue<Key, Value>::Push(Value&& v)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_queue.size() == m_maxCapacity)
        return false;

    m_queue.push(std::move(v));
    return true;
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void SingleQueue<Key, Value>::Subscribe(IConsumer<Key, Value>* consumer)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_consumer = consumer;
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
void SingleQueue<Key, Value>::Unsubscribe()
{
    std::unique_lock<std::mutex> lock(m_mutex);
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

    m_cv.notify_one();
    if (m_worker.joinable())
        m_worker.join();

    if (!waitConsume)
        return;

    std::unique_lock<std::mutex> lock(m_mutex);

    if (!m_consumer)
        return;

    while (!m_queue.empty())
    {
        try
        {
            m_consumer->Consume(m_id, std::move(m_queue.front()));
        }
        catch (...)
        {
            // Drop
        }

        m_queue.pop();
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
    catch (...) {}
}
