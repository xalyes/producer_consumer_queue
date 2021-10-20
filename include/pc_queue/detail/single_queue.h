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
    SingleQueue(Key id, IConsumer<Key, Value>* consumer);

    SingleQueue(const SingleQueue& other) = delete;
    SingleQueue& operator= (const SingleQueue& other) = delete;

    SingleQueue(SingleQueue&& other) noexcept;
    SingleQueue& operator= (SingleQueue&& other);

    bool Empty() const;
    void ConsumeAll();
    std::optional<Value> Pop();
    void Push(const Value& v);

    void Subscribe(IConsumer<Key, Value>* consumer);
    void Unsubscribe();

private:
    const Key m_id;
    IConsumer<Key, Value>* m_consumer;
    mutable std::mutex m_mutex;
    std::queue<Value> m_queue;
};

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
SingleQueue<Key, Value>::SingleQueue(Key id, IConsumer<Key, Value>* consumer)
    : m_id(id)
    , m_consumer(consumer)
{}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
SingleQueue<Key, Value>::SingleQueue(SingleQueue&& other) noexcept
    : m_id(other.m_id)
{
    m_consumer = other.m_consumer;
    other.m_consumer = nullptr;
    m_queue = std::move(other.m_queue);
}

//-------------------------------------------------------------------------------
template<typename Key, typename Value>
SingleQueue<Key, Value>& SingleQueue<Key, Value>::operator= (SingleQueue<Key, Value>&& other)
{
    m_id = other.m_id;
    m_consumer = other.m_consumer;
    other.m_consumer = nullptr;
    m_queue = std::move(other.m_queue);
    return *this;
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
    std::unique_lock<std::mutex> lock(m_mutex);

    if (!m_consumer)
        return;

    while (!m_queue.empty())
    {
        m_consumer->Consume(m_id, m_queue.front());
        m_queue.pop();
    }
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
void SingleQueue<Key, Value>::Push(const Value& v)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    m_queue.push(v);
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

