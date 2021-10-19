#define BOOST_TEST_MODULE producer_consumer_queue tests
#include <boost/test/included/unit_test.hpp>

#include <pc_queue/pc_queue.h>

#include <string>
#include <vector>
#include <functional>

//-------------------------------------------------------------------------------
template<class Key, class Value>
class Consumer : public IConsumer<Key, Value>
{
public:
    Consumer(std::function<void(Key, Value)> f)
        : m_f(f)
    {}

    void Consume(Key id, const Value& value) override
    {
        m_f(id, value);
    }

private:
    const std::function<void(Key, Value)> m_f;
};

//-------------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(BasicTest)
{
    std::vector<std::pair<int, std::string>> expectedQueue { {123, "1"}, {123, "2"}, {123, "3"}, {123, "4"}, {123, "5"}, {123, "6"} };
    std::vector<std::pair<int, std::string>> actualQueue;

    PCQueue<int, std::string> q;
    
    Consumer<int, std::string> c([&](int id, const std::string& value)
        {
            actualQueue.emplace_back(id, value);
        }
    );

    q.Subscribe(123, &c);

    for (auto& i : expectedQueue)
    {
        q.Enqueue(i.first, i.second);
    }

    q.StopProcessing(true);

    BOOST_TEST(expectedQueue.size() == actualQueue.size());
    for (size_t i = 0; i < expectedQueue.size(); i++)
    {
        BOOST_TEST(expectedQueue[i].first == actualQueue[i].first);
        BOOST_TEST(expectedQueue[i].second == actualQueue[i].second);
    }
}

//-------------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(MultithreadingTest)
{
    size_t count = 10000000;
    std::vector<std::pair<uint8_t, uint64_t>> actualQueue;
    actualQueue.reserve(count);

    std::atomic_uint32_t counter = 0;

    PCQueue<uint8_t, uint64_t> q;

    Consumer<uint8_t, uint64_t> c([&](uint8_t id, const uint64_t& value)
        {
            actualQueue.emplace_back(id, value);
        }
    );

    auto worker = [&](uint8_t key)
    {
        q.Subscribe(key, &c);

        while (counter < count)
            q.Enqueue(key, counter++);
    };

    std::thread t1{ [&]() { worker(1); } };
    std::thread t2{ [&]() { worker(2); } };
    std::thread t3{ [&]() { worker(3); } };
    std::thread t4{ [&]() { worker(4); } };
    std::thread t5{ [&]() { worker(5); } };
    std::thread t6{ [&]() { worker(6); } };

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    t6.join();

    q.StopProcessing(true);

    BOOST_TEST(counter == actualQueue.size());
}

//-------------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(PerformanceTest)
{
    size_t count = 20000000;

    auto perfTest = [&](int threadsCount)
    {
        std::vector<std::pair<int, std::string>> actualQueue;
        actualQueue.reserve(count);

        std::atomic_uint32_t counter = 0;

        Consumer<int, std::string> c([&](int id, const std::string& value)
            {
                actualQueue.emplace_back(id, value);
            }
        );

        auto worker = [&](PCQueue<int, std::string>& q, int key)
        {
            q.Subscribe(key, &c);

            while (counter < count)
                q.Enqueue(key, "msg" + std::to_string(counter++));
        };

        {
            PCQueue<int, std::string> q;
            auto begin = std::chrono::steady_clock::now();

            std::vector<std::thread> threads;

            for (int i = 0; i < threadsCount; i++)
            {
                threads.emplace_back([&q, &worker, i]() { worker(q, i); });
            }

            for (auto& t : threads)
            {
                t.join();
            }

            q.StopProcessing(true);

            auto end = std::chrono::steady_clock::now();
            std::cout << "Time elapsed with " << std::to_string(threadsCount) << " threads: " << std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << "[s]" << std::endl;
        }

        BOOST_TEST(counter == actualQueue.size());
    };

    perfTest(1);
    perfTest(2);
    perfTest(4);
    perfTest(8);
}
