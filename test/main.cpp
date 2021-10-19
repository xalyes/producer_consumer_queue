#define BOOST_TEST_MODULE producer_consumer_queue tests
#include <boost/test/included/unit_test.hpp>

#include <pc_queue/pc_queue.h>

#include <string>
#include <vector>
#include <functional>

class Consumer : public IConsumer<int, std::string>
{
public:
    Consumer(std::function<void(int, std::string)> f)
        : m_f(f)
    {}

    void Consume(int id, const std::string& value) override
    {
        m_f(id, value);
    }

private:
    const std::function<void(int, std::string)> m_f;
};

BOOST_AUTO_TEST_CASE(BasicTest)
{
    std::vector<std::pair<int, std::string>> expectedQueue { {123, "1"}, {123, "2"}, {123, "3"}, {123, "4"}, {123, "5"}, {123, "6"} };
    std::vector<std::pair<int, std::string>> actualQueue;

    PCQueue<int, std::string> q;
    
    Consumer c([&](int id, const std::string& value)
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
