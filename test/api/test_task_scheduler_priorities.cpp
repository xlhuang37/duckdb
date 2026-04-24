#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <thread>

using namespace duckdb;

namespace {
class TrackingTask : public Task {
public:
	TrackingTask(vector<int> &execution_order_p, mutex &lock_p, int marker_p, idx_t sleep_millis_p = 0)
	    : execution_order(execution_order_p), lock(lock_p), marker(marker_p), sleep_millis(sleep_millis_p) {
	}

	TaskExecutionResult Execute(TaskExecutionMode mode) override {
		D_ASSERT(mode == TaskExecutionMode::PROCESS_ALL);
		if (sleep_millis > 0) {
			std::this_thread::sleep_for(std::chrono::milliseconds(sleep_millis));
		}
		lock_guard<mutex> guard(lock);
		execution_order.push_back(marker);
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	vector<int> &execution_order;
	mutex &lock;
	int marker;
	idx_t sleep_millis;
};
} // namespace

TEST_CASE("Task scheduler SRPT policy prioritizes producers with lower estimated remaining work", "[api]") {
	DBConfig config;
	config.options.maximum_threads = 1;
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	scheduler.SetSchedulingPolicy(TaskSchedulingPolicy::SRPT);

	auto short_query = scheduler.CreateProducer();
	auto long_query = scheduler.CreateProducer();

	scheduler.SetProducerProgress(*short_query, 0.95);
	scheduler.SetProducerProgress(*long_query, 0.10);

	vector<int> execution_order;
	mutex lock;
	scheduler.ScheduleTask(*long_query, make_shared_ptr<TrackingTask>(execution_order, lock, 2));
	scheduler.ScheduleTask(*short_query, make_shared_ptr<TrackingTask>(execution_order, lock, 1));

	atomic<bool> marker(true);
	scheduler.ExecuteTasks(&marker, 2);

	REQUIRE(execution_order.size() == 2);
	REQUIRE(execution_order[0] == 1);
}

TEST_CASE("Task scheduler LAS policy favors producers with lower attained service", "[api]") {
	DBConfig config;
	config.options.maximum_threads = 1;
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	scheduler.SetSchedulingPolicy(TaskSchedulingPolicy::LAS);

	auto producer_a = scheduler.CreateProducer();
	auto producer_b = scheduler.CreateProducer();

	vector<int> execution_order;
	mutex lock;

	atomic<bool> marker(true);
	scheduler.ScheduleTask(*producer_a, make_shared_ptr<TrackingTask>(execution_order, lock, 1, 25));
	scheduler.ExecuteTasks(&marker, 1);

	scheduler.ScheduleTask(*producer_a, make_shared_ptr<TrackingTask>(execution_order, lock, 2));
	scheduler.ScheduleTask(*producer_b, make_shared_ptr<TrackingTask>(execution_order, lock, 3));
	scheduler.ExecuteTasks(&marker, 1);

	REQUIRE(execution_order.size() == 2);
	REQUIRE(execution_order[1] == 3);
}

TEST_CASE("Task scheduler SFQ policy starts new producers at frontier service", "[api]") {
	DBConfig config;
	config.options.maximum_threads = 1;
	DuckDB db(nullptr, &config);
	Connection con(db);
	auto &scheduler = TaskScheduler::GetScheduler(*db.instance);
	scheduler.SetSchedulingPolicy(TaskSchedulingPolicy::SFQ);

	auto producer_a = scheduler.CreateProducer();
	vector<int> execution_order;
	mutex lock;
	atomic<bool> marker(true);

	// Accumulate service on producer_a to advance the SFQ frontier.
	scheduler.ScheduleTask(*producer_a, make_shared_ptr<TrackingTask>(execution_order, lock, 1, 25));
	scheduler.ExecuteTasks(&marker, 1);

	// New producers should start at the current frontier instead of zero service.
	auto producer_new = scheduler.CreateProducer();
	scheduler.SetProducerPriority(*producer_a, 0);
	scheduler.SetProducerPriority(*producer_new, -1);

	scheduler.ScheduleTask(*producer_a, make_shared_ptr<TrackingTask>(execution_order, lock, 2));
	scheduler.ScheduleTask(*producer_new, make_shared_ptr<TrackingTask>(execution_order, lock, 3));
	scheduler.ExecuteTasks(&marker, 1);

	REQUIRE(execution_order.size() == 2);
	REQUIRE(execution_order[1] == 2);
}
