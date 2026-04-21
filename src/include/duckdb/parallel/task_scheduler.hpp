//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task_scheduler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/parallel/task.hpp"
#include <unordered_map>

namespace duckdb {

struct ConcurrentQueue;
struct QueueProducerToken;
class ClientContext;
class DatabaseInstance;
class TaskScheduler;

struct SchedulerThread;

enum class TaskSchedulingPolicy : uint8_t { STATIC_PRIORITY = 0, SRPT = 1, SFQ = 2 };

struct ProducerToken {
	ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token);
	~ProducerToken();

	TaskScheduler &scheduler;
	unique_ptr<QueueProducerToken> token;
	mutex producer_lock;
};

//! The TaskScheduler is responsible for managing tasks and threads
class TaskScheduler {
	// timeout for semaphore wait, default 5ms
	constexpr static int64_t TASK_TIMEOUT_USECS = 5000;

public:
	explicit TaskScheduler(DatabaseInstance &db);
	~TaskScheduler();

	DUCKDB_API static TaskScheduler &GetScheduler(ClientContext &context);
	DUCKDB_API static TaskScheduler &GetScheduler(DatabaseInstance &db);

	unique_ptr<ProducerToken> CreateProducer();
	//! Sets an explicit producer priority. Higher values are dequeued first.
	void SetProducerPriority(ProducerToken &producer, int64_t priority);
	//! Adjusts the producer priority by the provided delta.
	void AdjustProducerPriority(ProducerToken &producer, int64_t delta);
	//! Sets producer priority based on query size/cardinality (smaller queries get higher priority).
	void SetProducerQuerySize(ProducerToken &producer, idx_t query_size);
	//! Sets the scheduler policy used to select the next producer when tasks are available.
	void SetSchedulingPolicy(TaskSchedulingPolicy policy);
	//! Sets producer progress in the range [0, 1]. Used by dynamic policies to estimate remaining work.
	void SetProducerProgress(ProducerToken &producer, double progress);
	//! Schedule a task to be executed by the task scheduler
	void ScheduleTask(ProducerToken &producer, shared_ptr<Task> task);
	void ScheduleTasks(ProducerToken &producer, vector<shared_ptr<Task>> &tasks);
	//! Fetches a task from a specific producer, returns true if successful or false if no tasks were available
	bool GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	//! Run tasks forever until "marker" is set to false, "marker" must remain valid until the thread is joined
	void ExecuteForever(atomic<bool> *marker);
	//! Run tasks until `marker` is set to false, `max_tasks` have been completed, or until there are no more tasks
	//! available. Returns the number of tasks that were completed.
	idx_t ExecuteTasks(atomic<bool> *marker, idx_t max_tasks);
	//! Run tasks until `max_tasks` have been completed, or until there are no more tasks available
	void ExecuteTasks(idx_t max_tasks);

	//! Sets the amount of background threads to be used for execution, based on the number of total threads
	//! and the number of external threads. External threads, e.g. the main thread, will also be used for execution.
	//! Launches `total_threads - external_threads` background worker threads.
	void SetThreads(idx_t total_threads, idx_t external_threads);

	void RelaunchThreads();

	//! Returns the number of threads
	DUCKDB_API int32_t NumberOfThreads();

	idx_t GetNumberOfTasks() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(ProducerToken &token) const;

	//! Send signals to n threads, signalling for them to wake up and attempt to execute a task
	void Signal(idx_t n);

	//! Yield to other threads
	static void YieldThread();

	//! Set the allocator flush threshold
	void SetAllocatorFlushTreshold(idx_t threshold);
	//! Sets the allocator background thread
	void SetAllocatorBackgroundThreads(bool enable);

	//! Get the number of the CPU on which the calling thread is currently executing.
	//! Fallback to calling thread id if CPU number is not available.
	//! Result do not need to be exact 'return 0' is a valid fallback strategy
	static idx_t GetEstimatedCPUId();

private:
	struct ProducerSchedulingState {
		int64_t base_priority = 0;
		idx_t completed_tasks = 0;
		idx_t in_flight_tasks = 0;
		uint64_t runtime_micros = 0;
		double progress = 0;
	};

	void RegisterProducer(ProducerToken &producer);
	void UnregisterProducer(ProducerToken &producer);
	bool TryDequeuePrioritizedProducerTask(shared_ptr<Task> &task);
	void RecordTaskDequeued(Task &task);
	void RecordTaskCompletion(Task &task, uint64_t runtime_micros);
	void RelaunchThreadsInternal(int32_t n, bool destroy);

	friend struct ProducerToken;

private:
	DatabaseInstance &db;
	//! The task queue
	unique_ptr<ConcurrentQueue> queue;
	//! Lock for modifying the thread count
	mutex thread_lock;
	//! The active background threads of the task scheduler
	vector<unique_ptr<SchedulerThread>> threads;
	//! Markers used by the various threads, if the markers are set to "false" the thread execution is stopped
	vector<unique_ptr<atomic<bool>>> markers;
	//! Per-producer dynamic priorities (higher value = higher dequeue priority)
	mutex producer_priority_lock;
	unordered_map<ProducerToken *, ProducerSchedulingState> producer_priorities;
	TaskSchedulingPolicy scheduling_policy = TaskSchedulingPolicy::STATIC_PRIORITY;
	//! The threshold after which to flush the allocator after completing a task
	atomic<idx_t> allocator_flush_threshold;
	//! Whether allocator background threads are enabled
	atomic<bool> allocator_background_threads;
	//! Requested thread count (set by the 'threads' setting)
	atomic<int32_t> requested_thread_count;
	//! The amount of threads currently running
	atomic<int32_t> current_thread_count;
};

} // namespace duckdb
