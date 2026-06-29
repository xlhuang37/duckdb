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

namespace duckdb {

struct ConcurrentQueue;
struct QueueProducerToken;
class ClientContext;
class DatabaseInstance;
class TaskScheduler;

struct SchedulerThread;

struct ProducerToken {
	ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token);
	~ProducerToken();

	TaskScheduler &scheduler;
	unique_ptr<QueueProducerToken> token;
	mutex producer_lock;
};

//! A SchedulerQueryNode is the per-query handle into the multi-level priority queue. It owns one ProducerToken
//! per priority level and tracks the accumulated CPU time of the query, which is used to demote the query's
//! future tasks to lower-priority levels.
struct SchedulerQueryNode {
	SchedulerQueryNode(TaskScheduler &scheduler, vector<unique_ptr<ProducerToken>> tokens);
	~SchedulerQueryNode();

	TaskScheduler &scheduler;
	//! One producer token per priority level (index 0 = highest priority)
	vector<unique_ptr<ProducerToken>> tokens;
	//! Accumulated CPU time (in microseconds) consumed by tasks of this query
	atomic<int64_t> cpu_time_used;

	//! Compute the current priority level based on accumulated CPU time
	idx_t ComputeLevel() const;
	//! Add CPU time (in microseconds) consumed by a task of this query
	void AddCpuTime(int64_t usecs);
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

	unique_ptr<SchedulerQueryNode> CreateQueryNode();
	//! Schedule a task to be executed by the task scheduler
	void ScheduleTask(SchedulerQueryNode &node, shared_ptr<Task> task);
	void ScheduleTasks(SchedulerQueryNode &node, vector<shared_ptr<Task>> &tasks);
	//! Fetches a task from a specific query node, returns true if successful or false if no tasks were available
	bool GetTaskFromProducer(SchedulerQueryNode &node, shared_ptr<Task> &task);
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
	idx_t GetTaskCountForProducer(SchedulerQueryNode &node) const;

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
	void RelaunchThreadsInternal(int32_t n, bool destroy);

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
