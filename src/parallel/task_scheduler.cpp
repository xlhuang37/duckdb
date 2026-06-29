#include "duckdb/parallel/task_scheduler.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/block_allocator.hpp"
#ifndef DUCKDB_NO_THREADS
#include "concurrentqueue.h"
#include "duckdb/common/thread.hpp"
#include "lightweightsemaphore.h"

#include <ctime>
#include <thread>
#else
#include <queue>
#endif

#if defined(_WIN32)
#include <windows.h>
#elif defined(__GNUC__)
#include <sched.h>
#include <unistd.h>
#if defined(__GLIBC__)
#include <pthread.h>
#endif
#endif

namespace duckdb {
struct SchedulerThread {
#ifndef DUCKDB_NO_THREADS
	explicit SchedulerThread(unique_ptr<thread> thread_p) : internal_thread(std::move(thread_p)) {
	}

	unique_ptr<thread> internal_thread;
#endif
};

//! Number of priority levels in the multi-level priority queue (index 0 = highest priority)
static constexpr idx_t NUM_PRIORITY_LEVELS = 3;
//! Cumulative CPU-time thresholds (in microseconds) at which a query is demoted to the next lower level.
//! A query at level i is demoted to level i+1 once its accumulated CPU time reaches DEMOTION_QUANTUM_USECS[i].
//! The last level never demotes further (value -1 is a sentinel meaning "no further demotion").
static constexpr int64_t DEMOTION_QUANTUM_USECS[NUM_PRIORITY_LEVELS] = {50000, 500000, -1};

#ifndef DUCKDB_NO_THREADS
typedef duckdb_moodycamel::ConcurrentQueue<shared_ptr<Task>> concurrent_queue_t;
typedef duckdb_moodycamel::LightweightSemaphore lightweight_semaphore_t;

struct ConcurrentQueue {
	ConcurrentQueue() : tasks_in_queue(0) {
	}

	lightweight_semaphore_t semaphore;

	void Enqueue(SchedulerQueryNode &node, shared_ptr<Task> task);
	void EnqueueBulk(SchedulerQueryNode &node, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(SchedulerQueryNode &node, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(SchedulerQueryNode &node) const;
	concurrent_queue_t &GetQueue(idx_t level) {
		return q[level];
	}

private:
	concurrent_queue_t q[NUM_PRIORITY_LEVELS];
	atomic<idx_t> tasks_in_queue;
};

struct QueueProducerToken {
	QueueProducerToken(ConcurrentQueue &queue, idx_t level) : queue_token(queue.GetQueue(level)) {
	}

	duckdb_moodycamel::ProducerToken queue_token;
};

void ConcurrentQueue::Enqueue(SchedulerQueryNode &node, shared_ptr<Task> task) {
	const auto level = node.ComputeLevel();
	auto &producer = *node.tokens[level];
	lock_guard<mutex> producer_lock(producer.producer_lock);
	task->token = node.shared_from_this();
	if (q[level].enqueue(producer.token->queue_token, std::move(task))) {
		++tasks_in_queue;
		semaphore.signal();
	} else {
		throw InternalException("Could not schedule task!");
	}
}

void ConcurrentQueue::EnqueueBulk(SchedulerQueryNode &node, vector<shared_ptr<Task>> &tasks) {
	typedef std::make_signed<std::size_t>::type ssize_t;
	const auto level = node.ComputeLevel();
	auto &producer = *node.tokens[level];
	lock_guard<mutex> producer_lock(producer.producer_lock);
	for (auto &task : tasks) {
		task->token = node.shared_from_this();
	}
	if (q[level].enqueue_bulk(producer.token->queue_token, std::make_move_iterator(tasks.begin()), tasks.size())) {
		tasks_in_queue += tasks.size();
		semaphore.signal(NumericCast<ssize_t>(tasks.size()));
	} else {
		throw InternalException("Could not schedule tasks!");
	}
}

bool ConcurrentQueue::DequeueFromProducer(SchedulerQueryNode &node, shared_ptr<Task> &task) {
	// iterate the levels from highest to lowest priority using the matching producer token
	for (idx_t level = 0; level < NUM_PRIORITY_LEVELS; level++) {
		auto &producer = *node.tokens[level];
		lock_guard<mutex> producer_lock(producer.producer_lock);
		if (q[level].try_dequeue_from_producer(producer.token->queue_token, task)) {
			--tasks_in_queue;
			return true;
		}
	}
	return false;
}

bool ConcurrentQueue::Dequeue(shared_ptr<Task> &task) {
	// iterate the levels from highest to lowest priority
	for (idx_t level = 0; level < NUM_PRIORITY_LEVELS; level++) {
		if (q[level].try_dequeue(task)) {
			--tasks_in_queue;
			return true;
		}
	}
	return false;
}

idx_t ConcurrentQueue::GetTasksInQueue() const {
	return tasks_in_queue;
}
idx_t ConcurrentQueue::GetApproxSize() const {
	idx_t size = 0;
	for (idx_t level = 0; level < NUM_PRIORITY_LEVELS; level++) {
		size += q[level].size_approx();
	}
	return size;
}
idx_t ConcurrentQueue::GetProducerCount() const {
	idx_t count = 0;
	for (idx_t level = 0; level < NUM_PRIORITY_LEVELS; level++) {
		count += q[level].size_producers_approx();
	}
	return count;
}

idx_t ConcurrentQueue::GetTaskCountForProducer(SchedulerQueryNode &node) const {
	idx_t count = 0;
	for (idx_t level = 0; level < NUM_PRIORITY_LEVELS; level++) {
		auto &producer = *node.tokens[level];
		lock_guard<mutex> producer_lock(producer.producer_lock);
		count += q[level].size_producer_approx(producer.token->queue_token);
	}
	return count;
}

#else
struct ConcurrentQueue {
	reference_map_t<QueueProducerToken, std::queue<shared_ptr<Task>>> q[NUM_PRIORITY_LEVELS];
	mutable mutex qlock;

	void Enqueue(SchedulerQueryNode &node, shared_ptr<Task> task);
	void EnqueueBulk(SchedulerQueryNode &node, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(SchedulerQueryNode &node, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(SchedulerQueryNode &node) const;
};

void ConcurrentQueue::Enqueue(SchedulerQueryNode &node, shared_ptr<Task> task) {
	lock_guard<mutex> lock(qlock);
	const auto level = node.ComputeLevel();
	auto &producer = *node.tokens[level];
	task->token = node.shared_from_this();
	q[level][std::ref(*producer.token)].push(std::move(task));
}

void ConcurrentQueue::EnqueueBulk(SchedulerQueryNode &node, vector<shared_ptr<Task>> &tasks) {
	lock_guard<mutex> lock(qlock);
	const auto level = node.ComputeLevel();
	auto &producer = *node.tokens[level];
	for (auto &task : tasks) {
		task->token = node.shared_from_this();
		q[level][std::ref(*producer.token)].push(std::move(task));
	}
}

bool ConcurrentQueue::DequeueFromProducer(SchedulerQueryNode &node, shared_ptr<Task> &task) {
	lock_guard<mutex> lock(qlock);
	// iterate the levels from highest to lowest priority using the matching producer token
	for (idx_t level = 0; level < NUM_PRIORITY_LEVELS; level++) {
		auto &producer = *node.tokens[level];
		const auto it = q[level].find(std::ref(*producer.token));
		if (it == q[level].end() || it->second.empty()) {
			continue;
		}
		task = std::move(it->second.front());
		it->second.pop();
		return true;
	}
	return false;
}

bool ConcurrentQueue::Dequeue(shared_ptr<Task> &task) {
	throw InternalException("Global dequeue not supported for no threads queue");
}

idx_t ConcurrentQueue::GetTasksInQueue() const {
	lock_guard<mutex> lock(qlock);
	idx_t task_count = 0;
	for (idx_t level = 0; level < NUM_PRIORITY_LEVELS; level++) {
		for (auto &producer : q[level]) {
			task_count += producer.second.size();
		}
	}
	return task_count;
}

idx_t ConcurrentQueue::GetApproxSize() const {
	return GetTasksInQueue();
}

idx_t ConcurrentQueue::GetProducerCount() const {
	lock_guard<mutex> lock(qlock);
	idx_t count = 0;
	for (idx_t level = 0; level < NUM_PRIORITY_LEVELS; level++) {
		count += q[level].size();
	}
	return count;
}

idx_t ConcurrentQueue::GetTaskCountForProducer(SchedulerQueryNode &node) const {
	lock_guard<mutex> lock(qlock);
	idx_t count = 0;
	for (idx_t level = 0; level < NUM_PRIORITY_LEVELS; level++) {
		auto &producer = *node.tokens[level];
		const auto it = q[level].find(std::ref(*producer.token));
		if (it != q[level].end()) {
			count += it->second.size();
		}
	}
	return count;
}

struct QueueProducerToken {
	QueueProducerToken(ConcurrentQueue &queue, idx_t level) : queue(&queue), level(level) {
	}

	~QueueProducerToken() {
		lock_guard<mutex> lock(queue->qlock);
		queue->q[level].erase(*this);
	}

private:
	ConcurrentQueue *queue;
	idx_t level;
};
#endif

ProducerToken::ProducerToken(TaskScheduler &scheduler, unique_ptr<QueueProducerToken> token)
    : scheduler(scheduler), token(std::move(token)) {
}

ProducerToken::~ProducerToken() {
}

SchedulerQueryNode::SchedulerQueryNode(TaskScheduler &scheduler, vector<unique_ptr<ProducerToken>> tokens)
    : scheduler(scheduler), tokens(std::move(tokens)), cpu_time_used(0) {
}

SchedulerQueryNode::~SchedulerQueryNode() {
}

void SchedulerQueryNode::AddCpuTime(int64_t usecs) {
	if (usecs > 0) {
		cpu_time_used += usecs;
	}
}

idx_t SchedulerQueryNode::ComputeLevel() const {
	const auto used = cpu_time_used.load();
	idx_t level = 0;
	for (; level + 1 < NUM_PRIORITY_LEVELS; level++) {
		const auto quantum = DEMOTION_QUANTUM_USECS[level];
		if (quantum < 0 || used < quantum) {
			// this level never demotes further, or we have not used enough CPU time to be demoted yet
			break;
		}
	}
	return level;
}

#ifndef DUCKDB_NO_THREADS
//! Returns the CPU time (in microseconds) consumed by the calling thread so far. Used to attribute CPU time to
//! the query that owns the currently executing task. Falls back to wall-clock time if no thread CPU clock exists.
static int64_t GetThreadCpuTimeUsecs() {
#if defined(_WIN32)
	FILETIME creation_time, exit_time, kernel_time, user_time;
	if (GetThreadTimes(GetCurrentThread(), &creation_time, &exit_time, &kernel_time, &user_time)) {
		ULARGE_INTEGER kt, ut;
		kt.LowPart = kernel_time.dwLowDateTime;
		kt.HighPart = kernel_time.dwHighDateTime;
		ut.LowPart = user_time.dwLowDateTime;
		ut.HighPart = user_time.dwHighDateTime;
		// FILETIME is expressed in 100-nanosecond units
		return NumericCast<int64_t>((kt.QuadPart + ut.QuadPart) / 10);
	}
	return 0;
#elif defined(CLOCK_THREAD_CPUTIME_ID)
	struct timespec ts;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0) {
		return static_cast<int64_t>(ts.tv_sec) * 1000000 + ts.tv_nsec / 1000;
	}
	return 0;
#else
	// no per-thread CPU clock available - fall back to wall-clock time
	return std::chrono::duration_cast<std::chrono::microseconds>(
	           std::chrono::steady_clock::now().time_since_epoch())
	    .count();
#endif
}
#endif

TaskScheduler::TaskScheduler(DatabaseInstance &db)
    : db(db), queue(make_uniq<ConcurrentQueue>()),
      allocator_flush_threshold(db.config.options.allocator_flush_threshold),
      allocator_background_threads(Settings::Get<AllocatorBackgroundThreadsSetting>(db)), requested_thread_count(0),
      current_thread_count(1) {
	SetAllocatorBackgroundThreads(allocator_background_threads);
}

TaskScheduler::~TaskScheduler() {
#ifndef DUCKDB_NO_THREADS
	try {
		RelaunchThreadsInternal(0, true);
	} catch (...) {
		// nothing we can do in the destructor if this fails
	}
#endif
}

TaskScheduler &TaskScheduler::GetScheduler(ClientContext &context) {
	return TaskScheduler::GetScheduler(DatabaseInstance::GetDatabase(context));
}

TaskScheduler &TaskScheduler::GetScheduler(DatabaseInstance &db) {
	return db.GetScheduler();
}

shared_ptr<SchedulerQueryNode> TaskScheduler::CreateQueryNode() {
	// create one producer token per priority level, each bound to the matching level's queue
	vector<unique_ptr<ProducerToken>> tokens;
	tokens.reserve(NUM_PRIORITY_LEVELS);
	for (idx_t level = 0; level < NUM_PRIORITY_LEVELS; level++) {
		auto queue_token = make_uniq<QueueProducerToken>(*queue, level);
		tokens.push_back(make_uniq<ProducerToken>(*this, std::move(queue_token)));
	}
	return make_shared_ptr<SchedulerQueryNode>(*this, std::move(tokens));
}

void TaskScheduler::ScheduleTask(SchedulerQueryNode &node, shared_ptr<Task> task) {
	// Enqueue a task for the given query node and signal any sleeping threads
	queue->Enqueue(node, std::move(task));
}

void TaskScheduler::ScheduleTasks(SchedulerQueryNode &node, vector<shared_ptr<Task>> &tasks) {
	queue->EnqueueBulk(node, tasks);
}

bool TaskScheduler::GetTaskFromProducer(SchedulerQueryNode &node, shared_ptr<Task> &task) {
	return queue->DequeueFromProducer(node, task);
}

void TaskScheduler::ExecuteForever(atomic<bool> *marker) {
#ifndef DUCKDB_NO_THREADS
	static constexpr const int64_t INITIAL_FLUSH_WAIT = 500000; // initial wait time of 0.5s (in mus) before flushing

	const auto &block_allocator = BlockAllocator::Get(db);
	const auto &config = DBConfig::GetConfig(db);

	shared_ptr<Task> task;
	// loop until the marker is set to false
	while (*marker) {
		if (!block_allocator.SupportsFlush()) {
			// allocator can't flush, just start an untimed wait
			queue->semaphore.wait();
		} else if (!queue->semaphore.wait(INITIAL_FLUSH_WAIT)) {
			// allocator can flush, we flush this threads outstanding allocations after it was idle for 0.5s
			block_allocator.ThreadFlush(allocator_background_threads, allocator_flush_threshold,
			                            NumericCast<idx_t>(requested_thread_count.load()));
			auto decay_delay = Allocator::DecayDelay();
			if (!decay_delay.IsValid()) {
				// no decay delay specified - just wait
				queue->semaphore.wait();
			} else {
				if (!queue->semaphore.wait(UnsafeNumericCast<int64_t>(decay_delay.GetIndex()) * 1000000 -
				                           INITIAL_FLUSH_WAIT)) {
					// in total, the thread was idle for the entire decay delay (note: seconds converted to mus)
					// mark it as idle and start an untimed wait
					Allocator::ThreadIdle();
					queue->semaphore.wait();
				}
			}
		}
		if (queue->Dequeue(task)) {
			auto process_mode = TaskExecutionMode::PROCESS_ALL;
			if (Settings::Get<SchedulerProcessPartialSetting>(config)) {
				process_mode = TaskExecutionMode::PROCESS_PARTIAL;
			}
			const auto cpu_start = GetThreadCpuTimeUsecs();
			auto execute_result = task->Execute(process_mode);
			// attribute the CPU time spent executing the task to the owning query so that future tasks of
			// this query can be demoted to a lower-priority level. Must happen before a potential reschedule.
			if (task->token) {
				task->token->AddCpuTime(GetThreadCpuTimeUsecs() - cpu_start);
			}

			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED: {
				// task is not finished - reschedule immediately
				auto &token = *task->token;
				queue->Enqueue(token, std::move(task));
				break;
			}
			case TaskExecutionResult::TASK_BLOCKED:
				task->Deschedule();
				task.reset();
				break;
			}
		} else if (queue->GetTasksInQueue() > 0) {
			// failed to dequeue but there are still tasks remaining - signal again to retry
			queue->semaphore.signal(1);
		}
	}
	// this thread will exit, flush all of its outstanding allocations
	if (block_allocator.SupportsFlush()) {
		block_allocator.ThreadFlush(allocator_background_threads, 0, NumericCast<idx_t>(requested_thread_count.load()));
		Allocator::ThreadIdle();
	}
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

idx_t TaskScheduler::ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) {
#ifndef DUCKDB_NO_THREADS
	idx_t completed_tasks = 0;
	// loop until the marker is set to false
	while (*marker && completed_tasks < max_tasks) {
		shared_ptr<Task> task;
		if (!queue->Dequeue(task)) {
			return completed_tasks;
		}
		const auto cpu_start = GetThreadCpuTimeUsecs();
		auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
		if (task->token) {
			task->token->AddCpuTime(GetThreadCpuTimeUsecs() - cpu_start);
		}

		switch (execute_result) {
		case TaskExecutionResult::TASK_FINISHED:
		case TaskExecutionResult::TASK_ERROR:
			task.reset();
			completed_tasks++;
			break;
		case TaskExecutionResult::TASK_NOT_FINISHED:
			throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
		case TaskExecutionResult::TASK_BLOCKED:
			task->Deschedule();
			task.reset();
			break;
		}
	}
	return completed_tasks;
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

void TaskScheduler::ExecuteTasks(idx_t max_tasks) {
#ifndef DUCKDB_NO_THREADS
	shared_ptr<Task> task;
	for (idx_t i = 0; i < max_tasks; i++) {
		queue->semaphore.wait(TASK_TIMEOUT_USECS);
		if (!queue->Dequeue(task)) {
			return;
		}
		try {
			const auto cpu_start = GetThreadCpuTimeUsecs();
			auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
			if (task->token) {
				task->token->AddCpuTime(GetThreadCpuTimeUsecs() - cpu_start);
			}
			switch (execute_result) {
			case TaskExecutionResult::TASK_FINISHED:
			case TaskExecutionResult::TASK_ERROR:
				task.reset();
				break;
			case TaskExecutionResult::TASK_NOT_FINISHED:
				throw InternalException("Task should not return TASK_NOT_FINISHED in PROCESS_ALL mode");
			case TaskExecutionResult::TASK_BLOCKED:
				task->Deschedule();
				task.reset();
				break;
			}
		} catch (...) {
			return;
		}
	}
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

#ifndef DUCKDB_NO_THREADS
static void ThreadExecuteTasks(TaskScheduler *scheduler, atomic<bool> *marker) {
	scheduler->ExecuteForever(marker);
}
#endif

int32_t TaskScheduler::NumberOfThreads() {
	return current_thread_count.load();
}

idx_t TaskScheduler::GetNumberOfTasks() const {
	return queue->GetTasksInQueue();
}

idx_t TaskScheduler::GetProducerCount() const {
	return queue->GetProducerCount();
}

idx_t TaskScheduler::GetTaskCountForProducer(SchedulerQueryNode &node) const {
	return queue->GetTaskCountForProducer(node);
}

void TaskScheduler::SetThreads(idx_t total_threads, idx_t external_threads) {
	if (total_threads == 0) {
		throw SyntaxException("Number of threads must be positive!");
	}
#ifndef DUCKDB_NO_THREADS
	if (total_threads < external_threads) {
		throw SyntaxException("Number of threads can't be smaller than number of external threads!");
	}
#else
	if (total_threads != external_threads) {
		throw NotImplementedException(
		    "DuckDB was compiled without threads! Setting total_threads != external_threads is not allowed.");
	}
#endif
	requested_thread_count = NumericCast<int32_t>(total_threads - external_threads);
}

void TaskScheduler::SetAllocatorFlushTreshold(idx_t threshold) {
	allocator_flush_threshold = threshold;
}

void TaskScheduler::SetAllocatorBackgroundThreads(bool enable) {
	allocator_background_threads = enable;
	Allocator::SetBackgroundThreads(enable);
}

void TaskScheduler::Signal(idx_t n) {
#ifndef DUCKDB_NO_THREADS
	typedef std::make_signed<std::size_t>::type ssize_t;
	queue->semaphore.signal(NumericCast<ssize_t>(n));
#endif
}

void TaskScheduler::YieldThread() {
#ifndef DUCKDB_NO_THREADS
	std::this_thread::yield();
#endif
}

idx_t TaskScheduler::GetEstimatedCPUId() {
#if defined(__EMSCRIPTEN__)
	// FIXME: Wasm + multithreads can likely be implemented as
	//   return return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
	return 0;
#else
	// this code comes from jemalloc
#if defined(_WIN32)
	return (idx_t)GetCurrentProcessorNumber();
#elif defined(_GNU_SOURCE)
	auto cpu = sched_getcpu();
	if (cpu < 0) {
#ifndef DUCKDB_NO_THREADS
		// fallback to thread id
		return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
#else

		return 0;
#endif
	}
	return (idx_t)cpu;
#elif defined(__aarch64__) && defined(__APPLE__)
	/* Other oses most likely use tpidr_el0 instead */
	uintptr_t c;
	asm volatile("mrs %x0, tpidrro_el0" : "=r"(c)::"memory");
	return (idx_t)(c & ((1 << 3) - 1));
#else
#ifndef DUCKDB_NO_THREADS
	// fallback to thread id
	return (idx_t)std::hash<std::thread::id>()(std::this_thread::get_id());
#else
	return 0;
#endif
#endif
#endif
}

void TaskScheduler::RelaunchThreads() {
	lock_guard<mutex> t(thread_lock);
	auto n = requested_thread_count.load();
	RelaunchThreadsInternal(n, false);
}

#ifndef DUCKDB_NO_THREADS
static void SetThreadAffinity(thread &thread, const int &cpu_id) {
#if defined(__GLIBC__)
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset);
	CPU_SET(cpu_id, &cpuset);

	// note that we don't care about the return value here
	// if we did not manage to set affinity, the thread just does not have affinity, which is OK
	pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
#endif
}
#endif

void TaskScheduler::RelaunchThreadsInternal(int32_t n, bool destroy) {
#ifndef DUCKDB_NO_THREADS
	auto &config = DBConfig::GetConfig(db);
	auto new_thread_count = NumericCast<idx_t>(n);

	idx_t external_threads = 0;
	ThreadPinMode pin_thread_mode = ThreadPinMode::AUTO;
	if (!destroy) {
		// If we are destroying, i.e., calling ~TaskScheduler, we don't want to read the settings
		external_threads = Settings::Get<ExternalThreadsSetting>(config);
		pin_thread_mode = Settings::Get<PinThreadsSetting>(db);
	}

	if (threads.size() == new_thread_count) {
		current_thread_count = NumericCast<int32_t>(threads.size() + external_threads);
		return;
	}
	if (threads.size() > new_thread_count) {
		// we are reducing the number of threads: clear all threads first
		for (idx_t i = 0; i < threads.size(); i++) {
			*markers[i] = false;
		}
		Signal(threads.size());
		// now join the threads to ensure they are fully stopped before erasing them
		for (idx_t i = 0; i < threads.size(); i++) {
			threads[i]->internal_thread->join();
		}
		// erase the threads/markers
		threads.clear();
		markers.clear();
	}
	if (threads.size() < new_thread_count) {
		// we are increasing the number of threads: launch them and run tasks on them
		idx_t create_new_threads = new_thread_count - threads.size();

		// Whether to pin threads to cores
		static constexpr idx_t THREAD_PIN_THRESHOLD = 64;
		const auto pin_threads =
		    pin_thread_mode == ThreadPinMode::ON ||
		    (pin_thread_mode == ThreadPinMode::AUTO && std::thread::hardware_concurrency() > THREAD_PIN_THRESHOLD);
		for (idx_t i = 0; i < create_new_threads; i++) {
			// launch a thread and assign it a cancellation marker
			auto marker = unique_ptr<atomic<bool>>(new atomic<bool>(true));
			unique_ptr<thread> worker_thread;
			try {
				worker_thread = make_uniq<thread>(ThreadExecuteTasks, this, marker.get());
				if (pin_threads) {
					SetThreadAffinity(*worker_thread, NumericCast<int>(threads.size()));
				}
			} catch (std::exception &ex) {
				// thread constructor failed - this can happen when the system has too many threads allocated
				// in this case we cannot allocate more threads - stop launching them
				break;
			}
			auto thread_wrapper = make_uniq<SchedulerThread>(std::move(worker_thread));

			threads.push_back(std::move(thread_wrapper));
			markers.push_back(std::move(marker));
		}
	}
	current_thread_count = NumericCast<int32_t>(threads.size() + external_threads);
	BlockAllocator::Get(db).FlushAll();
#endif
}

} // namespace duckdb
