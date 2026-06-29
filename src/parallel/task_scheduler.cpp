#include "duckdb/parallel/task_scheduler.hpp"

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/block_allocator.hpp"

#include <algorithm>
#include <cstdlib>
#ifndef DUCKDB_NO_THREADS
#include "concurrentqueue.h"
#include "duckdb/common/thread.hpp"
#include "lightweightsemaphore.h"

#include <condition_variable>
#include <thread>
#else
#include <queue>
#endif

namespace {
//! Read an unsigned integer environment variable, returning `fallback` if unset/invalid.
duckdb::idx_t ReadEnvIdx(const char *name, duckdb::idx_t fallback) {
	const char *value = std::getenv(name); // NOLINT: prototype knob
	if (!value || value[0] == '\0') {
		return fallback;
	}
	char *end = nullptr;
	auto parsed = std::strtoull(value, &end, 10);
	if (end == value) {
		return fallback;
	}
	return static_cast<duckdb::idx_t>(parsed);
}

bool ReadEnvBool(const char *name) {
	const char *value = std::getenv(name); // NOLINT: prototype knob
	return value && (value[0] == '1' || value[0] == 't' || value[0] == 'T' || value[0] == 'y' || value[0] == 'Y');
}
} // namespace

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

//! Number of MLFQ priority bands (band 0 == highest priority). Hardcoded prototype value.
static constexpr idx_t MLFQ_NUM_BANDS = 3;

#ifndef DUCKDB_NO_THREADS
//! Cumulative attained-service thresholds (microseconds): a query is in band 0 while its total CPU service is below
//! BAND0_MAX_US, band 1 while below BAND1_MAX_US, and band 2 beyond. Derived from 5000ms / 22000ms quanta.
static constexpr int64_t MLFQ_BAND0_MAX_US = 5000000;  // 5000 ms
static constexpr int64_t MLFQ_BAND1_MAX_US = 22000000; // 22000 ms

//! Map cumulative attained CPU service (in microseconds) to an MLFQ band.
static idx_t ComputeBand(int64_t attained_service_usecs) {
	if (attained_service_usecs < MLFQ_BAND0_MAX_US) {
		return 0;
	}
	if (attained_service_usecs < MLFQ_BAND1_MAX_US) {
		return 1;
	}
	return 2;
}
#endif

struct SchedulerThread {
#ifndef DUCKDB_NO_THREADS
	explicit SchedulerThread(unique_ptr<thread> thread_p) : internal_thread(std::move(thread_p)) {
	}

	unique_ptr<thread> internal_thread;
	//! Affinity mailbox: the query this worker is currently attached to (written by the dispatcher, read by the
	//! worker). Guarded by mailbox_lock; the worker lock()s the weak_ptr for the duration of an attached dequeue.
	mutex mailbox_lock;
	weak_ptr<ProducerToken> assigned;
#endif
};

#ifndef DUCKDB_NO_THREADS
//! Dispatcher synchronization state, kept out of the header to avoid leaking <condition_variable>/<thread>.
struct DispatcherState {
	mutex lock;
	std::condition_variable cv;
	atomic<bool> active {false};
	atomic<bool> wake {false};
	unique_ptr<thread> worker;
};
#else
struct DispatcherState {};
#endif

#ifndef DUCKDB_NO_THREADS
typedef duckdb_moodycamel::ConcurrentQueue<shared_ptr<Task>> concurrent_queue_t;
typedef duckdb_moodycamel::LightweightSemaphore lightweight_semaphore_t;

struct ConcurrentQueue {
	ConcurrentQueue() : tasks_in_queue(0) {
	}

	lightweight_semaphore_t semaphore;
	//! When false (affinity scheduler disabled) the queue behaves exactly like upstream: it uses a single band (q[0])
	//! for both enqueue and dequeue, so default builds are unaffected.
	bool mlfq_enabled = false;

	void Enqueue(ProducerToken &token, shared_ptr<Task> task);
	void EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(ProducerToken &token) const;
	//! Approx number of tasks queued in a given band across all producers (dispatcher heuristic/instrumentation)
	idx_t GetBandTaskCount(idx_t band) const;
	//! Approx number of tasks a single producer currently has queued at a specific band
	idx_t GetProducerTaskCountAtBand(ProducerToken &token, idx_t band) const;
	//! Number of demotions observed (a producer's band increased on enqueue)
	idx_t GetDemotions() const {
		return demotions;
	}

private:
	//! Lazily create (caller must hold token.producer_lock) and return the producer's moodycamel token for `band`.
	QueueProducerToken &GetOrCreateLevelToken(ProducerToken &token, idx_t band);

	concurrent_queue_t q[MLFQ_NUM_BANDS];
	atomic<idx_t> tasks_in_queue;
	atomic<idx_t> demotions {0};
};

struct QueueProducerToken {
	explicit QueueProducerToken(concurrent_queue_t &level_queue) : queue_token(level_queue) {
	}

	duckdb_moodycamel::ProducerToken queue_token;
};

QueueProducerToken &ConcurrentQueue::GetOrCreateLevelToken(ProducerToken &token, idx_t band) {
	if (!token.level_tokens[band]) {
		token.level_tokens[band] = make_uniq<QueueProducerToken>(q[band]);
	}
	return *token.level_tokens[band];
}

void ConcurrentQueue::Enqueue(ProducerToken &token, shared_ptr<Task> task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	task->token = token;
	idx_t band = 0;
	if (mlfq_enabled) {
		band = ComputeBand(token.attained_service_usecs.load());
		if (band > token.band.load()) {
			++demotions;
		}
	}
	token.band.store(band);
	auto &level_token = GetOrCreateLevelToken(token, band);
	if (q[band].enqueue(level_token.queue_token, std::move(task))) {
		++tasks_in_queue;
		semaphore.signal();
	} else {
		throw InternalException("Could not schedule task!");
	}
}

void ConcurrentQueue::EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks) {
	typedef std::make_signed<std::size_t>::type ssize_t;
	lock_guard<mutex> producer_lock(token.producer_lock);
	for (auto &task : tasks) {
		task->token = token;
	}
	idx_t band = 0;
	if (mlfq_enabled) {
		band = ComputeBand(token.attained_service_usecs.load());
		if (band > token.band.load()) {
			++demotions;
		}
	}
	token.band.store(band);
	auto &level_token = GetOrCreateLevelToken(token, band);
	if (q[band].enqueue_bulk(level_token.queue_token, std::make_move_iterator(tasks.begin()), tasks.size())) {
		tasks_in_queue += tasks.size();
		semaphore.signal(NumericCast<ssize_t>(tasks.size()));
	} else {
		throw InternalException("Could not schedule tasks!");
	}
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	lock_guard<mutex> producer_lock(token.producer_lock);
	idx_t band = token.band.load();
	if (band >= MLFQ_NUM_BANDS || !token.level_tokens[band]) {
		return false;
	}
	if (!q[band].try_dequeue_from_producer(token.level_tokens[band]->queue_token, task)) {
		return false;
	}
	--tasks_in_queue;
	return true;
}

bool ConcurrentQueue::Dequeue(shared_ptr<Task> &task) {
	if (!mlfq_enabled) {
		if (!q[0].try_dequeue(task)) {
			return false;
		}
		--tasks_in_queue;
		return true;
	}
	// absolute priority: always drain higher bands (lower index) before lower ones
	for (idx_t band = 0; band < MLFQ_NUM_BANDS; band++) {
		if (q[band].try_dequeue(task)) {
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
	idx_t total = 0;
	for (idx_t band = 0; band < MLFQ_NUM_BANDS; band++) {
		total += q[band].size_approx();
	}
	return total;
}
idx_t ConcurrentQueue::GetProducerCount() const {
	idx_t total = 0;
	for (idx_t band = 0; band < MLFQ_NUM_BANDS; band++) {
		total += q[band].size_producers_approx();
	}
	return total;
}

idx_t ConcurrentQueue::GetTaskCountForProducer(ProducerToken &token) const {
	lock_guard<mutex> producer_lock(token.producer_lock);
	idx_t total = 0;
	for (idx_t band = 0; band < MLFQ_NUM_BANDS; band++) {
		if (token.level_tokens[band]) {
			total += q[band].size_producer_approx(token.level_tokens[band]->queue_token);
		}
	}
	return total;
}

idx_t ConcurrentQueue::GetBandTaskCount(idx_t band) const {
	if (band >= MLFQ_NUM_BANDS) {
		return 0;
	}
	return q[band].size_approx();
}

idx_t ConcurrentQueue::GetProducerTaskCountAtBand(ProducerToken &token, idx_t band) const {
	lock_guard<mutex> producer_lock(token.producer_lock);
	if (band >= MLFQ_NUM_BANDS || !token.level_tokens[band]) {
		return 0;
	}
	return q[band].size_producer_approx(token.level_tokens[band]->queue_token);
}

#else
struct ConcurrentQueue {
	reference_map_t<QueueProducerToken, std::queue<shared_ptr<Task>>> q;
	mutable mutex qlock;

	void Enqueue(ProducerToken &token, shared_ptr<Task> task);
	void EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks);
	bool DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task);
	bool Dequeue(shared_ptr<Task> &task);
	idx_t GetTasksInQueue() const;
	idx_t GetApproxSize() const;
	idx_t GetProducerCount() const;
	idx_t GetTaskCountForProducer(ProducerToken &token) const;
};

struct QueueProducerToken {
	explicit QueueProducerToken(ConcurrentQueue &queue) : queue(&queue) {
	}

	~QueueProducerToken() {
		lock_guard<mutex> lock(queue->qlock);
		queue->q.erase(*this);
	}

private:
	ConcurrentQueue *queue;
};

//! In the no-threads build the MLFQ collapses to a single band: only level_tokens[0] is ever used.
static QueueProducerToken &GetSingleLevelToken(ConcurrentQueue &queue, ProducerToken &token) {
	if (!token.level_tokens[0]) {
		token.level_tokens[0] = make_uniq<QueueProducerToken>(queue);
	}
	return *token.level_tokens[0];
}

void ConcurrentQueue::Enqueue(ProducerToken &token, shared_ptr<Task> task) {
	lock_guard<mutex> lock(qlock);
	task->token = token;
	q[std::ref(GetSingleLevelToken(*this, token))].push(std::move(task));
}

void ConcurrentQueue::EnqueueBulk(ProducerToken &token, vector<shared_ptr<Task>> &tasks) {
	lock_guard<mutex> lock(qlock);
	auto &level_token = GetSingleLevelToken(*this, token);
	for (auto &task : tasks) {
		task->token = token;
		q[std::ref(level_token)].push(std::move(task));
	}
}

bool ConcurrentQueue::DequeueFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	lock_guard<mutex> lock(qlock);
	if (!token.level_tokens[0]) {
		return false;
	}
	const auto it = q.find(std::ref(*token.level_tokens[0]));
	if (it == q.end() || it->second.empty()) {
		return false;
	}

	task = std::move(it->second.front());
	it->second.pop();

	return true;
}

bool ConcurrentQueue::Dequeue(shared_ptr<Task> &task) {
	throw InternalException("Global dequeue not supported for no threads queue");
}

idx_t ConcurrentQueue::GetTasksInQueue() const {
	lock_guard<mutex> lock(qlock);
	idx_t task_count = 0;
	for (auto &producer : q) {
		task_count += producer.second.size();
	}
	return task_count;
}

idx_t ConcurrentQueue::GetApproxSize() const {
	return GetTasksInQueue();
}

idx_t ConcurrentQueue::GetProducerCount() const {
	lock_guard<mutex> lock(qlock);
	return q.size();
}

idx_t ConcurrentQueue::GetTaskCountForProducer(ProducerToken &token) const {
	lock_guard<mutex> lock(qlock);
	if (!token.level_tokens[0]) {
		return 0;
	}
	const auto it = q.find(std::ref(*token.level_tokens[0]));
	if (it == q.end()) {
		return 0;
	}
	return it->second.size();
}
#endif

ProducerToken::ProducerToken(TaskScheduler &scheduler) : scheduler(scheduler) {
	level_tokens.resize(MLFQ_NUM_BANDS);
}

ProducerToken::~ProducerToken() {
	scheduler.DeregisterProducer(*this);
}

TaskScheduler::TaskScheduler(DatabaseInstance &db)
    : db(db), queue(make_uniq<ConcurrentQueue>()),
      allocator_flush_threshold(db.config.options.allocator_flush_threshold),
      allocator_background_threads(Settings::Get<AllocatorBackgroundThreadsSetting>(db)), requested_thread_count(0),
      current_thread_count(1) {
	SetAllocatorBackgroundThreads(allocator_background_threads);
	// read affinity-scheduler knobs from the environment (prototype configuration)
	affinity_enabled = ReadEnvBool("DUCKDB_AFFINITY_SCHED");
	affinity_k = MaxValue<idx_t>(ReadEnvIdx("DUCKDB_AFFINITY_K", 8), 1);
	mlfq_num_bands = MaxValue<idx_t>(ReadEnvIdx("DUCKDB_MLFQ_BANDS", 3), 1);
	mlfq_threshold_usecs = NumericCast<int64_t>(ReadEnvIdx("DUCKDB_MLFQ_THRESHOLD_US", 100000));
	affinity_debug = ReadEnvBool("DUCKDB_AFFINITY_DEBUG");
	query_max_threads = ReadEnvIdx("DUCKDB_QUERY_MAX_THREADS", 0);
#ifdef DUCKDB_NO_THREADS
	// no background workers exist without threads, so affinity scheduling has nothing to manage
	affinity_enabled = false;
#else
	// when affinity is off the queue stays single-band (byte-for-byte upstream behavior); when on it routes by band
	queue->mlfq_enabled = affinity_enabled;
#endif
	dispatcher = make_uniq<DispatcherState>();
	// The dispatcher runs for the scheduler's lifetime. It coordinates with thread-pool changes purely through
	// thread_lock (taken briefly in RebalanceAffinity), so it does not need to be torn down on every RelaunchThreads.
	StartDispatcher();
}

TaskScheduler::~TaskScheduler() {
#ifndef DUCKDB_NO_THREADS
	try {
		StopDispatcher();
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

shared_ptr<ProducerToken> TaskScheduler::CreateProducer() {
	// per-level moodycamel tokens are created lazily on first enqueue at each band
	auto token = make_shared_ptr<ProducerToken>(*this);
	if (affinity_enabled) {
		{
			lock_guard<mutex> guard(registry_lock);
			token->arrival_seq = arrival_counter++;
			producers[token.get()] = weak_ptr<ProducerToken>(token);
		}
		NotifyDispatcher();
	}
	return token;
}

void TaskScheduler::ScheduleTask(ProducerToken &token, shared_ptr<Task> task) {
	// Enqueue a task for the given producer token and signal any sleeping threads
	queue->Enqueue(token, std::move(task));
}

void TaskScheduler::ScheduleTasks(ProducerToken &producer, vector<shared_ptr<Task>> &tasks) {
	queue->EnqueueBulk(producer, tasks);
}

bool TaskScheduler::GetTaskFromProducer(ProducerToken &token, shared_ptr<Task> &task) {
	return queue->DequeueFromProducer(token, task);
}

void TaskScheduler::DeregisterProducer(ProducerToken &token) {
	if (!affinity_enabled) {
		return;
	}
	{
		lock_guard<mutex> guard(registry_lock);
		producers.erase(&token);
	}
	NotifyDispatcher();
}

void TaskScheduler::NotifyDispatcher() {
#ifndef DUCKDB_NO_THREADS
	if (!dispatcher) {
		return;
	}
	{
		lock_guard<mutex> guard(dispatcher->lock);
		dispatcher->wake = true;
	}
	dispatcher->cv.notify_one();
#endif
}

void TaskScheduler::RebalanceAffinity() {
#ifndef DUCKDB_NO_THREADS
	// snapshot the live producers (queries) under the registry lock
	vector<shared_ptr<ProducerToken>> live;
	{
		lock_guard<mutex> guard(registry_lock);
		live.reserve(producers.size());
		for (auto &entry : producers) {
			if (auto sp = entry.second.lock()) {
				live.push_back(std::move(sp));
			}
		}
	}

	lock_guard<mutex> tguard(thread_lock);
	const idx_t worker_count = threads.size();
	if (worker_count == 0) {
		return;
	}

	// Bands are maintained at enqueue time (ComputeBand); here we only read the cached band. Collect the producers
	// that currently have queued work in a demoted band (1+) so we can decide whether to rescue them.
	vector<shared_ptr<ProducerToken>> lower;
	for (auto &q : live) {
		idx_t band = q->band.load();
		if (band == 0) {
			continue;
		}
		if (queue->GetProducerTaskCountAtBand(*q, band) > 0) {
			lower.push_back(q);
		}
	}

	// Situational affinity: top-down scanning already work-conserves, so only divert workers when band 0 cannot keep
	// the pool busy AND several demoted queries have accumulated. Otherwise leave everyone unattached (the cheap path)
	// so the dispatcher imposes essentially no overhead in the common case.
	const idx_t band0_tasks = queue->GetBandTaskCount(0);
	const bool trigger = (band0_tasks < worker_count) && (lower.size() >= 2);

	vector<weak_ptr<ProducerToken>> assignment(worker_count); // default-constructed == unattached
	if (trigger) {
		// rescue the most-deprioritized queries first (highest band), FIFO within a band
		std::sort(lower.begin(), lower.end(),
		          [](const shared_ptr<ProducerToken> &a, const shared_ptr<ProducerToken> &b) {
			          auto ba = a->band.load();
			          auto bb = b->band.load();
			          if (ba != bb) {
				          return ba > bb;
			          }
			          return a->arrival_seq < b->arrival_seq;
		          });
		// divert at most ~25% of workers and pin them to the trailing slots, so the leading workers keep prioritizing
		// band 0. Each diverted worker is pinned to a distinct deprioritized producer (round-robin) to spread service
		// across many starved queries and avoid cross-producer dequeue contention on a single lower-band sub-queue.
		const idx_t divert = MaxValue<idx_t>(worker_count / 4, 1);
		for (idx_t i = 0; i < divert; i++) {
			idx_t slot_idx = worker_count - 1 - i;
			assignment[slot_idx] = weak_ptr<ProducerToken>(lower[i % lower.size()]);
		}
		stat_diversions++;
	}

	for (idx_t i = 0; i < worker_count; i++) {
		auto &slot = *threads[i];
		lock_guard<mutex> mguard(slot.mailbox_lock);
		// hysteresis: only count a reassignment when the target actually changes
		auto cur = slot.assigned.lock();
		auto next = assignment[i].lock();
		if (cur.get() != next.get()) {
			stat_reassignments++;
		}
		slot.assigned = assignment[i];
	}

	if (affinity_debug) {
		Printer::Print(StringUtil::Format(
		    "[affinity] rebalance: queries=%llu workers=%llu band0_tasks=%llu lower=%llu trigger=%d wakeups=%llu "
		    "diversions=%llu reassignments=%llu demotions=%llu hits=%llu misses=%llu",
		    (unsigned long long)live.size(), (unsigned long long)worker_count, (unsigned long long)band0_tasks,
		    (unsigned long long)lower.size(), (int)trigger, (unsigned long long)stat_dispatcher_wakeups.load(),
		    (unsigned long long)stat_diversions.load(), (unsigned long long)stat_reassignments.load(),
		    (unsigned long long)queue->GetDemotions(), (unsigned long long)stat_attached_hits.load(),
		    (unsigned long long)stat_attached_misses.load()));
	}
#endif
}

void TaskScheduler::DispatcherLoop() {
#ifndef DUCKDB_NO_THREADS
	// Re-evaluation interval while queries are active. The dispatcher sleeps indefinitely when idle (no producers) and
	// is woken by NotifyDispatcher() on arrivals/completions, so it never busy-spins.
	static constexpr const int64_t REEVAL_INTERVAL_MS = 5;
	while (dispatcher->active) {
		{
			std::unique_lock<std::mutex> lk(dispatcher->lock);
			bool has_queries;
			{
				lock_guard<mutex> guard(registry_lock);
				has_queries = !producers.empty();
			}
			if (!dispatcher->wake) {
				if (has_queries) {
					dispatcher->cv.wait_for(lk, std::chrono::milliseconds(REEVAL_INTERVAL_MS));
				} else {
					dispatcher->cv.wait(lk);
				}
			}
			dispatcher->wake = false;
		}
		if (!dispatcher->active) {
			break;
		}
		stat_dispatcher_wakeups++;
		RebalanceAffinity();
	}
#endif
}

void TaskScheduler::StartDispatcher() {
#ifndef DUCKDB_NO_THREADS
	if (!affinity_enabled || !dispatcher || dispatcher->worker) {
		return;
	}
	dispatcher->active = true;
	dispatcher->wake = false;
	try {
		dispatcher->worker = make_uniq<thread>([this]() { DispatcherLoop(); });
	} catch (std::exception &) {
		// could not launch the dispatcher - fall back to plain (unattached) execution
		dispatcher->active = false;
	}
#endif
}

void TaskScheduler::StopDispatcher() {
#ifndef DUCKDB_NO_THREADS
	if (!dispatcher || !dispatcher->worker) {
		return;
	}
	{
		lock_guard<mutex> guard(dispatcher->lock);
		dispatcher->active = false;
		dispatcher->wake = true;
	}
	dispatcher->cv.notify_one();
	dispatcher->worker->join();
	dispatcher->worker.reset();
#endif
}

void TaskScheduler::ExecuteForever(atomic<bool> *marker) {
#ifndef DUCKDB_NO_THREADS
	// no worker slot -> always runs as an unattached worker (plain global-queue execution)
	ExecuteForeverInternal(marker, nullptr);
#else
	throw NotImplementedException("DuckDB was compiled without threads! Background thread loop is not allowed.");
#endif
}

#ifndef DUCKDB_NO_THREADS
void TaskScheduler::ExecuteForeverInternal(atomic<bool> *marker, optional_ptr<SchedulerThread> slot) {
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

		// Affinity: if the dispatcher attached this worker to a query, try that query's producer first. If it has no
		// ready work, fall back to the global queue (the "normal route") so the worker never idles needlessly.
		bool got_task = false;
		if (affinity_enabled && slot) {
			shared_ptr<ProducerToken> attached;
			{
				lock_guard<mutex> mguard(slot->mailbox_lock);
				attached = slot->assigned.lock();
			}
			if (attached) {
				got_task = queue->DequeueFromProducer(*attached, task);
				if (got_task) {
					stat_attached_hits++;
				} else {
					stat_attached_misses++;
				}
			}
		}
		if (!got_task) {
			got_task = queue->Dequeue(task);
		}

		if (got_task) {
			auto process_mode = TaskExecutionMode::PROCESS_ALL;
			// affinity scheduling relies on partial processing so tasks yield, letting us account CPU service and let
			// the dispatcher re-evaluate priority bands at task granularity.
			if (affinity_enabled || Settings::Get<SchedulerProcessPartialSetting>(config)) {
				process_mode = TaskExecutionMode::PROCESS_PARTIAL;
			}

			optional_ptr<ProducerToken> charge_token = task->token;
			auto exec_begin = std::chrono::steady_clock::now();
			auto execute_result = task->Execute(process_mode);
			if (affinity_enabled && charge_token) {
				auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
				                   std::chrono::steady_clock::now() - exec_begin)
				                   .count();
				charge_token->attained_service_usecs.fetch_add(NumericCast<int64_t>(elapsed));
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
}
#endif

idx_t TaskScheduler::ExecuteTasks(atomic<bool> *marker, idx_t max_tasks) {
#ifndef DUCKDB_NO_THREADS
	idx_t completed_tasks = 0;
	// loop until the marker is set to false
	while (*marker && completed_tasks < max_tasks) {
		shared_ptr<Task> task;
		if (!queue->Dequeue(task)) {
			return completed_tasks;
		}
		auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);

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
			auto execute_result = task->Execute(TaskExecutionMode::PROCESS_ALL);
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
static void ThreadExecuteTasks(TaskScheduler *scheduler, atomic<bool> *marker, SchedulerThread *slot) {
	scheduler->ExecuteForeverInternal(marker, slot);
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

idx_t TaskScheduler::GetTaskCountForProducer(ProducerToken &token) const {
	return queue->GetTaskCountForProducer(token);
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
			// create the worker slot (mailbox) first so the worker can safely read it from the start; the thread
			// receives a stable pointer to the slot and the actual thread handle is stored back into it afterwards.
			auto thread_wrapper = make_uniq<SchedulerThread>(unique_ptr<thread>());
			auto *slot = thread_wrapper.get();
			unique_ptr<thread> worker_thread;
			try {
				worker_thread = make_uniq<thread>(ThreadExecuteTasks, this, marker.get(), slot);
				if (pin_threads) {
					SetThreadAffinity(*worker_thread, NumericCast<int>(threads.size()));
				}
			} catch (std::exception &ex) {
				// thread constructor failed - this can happen when the system has too many threads allocated
				// in this case we cannot allocate more threads - stop launching them
				break;
			}
			slot->internal_thread = std::move(worker_thread);

			threads.push_back(std::move(thread_wrapper));
			markers.push_back(std::move(marker));
		}
	}
	current_thread_count = NumericCast<int32_t>(threads.size() + external_threads);
	BlockAllocator::Get(db).FlushAll();
#endif
}

} // namespace duckdb
