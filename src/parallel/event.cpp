#include "duckdb/parallel/event.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

Event::Event(Executor &executor_p)
    : executor(executor_p), finished_tasks(0), total_tasks(0), finished_dependencies(0), total_dependencies(0),
      finished(false) {
}

void Event::CompleteDependency() {
	idx_t current_finished = ++finished_dependencies;
	D_ASSERT(current_finished <= total_dependencies);
	if (current_finished == total_dependencies) {
		// all dependencies have been completed: schedule the event
		D_ASSERT(total_tasks == 0);
		Schedule();
		if (total_tasks == 0) {
			Finish();
		}
	}
}

void Event::Finish() {
	D_ASSERT(!finished);
	FinishEvent();
	finished = true;
	// finished processing the pipeline, now we can schedule pipelines that depend on this pipeline
	for (auto &parent_entry : parents) {
		auto parent = parent_entry.lock();
		if (!parent) { // LCOV_EXCL_START
			continue;
		} // LCOV_EXCL_STOP
		// mark a dependency as completed for each of the parents
		parent->CompleteDependency();
	}
	FinalizeFinish();
}

void Event::AddDependency(Event &event) {
	total_dependencies++;
	event.parents.push_back(weak_ptr<Event>(shared_from_this()));
#ifdef DEBUG
	event.parents_raw.push_back(*this);
#endif
}

const vector<reference<Event>> &Event::GetParentsVerification() const {
	D_ASSERT(parents.size() == parents_raw.size());
	return parents_raw;
}

void Event::FinishTask() {
	D_ASSERT(finished_tasks.load() < total_tasks.load());
	idx_t current_tasks = total_tasks;
	idx_t current_finished = ++finished_tasks;
	D_ASSERT(current_finished <= current_tasks);
	if (current_finished == current_tasks) {
		Finish();
	}
}

ClientContext &Event::GetClientContext() {
	return executor.context;
}

void Event::InsertEvent(shared_ptr<Event> replacement_event) {
	replacement_event->parents = std::move(parents);
#ifdef DEBUG
	replacement_event->parents_raw = std::move(parents_raw);
#endif
	replacement_event->AddDependency(*this);
	executor.AddEvent(std::move(replacement_event));
}

void Event::SetTasks(vector<shared_ptr<Task>> tasks) {
	auto &ts = TaskScheduler::GetScheduler(executor.context);
	D_ASSERT(total_tasks == 0);
	D_ASSERT(!tasks.empty());
	this->total_tasks = tasks.size();
	ts.ScheduleTasks(executor.GetToken(), tasks);
}

void Event::SetTasksInline(vector<shared_ptr<Task>> tasks) {
	// Bounds how deeply sequential tasks may be chained inline on a single native stack before we fall back to the
	// scheduler queue. A chain forms when finishing an inline task triggers a dependent pipeline to be scheduled on
	// the same thread (FinishTask -> CompleteDependency -> Schedule). Without a bound, a long chain of dependent
	// sequential pipelines could overflow the stack.
	static constexpr idx_t MAX_INLINE_DEPTH = 32;
	static thread_local idx_t inline_depth = 0;

	D_ASSERT(total_tasks == 0);
	D_ASSERT(!tasks.empty());
	this->total_tasks = tasks.size();

	auto &node = executor.GetToken();
	if (inline_depth >= MAX_INLINE_DEPTH) {
		// too deep: fall back to normal scheduling to keep the native stack bounded
		auto &ts = TaskScheduler::GetScheduler(executor.context);
		ts.ScheduleTasks(node, tasks);
		return;
	}

	inline_depth++;
	for (auto &task : tasks) {
		// attribute CPU time to this query even though we bypass the queue (keeps LAS priority accounting correct)
		task->token = node;
		// ExecutorTask::Execute never throws (it converts exceptions into TASK_ERROR), so inline_depth is always
		// restored below without needing a try/catch here.
		const auto result = task->Execute(TaskExecutionMode::PROCESS_ALL);
		if (result == TaskExecutionResult::TASK_BLOCKED) {
			// the task interrupted (e.g. waiting on async I/O): hand it to the scheduler so a worker thread resumes
			// it once it is unblocked
			task->Deschedule();
		}
		// TASK_FINISHED / TASK_ERROR: FinishTask() / error propagation already happened inside Execute().
		// TASK_NOT_FINISHED cannot occur under PROCESS_ALL for pipeline tasks.
	}
	inline_depth--;
}

} // namespace duckdb
