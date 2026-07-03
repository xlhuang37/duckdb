#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/thread_cpu_time.hpp"
#include "duckdb/common/chrono.hpp"

namespace duckdb {

//! In PROCESS_ALL mode a task runs for at most this long before yielding with TASK_NOT_FINISHED, so it is rescheduled
//! and other (possibly higher-priority) tasks get a turn. This bounds how long a single task can monopolize a worker.
static constexpr int64_t TASK_TIME_SLICE_USECS = 10000; // 10 milliseconds

ExecutorTask::ExecutorTask(Executor &executor_p, shared_ptr<Event> event_p)
    : executor(executor_p), event(std::move(event_p)), context(executor_p.context) {
	executor.RegisterTask();
}

ExecutorTask::ExecutorTask(ClientContext &context_p, shared_ptr<Event> event_p, const PhysicalOperator &op_p)
    : executor(Executor::Get(context_p)), event(std::move(event_p)), op(&op_p), context(context_p) {
	thread_context = make_uniq<ThreadContext>(context_p);
	executor.RegisterTask();
}

ExecutorTask::~ExecutorTask() {
	if (thread_context) {
		executor.Flush(*thread_context);
	}
	executor.UnregisterTask();
}

void ExecutorTask::Deschedule() {
	auto this_ptr = shared_from_this();
	executor.AddToBeRescheduled(this_ptr);
}

void ExecutorTask::Reschedule() {
	auto this_ptr = shared_from_this();
	executor.RescheduleTask(this_ptr);
}

void ExecutorTask::StartCpuTimer() {
	cpu_time_start = GetThreadCpuTimeUsecs();
}

void ExecutorTask::FlushCpuTime() {
	if (cpu_time_start == 0) {
		return;
	}
	const auto now = GetThreadCpuTimeUsecs();
	if (token) {
		token->AddCpuTime(now - cpu_time_start);
	}
	cpu_time_start = now;
}

TaskExecutionResult ExecutorTask::Execute(TaskExecutionMode mode) {
	StartCpuTimer();
	// In PROCESS_ALL mode we execute in small (PROCESS_PARTIAL) steps and stop once the task finishes/blocks or the
	// time slice expires. When the slice expires with work remaining we return TASK_NOT_FINISHED so the scheduler
	// reschedules the task instead of letting it monopolize the worker.
	const auto slice_start = steady_clock::now();
	auto slice_expired = [&]() {
		const auto elapsed = duration_cast<microseconds>(steady_clock::now() - slice_start).count();
		return elapsed >= TASK_TIME_SLICE_USECS;
	};
	try {
		TaskExecutionResult result;
		if (thread_context) {
			do {
				TaskNotifier task_notifier {context};
				thread_context->profiler.StartOperator(op);
				// to allow continuous profiling, always execute in small steps
				result = ExecuteTask(TaskExecutionMode::PROCESS_PARTIAL);
				thread_context->profiler.EndOperator(nullptr);
				executor.Flush(*thread_context);
			} while (mode == TaskExecutionMode::PROCESS_ALL &&
			         result == TaskExecutionResult::TASK_NOT_FINISHED && !slice_expired());
		} else {
			do {
				TaskNotifier task_notifier {context};
				result = ExecuteTask(TaskExecutionMode::PROCESS_PARTIAL);
			} while (mode == TaskExecutionMode::PROCESS_ALL &&
			         result == TaskExecutionResult::TASK_NOT_FINISHED && !slice_expired());
		}
		FlushCpuTime();
		return result;
	} catch (std::exception &ex) {
		FlushCpuTime();
		executor.PushError(ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		FlushCpuTime();
		executor.PushError(ErrorData("Unknown exception in ExecutorTask::Execute"));
	} // LCOV_EXCL_STOP
	return TaskExecutionResult::TASK_ERROR;
}

} // namespace duckdb
