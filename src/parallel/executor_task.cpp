#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include <chrono>
#include <ctime>

#if defined(_WIN32)
#include <windows.h>
#endif

namespace duckdb {

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
	try {
		if (thread_context) {
			TaskExecutionResult result;
			do {
				TaskNotifier task_notifier {context};
				thread_context->profiler.StartOperator(op);
				// to allow continuous profiling, always execute in small steps
				result = ExecuteTask(TaskExecutionMode::PROCESS_PARTIAL);
				thread_context->profiler.EndOperator(nullptr);
				executor.Flush(*thread_context);
			} while (mode == TaskExecutionMode::PROCESS_ALL && result == TaskExecutionResult::TASK_NOT_FINISHED);
			FlushCpuTime();
			return result;
		} else {
			TaskNotifier task_notifier {context};
			auto result = ExecuteTask(mode);
			FlushCpuTime();
			return result;
		}
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
