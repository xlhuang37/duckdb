#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <chrono>
#include <ctime>
#include <thread>

#if defined(_WIN32)
#include <windows.h>
#endif

namespace duckdb {

//! Returns the CPU time (in microseconds) consumed by the calling thread so far.
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
		return static_cast<int64_t>((kt.QuadPart + ut.QuadPart) / 10);
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

TaskExecutor::TaskExecutor(TaskScheduler &scheduler)
    : scheduler(scheduler), token(scheduler.CreateQueryNode()), completed_tasks(0), total_tasks(0) {
}

TaskExecutor::TaskExecutor(ClientContext &context_p) : TaskExecutor(TaskScheduler::GetScheduler(context_p)) {
	context = context_p;
}

TaskExecutor::~TaskExecutor() {
}

void TaskExecutor::PushError(ErrorData error) {
	error_manager.PushError(std::move(error));
}

bool TaskExecutor::HasError() {
	return error_manager.HasError();
}

void TaskExecutor::ThrowError() {
	error_manager.ThrowException();
}

void TaskExecutor::ScheduleTask(unique_ptr<Task> task) {
	++total_tasks;
	scheduler.ScheduleTask(*token, std::move(task));
}
void TaskExecutor::FinishTask() {
	++completed_tasks;
}

void TaskExecutor::WorkOnTasks() {
	// repeatedly execute tasks until we are finished
	shared_ptr<Task> task_from_producer;
	// wait for all active tasks to finish
	while (completed_tasks != total_tasks) {
		if (scheduler.GetTaskFromProducer(*token, task_from_producer)) {
			const auto res = task_from_producer->Execute(TaskExecutionMode::PROCESS_ALL);
			std::ignore = res;
			D_ASSERT(res != TaskExecutionResult::TASK_BLOCKED);
			task_from_producer.reset();
		} else {
			std::this_thread::yield();
		}
	}

	// check if we ran into any errors while checkpointing
	if (HasError()) {
		// throw the error
		ThrowError();
	}
}

bool TaskExecutor::GetTask(shared_ptr<Task> &task) {
	return scheduler.GetTaskFromProducer(*token, task);
}

BaseExecutorTask::BaseExecutorTask(TaskExecutor &executor) : executor(executor) {
}

TaskExecutionResult BaseExecutorTask::Execute(TaskExecutionMode mode) {
	const auto cpu_start = GetThreadCpuTimeUsecs();
	auto flush_cpu_time = [&]() {
		if (token) {
			token->AddCpuTime(GetThreadCpuTimeUsecs() - cpu_start);
		}
	};
	if (executor.HasError()) {
		// another task encountered an error - bailout
		flush_cpu_time();
		executor.FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}
	try {
		{
			TaskNotifier task_notifier {executor.context};
			ExecuteTask();
		}
		flush_cpu_time();
		executor.FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	} catch (std::exception &ex) {
		flush_cpu_time();
		executor.PushError(ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		flush_cpu_time();
		executor.PushError(ErrorData("Unknown exception during Checkpoint!"));
	} // LCOV_EXCL_STOP
	executor.FinishTask();
	return TaskExecutionResult::TASK_ERROR;
}

} // namespace duckdb
