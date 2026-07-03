//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/thread_cpu_time.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <chrono>
#include <cstdint>
#include <ctime>

#if defined(_WIN32)
#include <windows.h>
#endif

namespace duckdb {

//! Returns the CPU time (in microseconds) consumed by the calling thread so far.
inline int64_t GetThreadCpuTimeUsecs() {
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

} // namespace duckdb
