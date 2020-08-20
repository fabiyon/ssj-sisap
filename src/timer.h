#pragma once

#include <ctime>
#include <iostream>
#include <sys/time.h>

class Timer {
private:
  double start;

public:
  Timer() : start(0) {}
  ~Timer() {}

  inline void Reset() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    start = ((uint64_t)tv.tv_sec * 1000000 + tv.tv_usec) / 1000.0;
  }

  inline double GetElapsedTimeMs() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return ((uint64_t)tv.tv_sec * 1000000 + tv.tv_usec) / 1000.0 - start;
  }
};
