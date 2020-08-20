#pragma once

#include <atomic>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <sys/mman.h>
#include <thread>
#include <gflags/gflags.h>
#include <glog/logging.h>

//#define __UNIX_LINUX__

#ifdef __UNIX_LINUX__
#include <limits.h>
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#else
#include <limits>
#include <unordered_map>
#include <map>
#include <unordered_set>
#include <set>
#endif

#include <algorithm>
#include <list>
#include <vector>
#include "timer.h"
#include <iostream>
#include <sstream>
#include <sys/time.h>

#ifdef __UNIX_LINUX__
#define hash tr1::unordered_map
#define shash tr1::unordered_set
#else
#define hash unordered_map
#define shash unordered_set
#endif

#define EPS 1e-8

#define STATISTICS

#define CACHELINE_SIZE 64
#define CACHE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#define COMPILER_MEMORY_FENCE asm volatile("" ::: "memory")

using namespace std;

class MyHash {
public:
  int *probs;
  vector<pair<int, int>> data;

  MyHash(int numKeys) {
    this->probs = new int[numKeys];
    memset(this->probs, 0, numKeys * sizeof(int)); // fills the block with zeros
  };

  void clear() { this->data.clear(); };

  bool exists(int key) { return (this->probs[key] != 0); };

  void add(int key) {
    int one = 1;
    this->data.push_back(make_pair<int, int>(int(key), int(one)));
    this->probs[key] = this->data.size();
  };

  void increase(int key) { this->data[this->probs[key] - 1].second++; };

  int &operator[](int key) { this->data[this->probs[key] - 1].second; };

  void erase(int key) { this->probs[key] = 0; };

  ~MyHash() { delete[] this->probs; };
};

struct Record {
  int id;
  int length;
  float sqrtLength;
  int *keywords;
  int indexPrefixLength;
  int probePrefixLength;
  int validIndexPrefixLength;
  int requiredOverlap;
  int hammingDistanceThreshold;
  int xpos_mp;
  int ypos_mp;
  int optimalPrefixSchemeDegree;
  int tids[0];
};

struct index_entry {
  Record *rec;
  int position;
};

typedef Record *Relation;

typedef vector<index_entry> InvertedList;
typedef hash<int, InvertedList> InvertedIndex;

class Statistics {
public:
  float avgRecordLength;
  float avgProbePrefixLength;
  float avgIndexPrefixLength;
  float avgNumIndexEntries;
  double grouping_duration;     // unsigned long long
  double indexing_duration;     // unsigned long long
  double filtering_duration;    // unsigned long long
  double verification_duration; // unsigned long long

  Statistics() { this->Init(); };

  void Init() {
    this->avgRecordLength = 0;
    this->avgProbePrefixLength = 0;
    this->avgIndexPrefixLength = 0;
    this->avgNumIndexEntries = 0;
    this->grouping_duration = 0;
    this->indexing_duration = 0;
    this->filtering_duration = 0;
    this->verification_duration = 0;
  };
};

extern Relation R;
extern Record * recs; // holds all records including all tids
extern Record ** recIx; // holds the pointers to the records inside recs
extern InvertedIndex idxR;

extern int *startingPosition;

extern int numRecords, numKeywords;
extern unsigned long long numCandidates, numResults;

DECLARE_int32(inline_records);
DECLARE_int32(batch_size);
DECLARE_double(threshold);
DECLARE_uint64(threads);
DECLARE_int32(pos_filter);

extern MyHash *Overlap;

#ifdef STATISTICS
extern Statistics *stats;
#endif

// IO functions.
void LoadRelationOptimized(const char *filename);

// Tool functions.
bool QualifyTextual(Record *rx, Record *ry, int xstart, int ystart, int o);
void VerifyAllPairs(Record *rx, Record *ry, int overlap);
void VerifyAllPairsThread(Record *rx, Record *ry, int overlap,
                          uint64_t &n_candidates, uint64_t &n_results);

// Filters.
bool QualifyPositionalFilter(Record *rx, int xpos, Record *ry, int ypos);

// Indexing functions.
void BuildIndex_PPJOIN();

// Algorithms.
void AllPairs(); // allp - 1 thread
void AllPairsStdThread(); // allps - multithreaded with cpu affinity
void AllPairsHyperThread(); // allph - multithreaded, thread placement by OS
