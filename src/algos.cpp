#include "def.h"

std::atomic<uint32_t> next_rid(0);
std::atomic<uint32_t> numberOfActiveThreads(0);

void generateAndVerifyCandidates()
{
  static const uint32_t kInvalidRid = ~uint32_t{0};

  Record *rx, *ry;
  int k, ypos;
  InvertedIndex::iterator iterI;
  uint32_t rid = kInvalidRid;

  uint32_t batch_processed;

  uint64_t candidates = 0;
  uint64_t results = 0;
  thread_local std::unordered_map<Record *, int> overlaps;
  int * starting_position_local = new int[numKeywords];
  
  numberOfActiveThreads.fetch_add(1);

  while (true)
  {
    if (batch_processed == FLAGS_batch_size || rid == kInvalidRid)
    {
      rid = next_rid.fetch_add(FLAGS_batch_size);
      batch_processed = 0;
    }

    if (rid >= numRecords)
    {
      break;
    }
    
    ++batch_processed;

    // FILTERING:
    if (FLAGS_inline_records == 1)
    {
      rx = recIx[rid];
    }
    else
    {
      rx = &R[rid];
    }

    // Use multiple hashtabs to reduce possible search overhead, and make
    // them thread-local to avoid memory allocation pressure
    overlaps.clear();

    // Traverse probe prefix of the record.
    for (int xpos = 0; xpos < rx->probePrefixLength; xpos++)
    {
      if (FLAGS_inline_records == 1)
      {
        k = rx->tids[xpos];
      }
      else
      {
        k = rx->keywords[xpos];
      }

      // Access inverted list of keyword k.
      int minLength = int(FLAGS_threshold * rx->length - EPS) + 1;
      if ((iterI = idxR.find(k)) != idxR.end())
      {
        for (int i = starting_position_local[k]; i < iterI->second.size(); i++) // the index access is expensive. If the array is never incremented (all 0 values) we lose several seconds over just entering 0.
        {
          ry = (iterI->second)[i].rec;

          // length optimization: ignore larger records in the index:
          if (ry->length < minLength)
          {
            starting_position_local[k]++;
            continue;
          }
          else if (ry->length > rx->length)
          { // we can skip longer already indexed records. They will probe this record later anyway.
            break;
          }

          ypos = (iterI->second)[i].position;

          // To avoid duplicates, (x,y) and (y,x)
          if (rx->id <= ry->id)
            break;

          if (FLAGS_pos_filter == 1) {
            if (!QualifyPositionalFilter(rx, xpos, ry, ypos)) {
              continue;
            }
          }
          auto ret = overlaps.emplace(ry, 1);
          if (!ret.second)
          {
            // Insert didn't happen
            ++(ret.first->second);
          }

        }
      }
    }

    for (auto &x : overlaps)
    {
      uint64_t nc = 0, nr = 0;
      VerifyAllPairsThread(rx, x.first, x.second, nc, nr);
      candidates += nc;
      results += nr;
    }

    // Advance RID for the next record
    ++rid;
  }

  // Accumulate results to global counters
  __sync_fetch_and_add(&numResults, results); 
  __sync_fetch_and_add(&numCandidates, candidates);
  numberOfActiveThreads.fetch_sub(1);
  delete[] starting_position_local;
}

/**
 * allps
 * with cpu affinity (round robin)
 * */
void AllPairsStdThread()
{
  std::vector<std::thread *> threads;
  cpu_set_t cpuset;
  for (uint32_t i = 0; i < FLAGS_threads; ++i) {
    std::thread * t = new std::thread(generateAndVerifyCandidates);
    threads.push_back(t);
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(t->native_handle(), sizeof(cpuset), &cpuset);
  }

  for (uint32_t i = 0; i < FLAGS_threads; ++i) {
    threads[i]->join();
  }

  std::cerr << "completed.\n";
}

/**
 * allph
 * thread placement by OS
 * */
void AllPairsHyperThread() {
  std::vector<std::thread *> threads;
  for (uint32_t i = 0; i < FLAGS_threads; ++i) {
    std::thread * t = new std::thread(generateAndVerifyCandidates);
    threads.push_back(t);
  }

  for (uint32_t i = 0; i < FLAGS_threads; ++i) {
    threads[i]->join();
  }

  std::cerr << "completed.\n";
}

/**
 * allp (also PPJ with parameter pos_filter=1)
 * single-threaded
 * */
void AllPairs()
{
  Record *rx, *ry;
  int k, counter = -1, overlap, ypos;
  hash<int, int>::iterator iterO;
  InvertedIndex::iterator iterI;

  for (int rid = 0; rid < numRecords; rid++)
  {
// FILTERING STAGE
#ifdef STATISTICS
    struct timespec tstart, tfinish;
    clock_gettime(CLOCK_MONOTONIC, &tstart);
#endif

    if (FLAGS_inline_records == 1)
    {
      rx = recIx[rid];
    }
    else
    {
      rx = &R[rid];
    }
    Overlap->clear();

    // Traverse probe prefix of the record.
    // Filtering needs about half of the runtime
    for (int xpos = 0; xpos < rx->probePrefixLength; xpos++)
    {
      if (FLAGS_inline_records == 1)
      {
        k = rx->tids[xpos];
      }
      else
      {
        k = rx->keywords[xpos];
      }
      int minLength = int(FLAGS_threshold * rx->length - EPS) + 1;

      // Access inverted list of keyword k.
      if ((iterI = idxR.find(k)) != idxR.end())
      {
        // The startingPosition is a huge improvement. If we start from 0
        // always, the runtime becomes really long
        for (int i = startingPosition[k]; i < iterI->second.size(); i++)
        {
          ry = (iterI->second)[i].rec;

          // length optimization:
          if (ry->length < minLength)
          {
            startingPosition[k]++;
            continue;
          }
          else if (ry->length > rx->length)
          { // we can skip longer already indexed records. They will probe this record later anyway.
            break;
          }

          ypos = (iterI->second)[i].position;

          // To avoid duplicates, (x,y) and (y,x)
          if (rx->id <= ry->id)
            break;

          if (Overlap->exists(ry->id))
            Overlap->increase(ry->id);
          else
          {
            // Apply positional filter (PPJoin extension)
            if (FLAGS_pos_filter == 1 && !QualifyPositionalFilter(rx, xpos, ry, ypos)) {
                continue;
            }

            Overlap->add(ry->id);
          }
        }
      }

    }

#ifdef STATISTICS
    // stats->filtering_duration += ctim.stopTimer();
    clock_gettime(CLOCK_MONOTONIC, &tfinish);
    stats->filtering_duration += (tfinish.tv_sec - tstart.tv_sec);
    stats->filtering_duration +=
        (tfinish.tv_nsec - tstart.tv_nsec) / 1000000000.0;
#endif

// VERIFICATION STAGE
// The verification needs roughly the other half of the total runtime
#ifdef STATISTICS
    clock_gettime(CLOCK_MONOTONIC, &tstart);
#endif

    for (vector<pair<int, int>>::iterator iterO = Overlap->data.begin();
         iterO != Overlap->data.end(); ++iterO)
    {

      if (FLAGS_inline_records == 1)
      {
        ry = recIx[iterO->first];
      }
      else
      {
        ry = &R[iterO->first];
      }
      overlap = iterO->second;
      Overlap->erase(iterO->first);

      // Verify (x,y) pair.
      VerifyAllPairs(rx, ry, overlap);
    }

#ifdef STATISTICS
    clock_gettime(CLOCK_MONOTONIC, &tfinish);
    stats->verification_duration += (tfinish.tv_sec - tstart.tv_sec);
    stats->verification_duration +=
        (tfinish.tv_nsec - tstart.tv_nsec) / 1000000000.0;
#endif

  }
}

