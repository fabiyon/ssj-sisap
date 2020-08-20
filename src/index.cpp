#include "def.h"

void BuildIndex_PPJOIN() {
  Record *r;
  int k, indexPrefixLength;
  index_entry entry;
  unsigned long numEntries = 0;
  InvertedIndex::iterator iterI;

#ifdef STATISTICS
  struct timespec tstart, tfinish;
  clock_gettime(CLOCK_MONOTONIC, &tstart);
#endif

  for (int x = 0; x < numRecords; x++) {
	if (FLAGS_inline_records == 1) {
	  r = recIx[x];
	} else {
      r = &R[x];
	}
    indexPrefixLength = r->indexPrefixLength;

    for (int i = 0; i < indexPrefixLength; i++) {
	  if (FLAGS_inline_records == 1) {
		k = r->tids[i];
	  } else {
        k = r->keywords[i];
	  }
      entry.rec = r;
      entry.position = i;

      idxR[k].push_back(entry); // Adds element at
                                // the end. Problem: k assumes that the tokens
                                // start with 0 and are dense
      startingPosition[k] = 0;  // Optimizes index
                                // probes by ignoring shorter records.

      numEntries++;
    }
  }

#ifdef STATISTICS
  stats->avgNumIndexEntries = (float)numEntries / numKeywords;
  clock_gettime(CLOCK_MONOTONIC, &tfinish);
  stats->indexing_duration += (tfinish.tv_sec - tstart.tv_sec);
  stats->indexing_duration += (tfinish.tv_nsec - tstart.tv_nsec) / 1000000000.0;

#endif
}

