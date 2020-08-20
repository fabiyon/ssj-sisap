#include "def.h"

DEFINE_string(input_file, "", "Input data file");
DEFINE_double(threshold, 0.99, "Threshold");
DEFINE_string(method, "allps", "Join algorithm");
DEFINE_uint64(threads, 1, "Number of worker threads");
DEFINE_int32(inline_records, 0, "Should TIDs be integrated into the record structs");
DEFINE_int32(batch_size, 100, "allps: Batch size");
DEFINE_int32(pos_filter, 0, "If AllPairs should use the positional filter (=PPJ)");

Relation R;
InvertedIndex idxR;

int *startingPosition;

int numRecords, numKeywords;
unsigned long long numCandidates, numResults;

#ifdef STATISTICS
Statistics *stats;
#endif

MyHash *Overlap;

unsigned long long total_duration = 0;

void Reset() {
  Overlap = new MyHash(numRecords);

  if (FLAGS_inline_records == 0) {
    for (int rid = 0; rid < numRecords; rid++)
      R[rid].validIndexPrefixLength = R[rid].indexPrefixLength;
  }

  idxR.clear();
}

void CleanUp() {
#ifdef STATISTICS
  delete stats;
#endif

  if (FLAGS_inline_records == 0) {
    for (int rid = 0; rid < numRecords; rid++)
      delete[] R[rid].keywords;
    delete[] R;
  } else { // TODO: free recs and recIx

  }

  delete Overlap;

  delete[] startingPosition;

}

#ifdef STATISTICS
void PrintStatistics() {

  cout << endl;
  cout << "DATASET AND INDEX STATS" << endl;
  cout << "\tInput File.................: " << FLAGS_input_file << endl;
  cout << "\t# Records..................: " << numRecords << endl;
  cout << "\t# Keywords.................: " << numKeywords << endl;
  cout << "\tAverage Record Length......: " << stats->avgRecordLength << endl;
  cout << "\tAverage Index List Length..: " << stats->avgNumIndexEntries
       << endl;
  cout << endl;
  cout << "PHASE TIME STATS" << endl;
  cout << "\tGrouping Time..............: " << (double)stats->grouping_duration
       << endl; 
  cout << "\tIndexing Time..............: " << (double)stats->indexing_duration
       << endl; 
  cout << "\tFiltering Time.............: " << (double)stats->filtering_duration
       << endl; 
  cout << "\tVerification Time..........: "
       << (double)stats->verification_duration << endl; 
  cout << endl;
  cout << "JOIN STATS" << endl;
  cout << "\tMethod.....................: " << FLAGS_method << endl;

  cout << "\tSimilarity Measure.........: Jaccard" << endl;

  cout << "\tSimilarity Threshold.......: " << FLAGS_threshold << endl;
  cout << "\t# Candidates...............: " << numCandidates << endl;
  cout << "\t# Results..................: " << numResults << endl;
  cout << "\tCPU Time (sec).............: "
       << (double)(stats->grouping_duration + stats->indexing_duration +
                   stats->filtering_duration + stats->verification_duration)
       << endl;
  cout << "\tTotal Duration.............: " << total_duration / 1000.0 << endl;
  cout << endl;
}
#endif

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  stringstream ss;

// Initialization.
#ifdef STATISTICS
  stats = new Statistics();
#endif

  numCandidates = numResults = 0;

  if (FLAGS_method == "allps" || FLAGS_method == "allph") {
    std::cout << "NumThreads=" << FLAGS_threads << std::endl;
  }
  
  LoadRelationOptimized(FLAGS_input_file.c_str());
  
  // Perform join.
  if (FLAGS_method == "allp") {
    FLAGS_method = "AllPairs";

    Reset();
    Timer t;
    t.Reset();

    BuildIndex_PPJOIN();
    std::cout << "Built index. Starting join..." << std::endl;

    AllPairs();
    total_duration += t.GetElapsedTimeMs();
  } else if (FLAGS_method == "allps") {
    FLAGS_method = "AllPairsStdThread";
    Reset();
    Timer t;
    t.Reset();

    BuildIndex_PPJOIN();

    AllPairsStdThread();
    total_duration += t.GetElapsedTimeMs();
  } else if (FLAGS_method == "allph") {
    FLAGS_method = "AllPairsHyperThread";
    Reset();
    Timer t;
    t.Reset();

    BuildIndex_PPJOIN();

    AllPairsHyperThread();
    total_duration += t.GetElapsedTimeMs();
  } else {
    CleanUp();

    return 1;
  }

#ifdef STATISTICS
  PrintStatistics();
#endif

  CleanUp();

  return 0;
}
