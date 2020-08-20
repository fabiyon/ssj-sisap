#include "def.h"

Record * recs; // holds all records including all tids
Record ** recIx;

void LoadRelationOptimized(const char *filename) {
  char c;
  stringstream ss;
  FILE *fp = 0;
  int recid = 0, keywordIdx = 0;
  unsigned long long totalLength = 0;

  std::cout << "Loading relation: " << filename << std::endl;

  std::ifstream file(filename);
  std::string line;
  // first line is special:
  std::getline(file, line);
  int numTids;
  sscanf(line.c_str(), "%d %d %d\n", &numRecords, &numKeywords, &numTids); // numKeywords is the number of distinct keywords, numTids is the total number of keywords
  R = new Record[numRecords];
  int tidCnt = 0;
  while (std::getline(file, line)) {
    stringstream linestream(line);

    std::string tmpString;
    std::getline(linestream, tmpString, '\t');
    R[recid].length = std::stoi(tmpString);
#ifdef STATISTICS
    totalLength += R[recid].length;
#endif
    R[recid].sqrtLength = sqrt(R[recid].length);
    R[recid].keywords = new int[R[recid].length];
    R[recid].probePrefixLength =
      min(R[recid].length,
        (int)(R[recid].length - R[recid].length * FLAGS_threshold + EPS) + 1);
    R[recid].indexPrefixLength =
      min(R[recid].length,
        (int)(R[recid].length -
          R[recid].length * 2.0 * FLAGS_threshold / (1.0 + FLAGS_threshold) + EPS) +
          1);

    while (std::getline(linestream, tmpString, ',')) {
      R[recid].keywords[keywordIdx] = std::stoi(tmpString);
      keywordIdx++;
    }
    R[recid].id = recid; // If you just have a pointer to this record object it is
          // useful to be able to determine it's record ID.
    recid++;
    keywordIdx = 0;
  } // end while

  startingPosition = new int[numKeywords];

#ifdef STATISTICS
  stats->avgRecordLength = (float)totalLength / numRecords;
#endif
  std::cout << "Loaded " << numRecords << " records" << std::endl;
}
