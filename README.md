# ssj-sisap

This is the code to the experiments to the paper "Parallelizing Filter-Verification based Exact Set Similarity Joins on Multicores" accepted for SISAP 2020: http://www.sisap.org/2020/papers.html

## Compilation
The code requires the packages libboost-all-dev libgflags-dev libgoogle-glog-dev to compile. It compiles with make.

## Input File Requirements
* First line: numberOfRecords numberOfDistinctTokens totalNumberOfTokens
* Record line: numberOfTokens TAB tokensCommaSeparated
* The records must be sorted in ascending lengths
* The tokens must be integers starting from 0 and be dense

## Usage
./mssj -method allp|allps|allph -input_file path_to_file -threshold threshold_between_.5_and_.99 -threads number_of_threads -inline_records true|false -batch_size batch_size -pos_filter true|false

Example:
./mssj -method allp -input_file ../data/BMS-POS_dup_dr.inp -threshold .9
