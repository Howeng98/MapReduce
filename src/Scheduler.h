#ifndef SEHEDULER_H
#define SEHEDULER_H

#include <iostream>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <string>
#include <sstream>
#include <queue>
#include <mpi.h>
#include <sys/time.h>
#include "Tool.h"

class Scheduler {
public:
    Scheduler(char **argv, int cpu_num, int rank, int size);
    ~Scheduler();
    void GetMapperTask();
    void AssignMapperTask();
    void GetReducerTask();
    void AssignReducerTask();
    void EndWorkerExcecute(int num);
    void DeleteFile(std::string filename);
    void Shuffle();
    time_t GetTime();

private:
    std::vector<int> MapperTaskPool;
    std::queue<int> ReducerTaskPool;
    std::unordered_map<int, int> Locality;
    std::unordered_map<int, double> RecordTime;
    std::ofstream *write_log;
    std::string job_name;
    std::string input_filename;
    std::string locality_filename;
    std::string output_dir;
    int rank;
    int size;
    int delay;
    int cpu_num;
    int worker_number;
    int num_reducer;
    int chunk_number;
    int chunk_size;
    double start_time;
};

#endif