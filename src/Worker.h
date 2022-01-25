#ifndef WORKER_H
#define WORKER_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <queue>
#include <utility>
#include <cstdio>
#include <algorithm>
#include <mutex>
#include <mpi.h>
#include <unistd.h>
#include <pthread.h>
#include "Tool.h"

#define WAIT 0

class Worker {
public:
    Worker(char **argv, int cpu_num, int rank, int size);
    ~Worker();
    void ThreadPoolMapper();
    void ThreadPoolReducer();
    void InputSplit(int chunk, Count *word_count, Word *words);
    void ReadFile(int task, Total *total);
    void Sort(Total *total);
    void Group(Total *toatl, Collect *group);
    void Reduce(Collect *group, Count *word_count);
    void Output(Count *word_count, int task);
    void Map(std::string line, Count *word_count, Word *words);
    void DeleteFile(std::string filename);
    static void* MapperFunction(void* input);
    static void* ReducerFunction(void* input);
    static bool cmp(Item a, Item b);
    int Partition(int num_reducer, std::string word);

private:
    int available_num; // check availabl thread
    int mapper_thread_number;
    int reducer_thread_number; // to tell from num_reducer
    int scheduler_index;
    int num_reducer;
    int chunk_size;
    int node_num;
    int cpu_num;
    int delay;
    int rank;

    pthread_t *threads;
    std::mutex *lock;
    std::mutex *send_lock;
    std::string input_filename;
    std::string job_name;
    std::string output_dir;
    std::queue<Chunk> *job_mapper;
    std::queue<int> *job_reducer;
    std::queue<int> *job_finished;
};

#endif