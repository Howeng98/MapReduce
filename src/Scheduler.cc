#include "Scheduler.h"

Scheduler::Scheduler(char **argv, int cpu_num, int rank, int size) {
    std::string log_file_name;

    this->job_name = std::string(argv[1]);
    this->num_reducer = std::stoi(argv[2]);
    this->delay = std::stoi(argv[3]);
    this->input_filename = std::string(argv[4]);
    this->chunk_size = std::stoi(argv[5]);
    this->locality_filename = std::string(argv[6]);
    this->output_dir = std::string(argv[7]);
    this->rank = rank;
    this->size = size;
    this->cpu_num = cpu_num;
    this->worker_number = size - 1;
    this->chunk_number = 0;
    this->start_time = MPI_Wtime();

    // scheduler need to write log file
    log_file_name = this->job_name + ".log";
    this->write_log = new std::ofstream(log_file_name);

    // clear stl
    this->MapperTaskPool.clear();
    while (!this->ReducerTaskPool.empty())
        this->ReducerTaskPool.pop();
    
    // log start event
    *this->write_log << GetTime() << ", Start_Job, " << argv[1] << ", ";
    *this->write_log << rank << ", " << cpu_num << ", " << argv[2] << ", ";
    *this->write_log << argv[3] << ", " << argv[4] << ", " << argv[5] << ", ";
    *this->write_log << argv[6] << ", " << argv[7] << "\n";
}

Scheduler::~Scheduler() {
    // end
    std::cout << "[Info]: Seheduler terminate\n";
    std::cout << "[Info]: Total cost time: " << MPI_Wtime() - this->start_time << "\n";

    // log end event
    *this->write_log << GetTime() << ", Finish_Job" << ", " << MPI_Wtime() - this->start_time << "\n";

    this->write_log->close();
    delete this->write_log;
}

void Scheduler::DeleteFile(std::string filename) {
    int result = 0;
    char *f = NULL;

    f = (char*)malloc(sizeof(char) * (filename.length() + 1));
    for (int i = 0; i < filename.length(); i++) {
        f[i] = filename[i];
    }
    f[filename.length()] = '\0';
    
    result = std::remove(f);
    if (result != -1) {
        std::cout << "[Info]: Remove file " << filename << " successfully\n";
    }

    free(f);
}

void Scheduler::Shuffle() {
    // read file and start to shuffle
    std::string filename;
    std::string chunk_str;
    std::string reducer_num_str;
    std::string word;
    int count;
    int intermediate_kv_num = 0;
    double st_time = MPI_Wtime();
    double e_time;
    Total records;

    *this->write_log << GetTime() << ", Start_Shuffle, ";

    
    for (int i = 1; i <= this->num_reducer; i++) {
        reducer_num_str = std::to_string(i);
        records.clear();
        for (int j = 1; j <= this->chunk_number; j++) {
            chunk_str = std::to_string(j);
            filename = "./intermediate_file/" + chunk_str + "_" + reducer_num_str + ".txt";

            std::ifstream input_file(filename);
            while (input_file >> word >> count) {
                records.push_back({word, count});
                intermediate_kv_num += 1;
            }
            input_file.close();

            this->DeleteFile(filename);
        }
        // write file
        filename = "./intermediate_file/" + reducer_num_str + ".txt";
        std::ofstream intermediate_file(filename);
        for (auto record : records) {
            intermediate_file << record.first << " " << record.second << "\n";
        }
        intermediate_file.close();
    }
    e_time = MPI_Wtime() - st_time;
    *this->write_log << intermediate_kv_num << "\n";
    *this->write_log << GetTime() << ", Finish_Shuffle, " << e_time << "\n";
}

// get time stamp
time_t Scheduler::GetTime() {
    struct timeval time_now{};
    gettimeofday(&time_now, nullptr);
    time_t msecs_time = (time_now.tv_sec * 1000) + (time_now.tv_usec / 1000);

    return msecs_time;
}

// scheduer need to know the number of chunk and its locality
void Scheduler::GetMapperTask() {
    // read number of chunk and its locality
    int chunk_index;
    int loc_num;
    std::ifstream input_file(this->locality_filename);

    while (input_file >> chunk_index >> loc_num) {
        this->MapperTaskPool.push_back(chunk_index);
        this->Locality[chunk_index] = loc_num % this->worker_number;
        this->chunk_number += 1;
    }

    input_file.close();
}

// scheduler get the number of reducer task
void Scheduler::GetReducerTask() {
    for (int i = 1; i <= this->num_reducer; i++) {
        this->ReducerTaskPool.push(i);
    }
}

// Scheduler assign the mapper task
void Scheduler::AssignMapperTask() {
    // 0: request, 1: finished job index
    MPI_Status status;
    int request[2];
    int task[2];
    int check = 1;
    int task_index;
    int termination_signal = -1;
    int finish_job_num = this->MapperTaskPool.size();

    this->RecordTime.clear(); // clear time record

    while (!this->MapperTaskPool.empty() || finish_job_num > 0) {
        // receive request
        MPI_Recv(request, 2, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        if (request[0] == 0) { // request job
            // if no mapper job, send terminal signal
            if (this->MapperTaskPool.empty()) {
                task[0] = -1;
                task[1] = 0;
                MPI_Send(task, 2, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
            } else {
                // assign mapper task to the node and consider its locality
                for (int i = 0; i < this->MapperTaskPool.size(); i++) {
                    if (status.MPI_SOURCE == this->Locality[this->MapperTaskPool[i]]) {
                        task_index = i;
                        break;
                    } else if (i == this->MapperTaskPool.size() - 1) task_index = 0;
                }
                task[0] = this->MapperTaskPool[task_index];
                task[1] = this->Locality[this->MapperTaskPool[task[0]]];
                this->MapperTaskPool.erase(this->MapperTaskPool.begin() + task_index);

                // Send task to the worker
                MPI_Send(task, 2, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);

                // record dispatch job event
                *this->write_log << GetTime() << ", Dispatch_MapTask, " << task[0] << ", " << status.MPI_SOURCE << "\n";
                // record this job start time
                this->RecordTime[task[0]] = MPI_Wtime();
            }
        } else if (request[0] == 1) { // tell job done
            this->RecordTime[request[1]] = MPI_Wtime() - this->RecordTime[request[1]];
            // record complet job event
            *this->write_log << GetTime() << ", Complete_MapTask, " << request[1] << ", " << this->RecordTime[request[1]] << "\n";
            finish_job_num -= 1;
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
}

void Scheduler::AssignReducerTask() {
    MPI_Status status;
    int task;
    int request[2]; // 0: request, 1: finished job index
    int termination_signal = -1;
    int finish_job_num = this->ReducerTaskPool.size();

    this->RecordTime.clear(); // clear time record

    while (!this->ReducerTaskPool.empty() || finish_job_num > 0) {
        // receive reducer thread from any node
        MPI_Recv(request, 2, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        if (request[0] == 0) {
            if (this->ReducerTaskPool.empty()) {
                MPI_Send(&termination_signal, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
            } else {
                // assign reducer task
                task = this->ReducerTaskPool.front();
                this->ReducerTaskPool.pop();

                // Send task to the worker
                MPI_Send(&task, 1, MPI_INT, status.MPI_SOURCE, 0, MPI_COMM_WORLD);
                // record dispatch event
                *this->write_log << GetTime() << ", Dispatch_ReduceTask, " << task << ", " << status.MPI_SOURCE << "\n";
                // record time
                this->RecordTime[task] = MPI_Wtime();
            }
        } else if (request[0] == 1) {
            this->RecordTime[request[1]] = MPI_Wtime() - this->RecordTime[request[1]];
            // record complete event
            *this->write_log << GetTime() << ", Complete_ReduceTask, " << request[1] << ", " << this->RecordTime[request[1]] << "\n";
            finish_job_num -= 1;
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
}

void Scheduler::EndWorkerExcecute(int num) {
    MPI_Status status;
    int worker_index;
    int signal = 1;

    for (int i = 0; i < this->worker_number; i++) {
        MPI_Send(&signal, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
        MPI_Recv(&signal, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
    }
    std::string job_name = (!num) ? "Mapper" : "Reducer";
    std::cout << "[Info]: " << job_name << " Task terminate successfully\n";
}