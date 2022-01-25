#include "Worker.h"

Worker::Worker(char **argv, int cpu_num, int rank, int size) {
    this->job_name = std::string(argv[1]);
    this->num_reducer = std::stoi(argv[2]);
    this->delay = std::stoi(argv[3]);
    this->input_filename = std::string(argv[4]);
    this->chunk_size = std::stoi(argv[5]);
    this->output_dir = std::string(argv[7]);
    this->rank = rank;
    this->cpu_num = cpu_num;
    this->node_num = rank;
    this->available_num = 0;
    this->scheduler_index = size - 1;
    this->mapper_thread_number = cpu_num - 1;
    this->reducer_thread_number = 1;
    this->threads = new pthread_t[cpu_num];
    this->lock = new std::mutex;
    this->send_lock = new std::mutex;
}

Worker::~Worker() {
    delete this->lock;
    delete this->send_lock;
    std::cout << "[Info]: Worker "<< this->rank << " terminate\n";
}

void Worker::DeleteFile(std::string filename) {
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

void Worker::ThreadPoolMapper() {
    bool task_finished = false;
    MPI_Status status;
    int signal = 1;
    int request[2];
    int chunk_index[2];

    this->job_mapper = new std::queue<Chunk>;
    this->job_finished = new std::queue<int>;

    // allocate mapper thread
    for (int i = 0; i < this->mapper_thread_number; i++) {
        pthread_create(&this->threads[i], NULL, Worker::MapperFunction, (void*)this);
    }

    while (!task_finished) {
        // check available thread number
        while (this->available_num == 0);

        request[0] = 0;
        request[1] = 0;
        MPI_Send(request, 2, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD);
        MPI_Recv(chunk_index, 2, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD, &status);

        this->lock->lock();
        this->job_mapper->push({chunk_index[0], chunk_index[1]});
        this->lock->unlock();
        
        // mapper job done
        if (chunk_index[0] == -1) {
            task_finished = true;
        }
    }

    // check all mapper thread return
    for (int i = 0; i < this->mapper_thread_number; i++) {
        pthread_join(this->threads[i], NULL);
    }

    while (!this->job_finished->empty()) {
        request[0] = 1;
        request[1] = this->job_finished->front();
        MPI_Send(request, 2, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD);
        this->job_finished->pop();
    }

    // delete queue
    delete this->job_mapper;
    delete this->job_finished;
    MPI_Barrier(MPI_COMM_WORLD);
}

void Worker::ThreadPoolReducer() {
    bool task_finished = false;
    MPI_Status status;
    int signal = 1;
    int request[2];
    int reducer_index;

    this->job_reducer = new std::queue<int>;
    this->job_finished = new std::queue<int>;

    for (int i = 0; i < this->reducer_thread_number; i++) {
        pthread_create(&this->threads[i], NULL, Worker::ReducerFunction, this);
    }

    while (!task_finished) {
        // check available thread
        while (this->available_num == 0);

        request[0] = 0;
        request[1] = 0;
        MPI_Send(request, 2, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD);
        MPI_Recv(&reducer_index, 1, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD, &status);

        this->lock->lock();
        this->job_reducer->push(reducer_index);
        this->lock->unlock();
        
        if (reducer_index == -1) { // reducer job done
            task_finished = true;
        }
    }

    // check all mapper thread return
    for (int i = 0; i < this->reducer_thread_number; i++) {
        pthread_join(this->threads[i], NULL);
    }

    while (!this->job_finished->empty()) {
        request[0] = 1;
        request[1] = this->job_finished->front();
        MPI_Send(request, 2, MPI_INT, this->scheduler_index, 0, MPI_COMM_WORLD);
        this->job_finished->pop();
    }

    // delete queue
    delete this->job_reducer;
    delete this->job_finished;
    MPI_Barrier(MPI_COMM_WORLD);
}

void* Worker::MapperFunction(void *input) {
    Worker *mapper = (Worker*)input;
    Count *word_count = new Count;
    Word *words = new Word;
    Chunk chunk;
    bool task_finished = false;

    while (!task_finished) {
        chunk.first = -1;
        mapper->lock->lock();
        if (!mapper->job_mapper->empty()) {
            chunk = mapper->job_mapper->front();
            if (chunk.first == -1) {
                task_finished = true;
            } else {
                mapper->available_num -= 1;
                mapper->job_mapper->pop();
            }
        }
        mapper->lock->unlock();

        if (chunk.first != -1) {
            if (chunk.second != mapper->rank && WAIT) {
                sleep(mapper->delay);
            }
            word_count->clear();
            words->clear();

            // Input Split
            mapper->InputSplit(chunk.first, word_count, words);

            // get word partition number
            std::vector<std::vector<std::string>> split_result(mapper->num_reducer + 1);
            for (auto word : *words) {
                split_result[mapper->Partition(mapper->num_reducer, word)].push_back(word);
            }

            // generate intermediate file
            for (int i = 1; i <= mapper->num_reducer; i++) {
                std::string chunk_str = std::to_string(chunk.first);
                std::string reducer_num_str = std::to_string(i);
                std::string filename = "./intermediate_file/" + chunk_str + "_" + reducer_num_str + ".txt";
                std::ofstream myfile(filename);
                for (auto word : split_result[i]) {
                    myfile << word << " " << (*word_count)[word] << "\n";
                }
                myfile.close();
            }

            mapper->lock->lock();
            mapper->job_finished->push(chunk.first);
            mapper->available_num += 1;
            mapper->lock->unlock();
        }
    }

    delete word_count;
    delete words;
    pthread_exit(NULL);
}

void Worker::InputSplit(int chunk, Count *word_count, Word *words) {
    int start_pos = 1 + (chunk - 1) * this->chunk_size;

    // read chunk file
    std::ifstream input_file(this->input_filename);
    std::string line;

    // find the chunk start position
    for (int i = 1; i < start_pos; i++) {
        getline(input_file, line);
    }

    for (int i = 1; i <= this->chunk_size; i++) {
        getline(input_file, line);
        this->Map(line, word_count, words);
    }
    input_file.close();
}

void Worker::Map(std::string line, Count *word_count, Word *words) {
    int pos = 0;
    std::string word;
    std::vector<std::string> tmp_words;

    while ((pos = line.find(" ")) != std::string::npos) {
        word = line.substr(0, pos);
        tmp_words.push_back(word);
        line.erase(0, pos + 1);
    }

    if (!line.empty())
        tmp_words.push_back(line);

    for (auto w : tmp_words) {
        if (word_count->count(w) == 0) {
            words->push_back(w);
            (*word_count)[w] = 1;
        } else {
            (*word_count)[w] += 1;
        }
    }
}

int Worker::Partition(int num_reducer, std::string word) {
    return ((word.length() % num_reducer) + 1);
}

void* Worker::ReducerFunction(void* input) {
    Worker *reducer = (Worker*)input;
    Count *word_count = new Count;
    Total *total = new Total;
    Collect *group = new Collect;

    int task;
    bool task_finished = false;

    while (!task_finished) {
        task = -1;
        reducer->lock->lock();
        if (!reducer->job_reducer->empty()) {
            task = reducer->job_reducer->front();
            if (task == -1) {
                task_finished = true;
            } else {
                reducer->available_num -= 1;
                reducer->job_reducer->pop();
            }
        }
        reducer->lock->unlock();

        if (task != -1) {
            word_count->clear();
            total->clear();
            group->clear();

            // read intermediate file
            reducer->ReadFile(task, total);
            // sort words
            reducer->Sort(total);
            // group
            reducer->Group(total, group);
            // reduce
            reducer->Reduce(group, word_count);
            // output
            reducer->Output(word_count, task);

            reducer->lock->lock();
            reducer->job_finished->push(task);
            reducer->available_num += 1;
            reducer->lock->unlock();
        }
    }

    delete word_count;
    delete total;
    delete group;
    pthread_exit(NULL);
}

void Worker::ReadFile(int task, Total *total) {
    std::string filename;
    std::string reducer_num_str = std::to_string(task);
    std::string word;
    int count;

    filename = "./intermediate_file/" + reducer_num_str + ".txt";
    
    std::ifstream input_file(filename);
    while (input_file >> word >> count) {
        total->push_back({word, count});
    }
    input_file.close();

    DeleteFile(filename);
}

bool Worker::cmp(Item a, Item b) {
    return a.first < b.first;
}

void Worker::Sort(Total *total) {
    sort(total->begin(), total->end(), cmp);
}

void Worker::Group(Total *total, Collect *group) {
    for (auto item : *total) {
        if (group->count(item.first) == 0) {
            std::vector<int> tmp_vec;
            tmp_vec.clear();
            (*group)[item.first] = tmp_vec;
            (*group)[item.first].push_back(item.second);
        } else {
            (*group)[item.first].push_back(item.second);
        }
    }
}

void Worker::Reduce(Collect *group, Count *word_count) {
    for (auto item : *group) {
        (*word_count)[item.first] = 0;
        for (auto num : item.second) {
            (*word_count)[item.first] += num;
        }
    }
}

void Worker::Output(Count *word_count, int task) {
    std::string task_str = std::to_string(task);
    std::string filename = this->output_dir + this->job_name + "-" + task_str + ".out";
    std::ofstream myfile(filename);

    for (auto word : *word_count) {
        myfile << word.first << " " << word.second << "\n";
    }
    myfile.close();
}