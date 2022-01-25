#include <iostream>
#include <fstream>
#include <string>
#include <cstring>
#include <map>
#include <vector>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <mpi.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/time.h>
#include <ctime>
#define ROW_NUM 50
#define COL_NUM 50
#define TAKEN_STATE 1
#define READY_STATE 0
#define DESTINATION 1
// #define FINISH_STATE 1
unsigned int microsecond = 1000000;
using namespace std;
using std::cout; using std::endl;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::system_clock;
typedef pair<int, int> task_item; //for store nodeID, task_state
typedef pair<string, int> Item;
pair <int, int> temp_pair; //for read

map<string, int> local_file_system;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
string* input_words;
map <int, pair<int, int>> loc_config;
vector<string> assign_chunk;
vector<Item*> nice_thing;


map <int, int>::iterator iter;
map <int, int>::reverse_iterator iter_r;
int chunk_size;
int current_row;

struct arg
{
    int thread_id;
    int rank;
    string data;
};

bool exist_available_task(int task_available[], int size){
    for(int idx=0; idx<size; idx++){
        if(task_available[idx] == READY_STATE){
            return true;
        }
    }
    return false;
}

void* worker_task(void* arg){
    struct arg* arg_temp = (struct arg*)arg;
    int start_row, word_count;
    string word, word_name;
    size_t pos = 0;
    vector<Item> thread_local_file_result;
    map<string, int> wordcount_list;
    string input_data;
    pthread_mutex_lock(&mutex);
    input_data = arg_temp->data;
    // do somethings    
    // cout << "Worker::: rank: " << arg_temp->rank << " | threadID: " << arg_temp->thread_id << " | Data: " << arg_temp->data << endl;
    // cout << "Worker::: rank: " << arg_temp->rank << " | threadID: " << arg_temp->thread_id << endl;
    while ((pos = input_data.find(" ")) != string::npos){
        word = input_data.substr(0, pos);
        input_data.erase(0, pos+1);
        // if(word == "Aulacomnium"){
        //     cout << "Aulacomnium" << endl;
        // }

        if (local_file_system.count(word) == 0)
        {
            local_file_system[word] = 1;
        }
        else
        {
            local_file_system[word] = local_file_system[word]+1;
        }
        // cout << arg_temp->rank << "| test_output:" << word << " | " << local_file_system[word] << endl;
    }

    // for(auto iter_ : local_file_system){
    //     cout << arg_temp->rank << "| test_output:" << iter_.first << " || " << iter_.second << endl;
    // }
    pthread_mutex_unlock(&mutex); 
    // return static_cast<void*>(thread_local_file_result);
    //Finish this thread's job, feedback to jobtracker
	pthread_exit(NULL);
}

int main(int argc, char **argv)
{
    auto start = std::chrono::system_clock::now();
    // Arguments passing
    assert(argc == 8);
    string job_name                 = string(argv[1]);
    int num_reducer                 = stoi(argv[2]);
    int delay                       = stoi(argv[3]);
    string input_filename           = string(argv[4]);
    chunk_size                      = stoi(argv[5]);
    string locality_config_filename = string(argv[6]);
    string output_dir               = string(argv[7]);

    // Get # of CPU
    cpu_set_t cpu_set;
    sched_getaffinity(0, sizeof(cpu_set), &cpu_set);
    int num_threads = CPU_COUNT(&cpu_set) - 1;
    // printf("%d cpus available\n", CPU_COUNT(&cpu_set));

    // Pthread setup
    pthread_mutex_init(&mutex, NULL);
    // pthread_t threads[num_threads];
    vector<pthread_t> threads(num_threads);
	struct arg args[num_threads];
    // int *return_vals = malloc(num_threads * sizeof(vector));

    // Vector variables
    map <string, int> word_count;
    int global_sum, local_sum=1;
    int how_many_words_in_a_row = 0;

    input_words = (string*)malloc(ROW_NUM * COL_NUM * sizeof(string));


    // Handle .loc config file
    ifstream loc_file(locality_config_filename);
    string loc_line;
    while(getline(loc_file, loc_line)){
        size_t pos = 0;
        string loc_params;
        int chunkID, nodeID;
        
        while ((pos = loc_line.find(" ")) != string::npos){
            chunkID = stoi(loc_line.substr(0, pos));
            loc_line.erase(0, pos + 1);
            nodeID = stoi(loc_line.substr(0, pos));
            loc_config[chunkID] = make_pair(nodeID, READY_STATE);                                    
        }
    }

    // find in map!
    // if (loc_config.find(1) != loc_config.end()) {
    //     cout << arg_temp->rank << " | Key Exists!" << endl;
    // }

    // MPI Initialize
    int rank, size;
	MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    // printf("From process %d out of %d\n", rank, size);

    ofstream out_logfile(output_dir + job_name + "-" + "log.out");
    // Read words file
    ifstream input_file(input_filename);
    string line;
    // Jobtracker
    if(rank==0){

        auto millisec_since_epoch = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        out_logfile << millisec_since_epoch << ",";
        out_logfile << "Start_Job" << ",";
        out_logfile << job_name << ",";
        out_logfile << size << ",";
        out_logfile << num_threads+1 << ",";
        out_logfile << num_reducer << ",";
        out_logfile << delay << ",";
        out_logfile << input_filename << ",";
        out_logfile << chunk_size << ",";
        out_logfile << locality_config_filename << ",";
        out_logfile << output_dir << endl;

        int temp_row = 0, temp_col = 0;
        string temp_chunk_line="";
        int counter = 1;
        while (getline(input_file, line)){
            size_t pos = 0;
            string word;
            temp_col = 0;
            // cout << line << endl;
            if(counter%(chunk_size)==0){
                //clear
                temp_chunk_line += line;
                temp_chunk_line += " ";
                // cout << temp_chunk_line << endl;
                assign_chunk.push_back(temp_chunk_line);
                temp_chunk_line = "";
            }
            else{
                //concatnate
                temp_chunk_line += line;
                temp_chunk_line += " ";
            }
        
            while ((pos = line.find(" ")) != string::npos)
            {
                word = line.substr(0, pos);
                line.erase(0, pos+1);
                temp_col++;
            }        
            //push last word of the line to words
            if (!line.empty()){
                // input_words[temp_row * ROW_NUM + temp_col] = line;
            }
            temp_row++;
            how_many_words_in_a_row = temp_col+1;
            counter++;
        }

        // for checking which task is still available
        int task_available[assign_chunk.size()] = {READY_STATE};

        int rank_state_switch[size-1] = {0};

        //TODO: add while loop!!!!
        // Recv request from tasktrackers
        int tasktrackerID;
        int jobtracker_recv_counter = 0;
        int tasktracker_sleeped[size-1] = {0};
        int tasktracker_doing_taskID[size-1] = {0};
        int state_switch=0;
        while(true){
            for(int idx=0;idx<size-1;idx++){
                rank_state_switch[idx] = 0;
            }
            MPI_Recv(&tasktrackerID, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // cout << "===Jobtracker ::: recv1 request from nodeID:" << tasktrackerID << endl;

            // Handle request from tasktrackers
            int myChunkID, myNodeID, myState;
            int c_len;
            // check whether got locality
            for(auto iter_ : loc_config){
                myChunkID = iter_.first;
                temp_pair = iter_.second;
                myNodeID = temp_pair.first;
                myState = temp_pair.second;
                // if loc nodeID == request's nodeID
                
                if((myNodeID)%(size-1)+1 == tasktrackerID && task_available[myChunkID-1] == READY_STATE){
                    rank_state_switch[tasktrackerID-1] = 1;
                    // cout << "===TaskID:" << tasktrackerID << " getting task from chunkID:" << myChunkID << endl;
                    c_len = assign_chunk[myChunkID-1].size();
                    task_available[myChunkID-1] = TAKEN_STATE;
                    auto millisec_since_epoch = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                    out_logfile << millisec_since_epoch << ",";
                    out_logfile << "Dispatch_MapTask" << ",";
                    out_logfile << myChunkID << ",";
                    out_logfile << tasktrackerID << endl;
                    MPI_Send(assign_chunk[myChunkID-1].c_str(), c_len+1, MPI_CHAR, tasktrackerID, 0, MPI_COMM_WORLD);
                    tasktracker_doing_taskID[tasktrackerID-1]=myChunkID;
                    break;
                }
            }

            if(rank_state_switch[tasktrackerID-1]!=1){
                // no locality
                // first time sleep
                char sleeping_message[6] = "SLEEP";
                // cout << tasktracker_sleeped[tasktrackerID-1] << endl;
                if(tasktracker_sleeped[tasktrackerID-1] == 0){ //haven't sleep yet
                    MPI_Send(sleeping_message, 6, MPI_CHAR, tasktrackerID, 0, MPI_COMM_WORLD);
                    tasktracker_sleeped[tasktrackerID-1] = 1;
                }
                else if(tasktracker_sleeped[tasktrackerID-1] == 1){
                    // sleeped
                    for(auto iter_ : loc_config){
                        myChunkID = iter_.first;
                        temp_pair = iter_.second;
                        myNodeID = temp_pair.first;
                        myState = temp_pair.second;

                        if(task_available[myChunkID-1] == READY_STATE){
                            c_len = assign_chunk[myChunkID-1].size();
                            // c = new char[c_len];
                            task_available[myChunkID-1] = TAKEN_STATE;
                            // cout << "+++TaskID:" << tasktrackerID << " getting task from chunkID:" << myChunkID << endl;
                            auto millisec_since_epoch = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                            out_logfile << millisec_since_epoch << ",";
                            out_logfile << "Dispatch_MapTask" << ",";
                            out_logfile << myChunkID << ",";
                            out_logfile << tasktrackerID << endl;
                            MPI_Send(assign_chunk[myChunkID-1].c_str(), c_len+1, MPI_CHAR, tasktrackerID, 0, MPI_COMM_WORLD);
                            tasktracker_doing_taskID[tasktrackerID-1]=myChunkID;
                            break;
                        }
                    }
                }
            }
            if(!exist_available_task(task_available, assign_chunk.size())){
                // cout << "===Jobtracker ::: " << "finish in exist_available_task" << endl;
                break;
            }
            for(int i=0;i<assign_chunk.size();i++){
                if(task_available[i] == TAKEN_STATE){
                    auto millisec_since_epoch = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
                    out_logfile << millisec_since_epoch << ",";
                    out_logfile << "Complete_MapTask" << ",";
                    out_logfile << i+1 << ",";
                    if(tasktracker_doing_taskID[tasktrackerID]==(i+1)){
                        out_logfile << tasktrackerID << endl;
                    }
                    else{
                        out_logfile << tasktrackerID << endl;
                    }
                    
                }
            }
        }
        while(true){
            MPI_Recv(&tasktrackerID, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            // cout << "===Jobtracker ::: recv2 request from nodeID:" << tasktrackerID << endl;
            char ending_message[7] = "FINISH";
            MPI_Send(ending_message, 7, MPI_CHAR, tasktrackerID, 0, MPI_COMM_WORLD);
            jobtracker_recv_counter++;
            // cout << "============recv_counter:" << jobtracker_recv_counter << endl;
            if(jobtracker_recv_counter >= size-1){
                // cout << "===Jobtracker ::: " << "finish in counter exceed" << endl;
                break;
            }
        }
    }
    
    //####################################################################################################################
    // Scheduler
    if(rank!=0) {
        // Tasktracker
        // sending request to jobtracker
        int while_c = 0;
        int tasktracker_counter = 0;
        
        while(true){
            int send_buf = rank;
            MPI_Send(&send_buf, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
            // MPI_Send()
            tasktracker_counter++;

            // recv response from jobtracker
            MPI_Status status;
            MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
            int count;
            MPI_Get_count(&status,MPI_CHAR,&count);
            string recv_s;
            char recv_buf[count];
            MPI_Recv(&recv_buf, count, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            //Start working!
            recv_s = recv_buf;
            // cout << "===Tasktracker" << rank << "::: recv_buf: " << recv_s << endl;

            // Do corresponding actions depend on recv_buf which receive from jobtracker
            if(recv_s == "SLEEP"){
                usleep(delay * microsecond);
            }
            else if(recv_s == "FINISH"){
                // cout << "===Tasktracker" << rank << "::: rankID:" << rank << " finished job" << endl;
                break;
            }
            else{
                vector<string> patch_data;
                string word, temp_string="",new_s;
                string space_delimiter = " ";
                size_t pos = 0;
                int counter=0, inner_counter=0;
                new_s = recv_s;
                how_many_words_in_a_row = 0;
                while ((pos = new_s.find(" ")) != string::npos)
                {
                    word = new_s.substr(0, pos);
                    new_s.erase(0, pos+1);
                    how_many_words_in_a_row++;
                }

                int patch_data_size = how_many_words_in_a_row / num_threads;
                int mod_patch_data_size = how_many_words_in_a_row % num_threads;
                // cout << how_many_words_in_a_row << " " << num_threads << " " << patch_data_size << " " << mod_patch_data_size << endl;
                while ((pos = recv_s.find(" ")) != string::npos){
                    word = recv_s.substr(0, pos);
                    temp_string += word;
                    temp_string += " ";
                    recv_s.erase(0, pos+1);
                    counter++;
                    if(counter >= patch_data_size && inner_counter < num_threads-1){
                        // cout << temp_string << endl;
                        patch_data.push_back(temp_string + " ");
                        inner_counter++;
                        temp_string = "";
                        counter = 0;
                    }
                    if(inner_counter>=num_threads-1){
                        if(counter >= patch_data_size + mod_patch_data_size){
                            patch_data.push_back(temp_string + " ");
                            break;
                        }
                    }
                }
                // for(int t=0;t<patch_data.size();t++){
                //     cout << patch_data[t] << endl;
                // }
                // cout << endl;

                int idx;
                for(idx=0; idx<num_threads; idx++){
                    args[idx].thread_id = idx;
                    args[idx].rank = rank;
                    args[idx].data = patch_data[idx];
                    pthread_create(&threads[idx], NULL, worker_task, (void*)&args[idx]);
                }
                for(idx=0; idx<num_threads; idx++){
                    pthread_join(threads[idx], NULL);
                }
    
            }
        }
        // Here can show each tasktracker(rank) 's local file system storage about word_count, thus we need to combine them
        // for(auto iter_ : local_file_system){
        //     cout << "===" << rank << " || " << iter_.first << " || " << iter_.second << endl;
        // }

        vector<Item> wordcount_list;
        for (auto &item : local_file_system){
            wordcount_list.push_back(item);
        }

        auto start_shuffle = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        out_logfile << start_shuffle << ",";
        out_logfile << "Start_Shuffle" << ",";
        out_logfile << wordcount_list.size() << endl;
        bool ASCENDING = true;
        if(ASCENDING){
            sort(wordcount_list.begin(), wordcount_list.end(), [](const Item &item1, const Item &item2) -> bool
            { return item1.first < item2.first; });
        }
        else{
            sort(wordcount_list.begin(), wordcount_list.end(), [](const Item &item1, const Item &item2) -> bool
            { return item1.first > item2.first; });
        }
        
        auto end_shuffle = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        out_logfile << end_shuffle << ",";
        out_logfile << "Finish_Shuffle" << ",";
        out_logfile << end_shuffle-start_shuffle << endl;
        ofstream out(output_dir + job_name + "-" + to_string(rank) + ".out");
        // cout << "Rank:" << rank << " | Address:" << &wordcount_list << endl;
        string super_string="";
        for (auto &iter_ : local_file_system){
            if(iter_.first != ""){
                // out << iter_.first << " " << iter_.second << endl;
                super_string+= iter_.first;
                super_string+= " ";
                super_string+= to_string(iter_.second);
                super_string+= " ";
            }
        }

        // cout << super_string << endl;
        // cout << super_string.size() << endl;
        if(rank!=1){
            // cout << "Rank: " << rank << " | mysuperstring:" << super_string.c_str() << endl;
            // cout << "sent1" << endl;
            MPI_Status status1;
            // MPI_Probe(MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &status1);
            // // MPI_Probe( int source , int tag , MPI_Comm comm , MPI_Status* status);
            // int count;
            // cout << "sent2" << endl;
            // MPI_Get_count(&status1,MPI_CHAR,&count);
            char recv_bufff[6];
            // cout << "sent3" << endl;
            MPI_Recv(&recv_bufff, 6, MPI_CHAR, 1, 3, MPI_COMM_WORLD, &status1);
            // cout << "sent4" << endl;
            // cout << recv_bufff << endl;
            MPI_Send(super_string.c_str(), super_string.size()+1, MPI_CHAR, 1, 4, MPI_COMM_WORLD);
            // cout << "sent5" << endl;
        }
        // cout << "xaxa" << endl;
        if(rank==1){
            if(size-1>=2){
                // cout << "Here is listenXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX" << endl;
                int collected_tasktracker;
                // usleep(1 * microsecond);
                // while(collected_tasktracker != size-2){
                for(collected_tasktracker=2; collected_tasktracker <= size-1; collected_tasktracker++){
                    // cout << "Chechk chekc chekc " << endl;
                    
                    // cout << "listtennnnnnnnnnnnn" << endl;
                    
                    // cout << "QQQQ" << endl;
                    char start_collect_msg[6] = "START";
                    // cout << "listten1" << endl;
                    MPI_Send(start_collect_msg, 6, MPI_CHAR , collected_tasktracker, 3, MPI_COMM_WORLD);
                    // cout << "listten2" << endl;
                    // cout << "QQ" << " " << collected_tasktracker << endl;


                    MPI_Status status;
                    MPI_Probe(collected_tasktracker, 4, MPI_COMM_WORLD, &status);
                    int count;
                    // cout << "Chechk chekc chekc333333333333333 " << endl;
                    MPI_Get_count(&status,MPI_CHAR,&count);
                    char recv_buf[count];
                    // string recv_s;
                    MPI_Recv(&recv_buf, count, MPI_CHAR, collected_tasktracker, 4, MPI_COMM_WORLD, &status);
                    // cout << "listten3" << endl;
                    // collected_tasktracker++;
                    // cout << "My super_string:" << super_string << endl;
                    // cout << "Recv_buf:" << recv_buf << endl;
                    string other_node_data = recv_buf;
                    size_t pos=0;
                    string word;
                    int check_odd_even = 0;
                    string word_name;
                    int word_count;
                    while ((pos = other_node_data.find(" ")) != string::npos){
                            word = other_node_data.substr(0, pos);
                            if(check_odd_even%2==0){
                                word_name = word;
                                // cout << word << endl;
                            }
                            else{
                                word_count = stoi(word);
                                if (local_file_system.count(word_name) == 0)
                                {
                                    local_file_system[word_name] = word_count;
                                }
                                else
                                {
                                    local_file_system[word_name] = word_count + local_file_system[word_name];
                                }
                            }
                            check_odd_even++;
                            other_node_data.erase(0, pos+1);
                    }
                }
            }
            //print result
            vector<Item> wordcount_list;
            for (auto &item : local_file_system){
                if(item.first != "")
                    wordcount_list.push_back(item);
            }
            bool ASCENDING = true;
            if(ASCENDING){
                sort(wordcount_list.begin(), wordcount_list.end(), [](const Item &item1, const Item &item2) -> bool
                { return item1.first < item2.first; });
            }
            else{
                sort(wordcount_list.begin(), wordcount_list.end(), [](const Item &item1, const Item &item2) -> bool
                { return item1.first > item2.first; });
            }
            
            // //printtttt!
            // for(auto iter_ : local_file_system){
            //     if(iter_.first != ""){
            //         cout << rank << " | " << iter_.first << " | " << iter_.second << endl;
            //     }
            // }

            size_t item_per_file = ceil((double)wordcount_list.size() / num_reducer);
            int reducer_task_id = 1;

            for (size_t i = 0; i < wordcount_list.size(); i += item_per_file)
            {
                size_t size = min(i + item_per_file, local_file_system.size());
                vector<Item> chunk(wordcount_list.begin() + i, wordcount_list.begin() + size);
                ofstream out(output_dir + job_name + "-" + to_string(reducer_task_id) + ".out");
                for (int j = 0; j < chunk.size(); j++)
                {
                    if(chunk.at(j).first != "")
                        out << chunk.at(j).first << " " << chunk.at(j).second << "\n";
                }
                reducer_task_id++;
            }

        }
    }
    auto end = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    cout << "Elapsed Time(ms):" << elapsed.count() << endl;
    pthread_exit(NULL);
    MPI_Allreduce(&local_sum, &global_sum, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    MPI_Finalize();

    return 0;
}
