#include <iostream>
#include <fstream>
#include <string>
#include <map>
#include <vector>
#include <algorithm>
#include <cmath>

using namespace std;
typedef pair<string, int> Item;

int main(int argc, char **argv)
{
    // Arguments passing
    string job_name                 = string(argv[1]);
    int num_reducer                 = stoi(argv[2]);
    int delay                       = stoi(argv[3]);
    string input_filename           = string(argv[4]);
    int chunk_size                  = stoi(argv[5]);
    string locality_config_filename = string(argv[6]);
    string output_dir               = string(argv[7]);

    map<string, int> word_count;
    // read words file
    ifstream input_file(input_filename);
    string line;
    while (getline(input_file, line))
    {
        size_t pos = 0;
        string word;
        vector<string> words;
        while ((pos = line.find(" ")) != string::npos)
        {
            word = line.substr(0, pos);
            words.push_back(word);
            if (word_count.count(word) == 0)
            {
                word_count[word] = 1;
            }
            else
            {
                word_count[word]++;
            }

            //done current line delete current line
            line.erase(0, pos + 1);
        }
        
        //push last word of the line to words
        if (!line.empty())
            words.push_back(line);
            if (word_count.count(line) == 0)
            {
                word_count[line] = 1;
            }
            else
            {
                word_count[line]++;
            }
    }

    vector<Item> wordcount_list;
    for (auto &item : word_count)
    {
        wordcount_list.push_back(item);
    }
    sort(wordcount_list.begin(), wordcount_list.end(), [](const Item &item1, const Item &item2) -> bool
              { return item1.first < item2.first; });

    size_t item_per_file = ceil((double)wordcount_list.size() / num_reducer);
    int reducer_task_id = 1;

    for (size_t i = 0; i < wordcount_list.size(); i += item_per_file)
    {
        size_t size = min(i + item_per_file, word_count.size());
        vector<Item> chunk(wordcount_list.begin() + i, wordcount_list.begin() + size);
        ofstream out(output_dir + job_name + "-" + to_string(reducer_task_id) + ".out");
        for (int j = 0; j < chunk.size(); j++)
        {
            out << chunk.at(j).first << " " << chunk.at(j).second << "\n";
        }
        reducer_task_id++;
    }

    return 0;
}
