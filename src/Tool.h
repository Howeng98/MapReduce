#ifndef TOOL_H
#define TOOL_H

#include <map>
#include <vector>
#include <utility>

typedef std::map<std::string, int> Count;
typedef std::vector<std::string> Word;
typedef std::pair<int, int> Chunk;
typedef std::pair<std::string, int> Item;
typedef std::vector<Item> Total;
typedef std::map<std::string, std::vector<int> > Collect;

#endif