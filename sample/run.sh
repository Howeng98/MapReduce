#!/bin/bash
#Run this in terminal
#+ Command to compile c++ program. Here i used common one
clear
make
srun -N2 -c5 ./hw4 "TEST01" 7 3 "../testcases/01.word" 2 "../testcases/01.loc" "../outputs/"
exit 0
