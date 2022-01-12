# MapReduce
CS542200 Parallel Programming HW4. Implement MapReduce by MPI + Pthread. 

Rememeber to create the missing folder (e.g. outputs/).

## How to compile
```
make 
```

## How to run 
```
srun -N2 -c5 ./hw4 "TEST01" 7 3 "../testcases/01.word" 2 "01.loc" "../outputs/test01/"
```
