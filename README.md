# Hadoop-Mapreduce-Programs

Implementations of some common map-reduce algorithms in Java to be run on Hadoop.


# MatrixMultiply.java

The program achieves the task of multiplying two matrices in ONE STEP ie by a single map-reduce job.
Here the size of the matrices are set by default as 2x5 and 5x3 which maybe changed by changing the values of m,n,p in the main function and subsequently changing the array size in the Reducer part of the program.

#InvertIndex.java


This program uses map reduce to list the words and the files they belong to from a list of input files.Here the input given were the files input1.txt, input2.txt, input3.txt and the output found is as shown in the file InverseIndexingOUTPUT.txt

#DistributedGrep.java


Grep searches target files for a particular keyword and prints out all the lines and the files that contains that keyword.
The target files in this case are grepinput1.txt, grepinput2.txt and grepinput3.txt. 

The main (String[] args) receives a keyword in args .

The main goal of this map-reduce program is to print out all the lines that contain this keyword.We need to pass the keyword to the map() function for which we use the Configuration class.
