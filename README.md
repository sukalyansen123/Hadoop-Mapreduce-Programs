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

#matrixvector.java

The goal of this program is to multiply a matrix with a vector where the matrix is stored in two separate files matrix.txt and vector in vector.txt.

The matrix is specified as :
row num,col num,value

And the vector as the values separated by commas in a single line.

The output of the program is in matrixvectorOUTPUT.txt

#MatrixVectorStrip.java

A variant of the above program.This does the same thing with one difference.Here the main goal is to find the Matrix vector product in case the input vector file size is more than the allotted main memory bock size.
The matrix and vector are divided into equal number of strips and are stored in separate input files where they fit easily.Then instead of a single mapper ,one mapper is used each for a single input file,and all the outputs are fed to a single reducer.
Here I have used 2 strips each for matrix and vector , which are the files matrix1.txt,matrix2.txt,vector1.txt,vector2.txt. The output is in MatrixVectorStripOutput.txt.


To get a better insight into the algorithm refer to the following link ,page 32 : http://infolab.stanford.edu/~ullman/mmds/book.pdf


