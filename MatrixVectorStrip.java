import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import java.util.Iterator;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

//This is the program which may be used in case the vector doesnt fit into main memory,the matrix and vector are divided intp equal number of strips// 
//Each strip make up an input file,in this case 2 strips,hence 2 mappers for each strip//
//The output of the mappers form input of a single reducer from which we get final output//

public class MatrixVectorStrip{

//Mapper Class 1//

  public static class Map1
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
Configuration conf = context.getConfiguration();
  Text outputkey = new Text();
  Text outputvalue= new Text();
try{
Path pt=new Path("hdfs://localhost:54310/user/sukalyan/inputdir2/vector1.txt");
//location of my first part of the vector//
                        FileSystem fs = FileSystem.get(new Configuration());
                        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));  

  
String[] arr=br.readLine().split(",");
Integer val=new Integer(0);
Integer vec= new Integer(0);
Integer result= new Integer(0);
Integer col=new Integer(0);

String[] line=value.toString().split(",");
col=Integer.parseInt(line[1]);
val=Integer.parseInt(line[2]);
outputkey.set(line[0]);
vec=Integer.parseInt(arr[col]);
result=val*vec;


outputvalue.set(result.toString());
context.write(outputkey,new Text(outputvalue));
}catch(Exception e){}  
    }
  }
  
//Mapper Class 2//

public static class Map2
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
Configuration conf = context.getConfiguration();
  Text outputkey1 = new Text();
  Text outputvalue1= new Text();
try{
Path pt1=new Path("hdfs://localhost:54310/user/sukalyan/inputdir3/vector2.txt");
    //location of my second part of vector//
                        FileSystem fs1 = FileSystem.get(new Configuration());
                        BufferedReader br1=new BufferedReader(new InputStreamReader(fs1.open(pt1)));  

  
String[] arr1=br1.readLine().split(",");
Integer val1=new Integer(0);
Integer vec1= new Integer(0);
Integer result1= new Integer(0);
Integer col1=new Integer(0);

String[] line1=value.toString().split(",");
col1=Integer.parseInt(line1[1]);
val1=Integer.parseInt(line1[2]);
outputkey1.set(line1[0]);
vec1=Integer.parseInt(arr1[col1-2]);
result1=val1*vec1;


outputvalue1.set(result1.toString());
context.write(outputkey1,new Text(outputvalue1));
}catch(Exception e){}  
    }
  }
  public static class Reduce
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    Integer sum= new Integer(0);
Integer add= new Integer(0);
for(Text val:values){
add=Integer.parseInt(val.toString());
sum=sum+add;
}
context.write(key,new Text(sum.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "matrixvector2");
    job.setJarByClass(MatrixVectorStrip.class);
    
    
   //MultipleInputs Class used for taking multiple input//
   
   MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,Map1.class);
    MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,Map2.class);
  FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
   

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
