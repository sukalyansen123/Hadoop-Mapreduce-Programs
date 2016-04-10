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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 

public class matrixvector {

 public static class Map
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
Configuration conf = context.getConfiguration();
  Text outputkey = new Text();
  Text outputvalue= new Text();
  try{
  //Since we need the whole vector in every single step we send the entire vector input file directly from the hdfs to mapper//
  
                        Path pt=new Path("hdfs://localhost:54310/user/sukalyan/inputdir1/vector.txt");
                        FileSystem fs = FileSystem.get(new Configuration());
                        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));  
                        String[] arr=br.readLine().split(",");

//my vector input file is stored in hdfs://localhost:54310/user/sukalyan/inputdir1/vector.txt//

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

//key is row number and corressponding value is product with vector element//

outputvalue.set(result.toString());
context.write(outputkey,new Text(outputvalue));
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
    Job job = Job.getInstance(conf, "matrixvector");
    job.setJarByClass(matrixvector.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);

    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
