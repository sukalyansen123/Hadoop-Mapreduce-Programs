import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 

public class DistributedGrep {

  public static class Map
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
  Configuration conf = context.getConfiguration();
  Text outputkey = new Text();
  Text outputvalue= new Text();
      FileSplit fileSplit = ( FileSplit )context.getInputSplit( );
  String filename = "" + fileSplit.getPath( ).getName( );
  String a=conf.get("keyword");
  String keyword = ".*?"+a+".*?";
  outputkey.set(filename);
  String line=value.toString();
    if(line.matches(keyword)){
      outputvalue.set(line);
      context.write(outputkey,outputvalue);
      }
    }
  }

  public static class Reduce
       extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
    String result="";
for(Text val:values){
result=result + val.toString() + "\n"+"\t"+"\t";
context.write(key,new Text(result));
}
    }
  }

  public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
String keyword= "keyword";  
if(args.length > 2) keyword = args[2];
conf.set( "keyword", keyword );  
Job job = Job.getInstance(conf, "distributedgrep");
    job.setJarByClass(DistributedGrep.class);
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
