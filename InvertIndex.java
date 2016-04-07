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
 
public class InvertIndex {
 
    public static class Map extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String line = value.toString();
            Text outputkey = new Text();
            Text outputvalue = new Text();

//using FileSplit class to get the filenames//

           FileSplit split = (FileSplit)(context.getInputSplit());
           String fileName = split.getPath().getName();

// output value is the filename in which a word occurs and the word is the outputkey//
	   outputvalue=new Text(fileName);
	   StringTokenizer itr= new StringTokenizer(line);
	   while(itr.hasMoreTokens()){
		outputkey.set(itr.nextToken());
                context.write(outputkey,outputvalue);

}
        }
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           

// initialised boolean start to true to detect start of output line so that no "," is placed at the beginning// 

 boolean start=true;
	StringBuilder list= new StringBuilder();
	for(Text value: values){
if(!start){
list.append(",");
}
start=false;
list.append(value.toString());

}
context.write(key,new Text(list.toString()));
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

 
        Job job = new Job(conf, "InverseIndexing");
        job.setJarByClass(InvertIndex.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
 
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
 
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        job.waitForCompletion(true);
    }
}
