import java.io.IOException;
import java.util.*;
 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class MatrixMultiply {
 
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int m = Integer.parseInt(conf.get("m"));
            int p = Integer.parseInt(conf.get("p"));
            String[] line = value.toString().split(",");
            Text reducerKey = new Text();
            Text reducerValue = new Text();
            if (line[0].equals("A")) {
                for (int k = 0; k < p; k++) {
                    reducerKey.set(line[1] + "," + k);
                    reducerValue.set("A," + line[2] + "," + line[3]);
                    context.write(reducerKey,reducerValue);
                }
            } else {
                for (int i = 0; i < m; i++) {
                    reducerKey.set(i + "," + line[2]);
                    reducerValue.set("B," + line[1] + "," + line[3]);
                    context.write(reducerKey, reducerValue);
                }
            }
        }
    }
 
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] value;
            int n = Integer.parseInt(context.getConfiguration().get("n"));
            int[] a=new int[n];
	int[] b= new int[n];
            for (Text val : values) {
                value = val.toString().split(",");

                if (value[0].equals("A")) {
                   int col=Integer.parseInt(value[1]);
                   a[col]=Integer.parseInt(value[2]);
                } else {
int row=Integer.parseInt(value[1]);
                   b[row]=Integer.parseInt(value[2]);
                                    }
            }
            
            int result = 0;
                        for (int j = 0; j < n; j++) {
                               result += a[j] * b[j];
            }
            if (result != 0) {
                context.write(null, new Text(key.toString() + "," + Integer.toString(result)));
            }
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // A is an m-by-n matrix; B is an n-by-p matrix.
        conf.set("m", "2");
        conf.set("n", "5");
        conf.set("p", "3");
 
        Job job = new Job(conf, "MatrixMatrixMultiplicationOneStep");
        job.setJarByClass(MatrixMultiply.class);
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
