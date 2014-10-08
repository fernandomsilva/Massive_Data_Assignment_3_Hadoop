/*
Student: Fernando de Mesentier Silva
N14662208 - NetID: fdm240
*/
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
        
public class TopK {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
			String new_token = tokenizer.nextToken();
			if (new_token.length() == 7) {
            	//word.set(tokenizer.nextToken());
				word.set(new_token);
	            context.write(word, one);
			}
        }
    }
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	private TreeMap<Integer, String> TopK = new TreeMap<Integer, String>();
	private Integer N = 100;

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
		
		TopK.put(sum, key.toString());
		if (TopK.size() > N) {
			TopK.remove(TopK.firstKey());
		}
        //context.write(key, new IntWritable(sum));
    }
    protected void cleanup(Context context)
                      throws IOException, InterruptedException {
 
        //for ( Text key : TopK.values() ) {
        //    context.write(key, );
        //}
		Text word = new Text();
		for (java.util.Map.Entry<Integer, String> entry : TopK.descendingMap().entrySet()) {
			word.set(entry.getValue());
			context.write(word, new IntWritable(entry.getKey()));
		}
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
        Job job = new Job(conf, "wordcount");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(1);
    job.setJarByClass(TopK.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}
