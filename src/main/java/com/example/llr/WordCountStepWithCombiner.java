package com.example.llr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class WordCountStepWithCombiner {
    
    public static class CountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length < 3) return;
            
            try {
                int year = Integer.parseInt(parts[1].trim());
                long count = Long.parseLong(parts[2].trim());
                int decade = (year / 10) * 10;
                context.write(new Text(String.valueOf(decade)), new LongWritable(count));
            } catch (NumberFormatException e) {

            }
        }
    }
    
    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }
    
    public static boolean run(Configuration conf, String input, String output) throws Exception {
        Job job = Job.getInstance(conf, "Job 1: Calculate N (WITH COMBINER)");
        job.setJarByClass(WordCountStepWithCombiner.class);
        job.setMapperClass(CountMapper.class);
        
        job.setCombinerClass(SumReducer.class);
        System.err.println("*** JOB 1 RUNNING WITH COMBINER ***");
        
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        job.setInputFormatClass(SequenceFileInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job.waitForCompletion(true);
    }
}
