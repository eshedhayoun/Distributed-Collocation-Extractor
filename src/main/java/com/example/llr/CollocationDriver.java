package com.example.llr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CollocationDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: <1gram> <2gram> <output> <stopwords>");
            return -1;
        }

        String oneGram = args[0];
        String twoGram = args[1];
        String output = args[2];
        String stopwords = args[3];

        Configuration conf = getConf();
        conf.set("stop.words.path", stopwords);

        // JOB 1: Calculate N (total words per decade)
        System.err.println("=== JOB 1: Calculate N ===");
        String job1Out = output + "/step1_N";
        if (!WordCountStep.run(conf, oneGram, job1Out)) {
            System.err.println("JOB 1 FAILED!");
            return 1;
        }
        System.err.println("JOB 1 COMPLETED: " + job1Out);

        // JOB 2: Join bigrams with c1
        System.err.println("=== JOB 2: Join c1 ===");
        String job2Out = output + "/step2_partial";
        Configuration conf2 = new Configuration(conf);
        conf2.set("mapreduce.reduce.memory.mb", "4096");
        conf2.set("mapreduce.reduce.java.opts", "-Xmx3072m");
        
        Job job2 = Job.getInstance(conf2, "Job 2: Join c1");
        job2.setJarByClass(CollocationDriver.class);
        job2.setReducerClass(Job2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setNumReduceTasks(20);
        
        MultipleInputs.addInputPath(job2, new Path(oneGram), SequenceFileInputFormat.class, Job2UnigramMapper.class);
        MultipleInputs.addInputPath(job2, new Path(twoGram), SequenceFileInputFormat.class, Job2BigramMapper.class);
        
        FileOutputFormat.setOutputPath(job2, new Path(job2Out));
        if (!job2.waitForCompletion(true)) {
            System.err.println("JOB 2 FAILED!");
            return 1;
        }
        System.err.println("JOB 2 COMPLETED: " + job2Out);

        // JOB 3: Calculate LLR with CUSTOM PARTITIONER
        System.err.println("=== JOB 3: Calculate LLR (with DecadePartitioner) ===");
        String job3Out = output + "/final_result";
        Configuration conf3 = new Configuration(conf);
        conf3.set("n.path", job1Out + "/part-r-00000");
        conf3.set("mapreduce.reduce.memory.mb", "4096");
        conf3.set("mapreduce.reduce.java.opts", "-Xmx3072m");
        
        Job job3 = Job.getInstance(conf3, "Job 3: Calculate LLR");
        job3.setJarByClass(CollocationDriver.class);
        job3.setReducerClass(Job3Reducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        
        job3.setPartitionerClass(DecadePartitioner.class);
        
        job3.setNumReduceTasks(50);
        
        MultipleInputs.addInputPath(job3, new Path(job2Out), TextInputFormat.class, Job3PartialMapper.class);
        MultipleInputs.addInputPath(job3, new Path(oneGram), SequenceFileInputFormat.class, Job3UnigramMapper.class);
        
        FileOutputFormat.setOutputPath(job3, new Path(job3Out));
        if (!job3.waitForCompletion(true)) {
            System.err.println("JOB 3 FAILED!");
            return 1;
        }
        System.err.println("JOB 3 COMPLETED: " + job3Out);

        System.err.println("=== ALL JOBS COMPLETED SUCCESSFULLY ===");
        System.err.println("Final output: " + job3Out);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CollocationDriver(), args);
        System.exit(exitCode);
    }
}
