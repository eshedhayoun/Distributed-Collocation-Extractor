package com.example.llr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Job3PartialMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length >= 2) {
            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }
}
