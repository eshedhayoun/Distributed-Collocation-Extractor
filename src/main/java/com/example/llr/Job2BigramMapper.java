package com.example.llr;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Set;

public class Job2BigramMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Set<String> stopWords;

    @Override
    protected void setup(Context context) {
        stopWords = NLPUtils.loadStopWords(context.getConfiguration());
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");
        if (parts.length < 3) return;
        
        String[] words = parts[0].trim().split("\\s+");
        if (words.length < 2) return;
        
        String w1 = words[0].trim();
        String w2 = words[1].trim();
        
        if (!NLPUtils.isValid(w1, stopWords)) return;
        if (!NLPUtils.isValid(w2, stopWords)) return;
        
        try {
            int year = Integer.parseInt(parts[1].trim());
            long count = Long.parseLong(parts[2].trim());
            int decade = (year / 10) * 10;
            
            context.write(
                new Text(decade + ":" + w1),
                new Text("bigram:" + w2 + "," + count)
            );
        } catch (NumberFormatException e) {

        }
    }
}
