package com.example.llr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class DecadePartitioner extends Partitioner<Text, Text> {
    
    @Override
    public int getPartition(Text key, Text value, int numPartitions) {

        String keyStr = key.toString();
        
        int colonIndex = keyStr.indexOf(':');
        if (colonIndex == -1) {
            System.err.println("WARNING: Invalid key format (no colon): " + keyStr);
            return 0;
        }
        
        try {
            String decadeStr = keyStr.substring(0, colonIndex);
            int decade = Integer.parseInt(decadeStr);
            
            int partition = (decade & Integer.MAX_VALUE) % numPartitions;
            
            if (Math.random() < 0.0001) {
                System.err.println("PARTITION: decade=" + decade + " → partition=" + partition + " (key=" + keyStr + ")");
            }
            
            return partition;
            
        } catch (NumberFormatException e) {
            System.err.println("ERROR: Could not parse decade from key: " + keyStr);
            return 0;
        }
    }
}
