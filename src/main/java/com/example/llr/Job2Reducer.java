package com.example.llr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Job2Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        String[] keyParts = key.toString().split(":");
        if (keyParts.length < 2) return;
        
        String decade = keyParts[0];
        String w1 = keyParts[1];

        List<String> valueList = new ArrayList<>();
        for (Text val : values) {
            valueList.add(val.toString());
        }

        long c1 = -1;
        for (String s : valueList) {
            if (s.startsWith("c1:")) {
                c1 = Long.parseLong(s.substring(3));
                break;
            }
        }

        if (c1 <= 0) return;

        for (String s : valueList) {
            if (s.startsWith("bigram:")) {
                String payload = s.substring(7);
                String[] parts = payload.split(",");
                if (parts.length < 2) continue;

                String w2 = parts[0];
                String c12 = parts[1];

                context.write(
                    new Text(decade + ":" + w2),
                    new Text("partial:" + w1 + "," + c1 + "," + c12)
                );
            }
        }
    }
}
