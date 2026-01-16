package com.example.llr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Job3Reducer extends Reducer<Text, Text, Text, Text> {
    private Map<Integer, Long> N = new HashMap<>();
    private Map<Integer, PriorityQueue<Result>> topByDecade = new TreeMap<>();
    private static final int TOP_K = 100;

    private static class Result implements Comparable<Result> {
        String w1, w2;
        double llr;
        Result(String w1, String w2, double llr) {
            this.w1 = w1;
            this.w2 = w2;
            this.llr = llr;
        }
        public int compareTo(Result o) {
            return Double.compare(this.llr, o.llr); // Min-heap
        }
    }

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        String nDirPath = conf.get("n.path");
        
        System.err.println("=== JOB3 REDUCER SETUP ===");
        System.err.println("Loading N values from: " + nDirPath);
        
        Path nDir = new Path(nDirPath).getParent();
        FileSystem fs = nDir.getFileSystem(conf);
        
        FileStatus[] files = fs.listStatus(nDir, new PathFilter() {
            public boolean accept(Path path) {
                return path.getName().startsWith("part-r-");
            }
        });
        
        System.err.println("Found " + files.length + " N value files");
        
        for (FileStatus file : files) {
            FSDataInputStream in = fs.open(file.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));
            
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length >= 2) {
                    int decade = Integer.parseInt(parts[0].trim());
                    long total = Long.parseLong(parts[1].trim());
                    N.put(decade, total);
                    System.err.println("  Loaded: decade=" + decade + " N=" + total);
                }
            }
            reader.close();
        }
        
        System.err.println("Total decades loaded: " + N.size());
        System.err.println("======================");
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        String[] keyParts = key.toString().split(":");
        if (keyParts.length < 2) {
            System.err.println("SKIP: Invalid key format: " + key);
            return;
        }
        
        int decade = Integer.parseInt(keyParts[0]);
        String w2 = keyParts[1];

        List<String> valueList = new ArrayList<>();
        for (Text val : values) {
            valueList.add(val.toString());
        }

        long c2 = -1;
        for (String s : valueList) {
            if (s.startsWith("c2:")) {
                c2 = Long.parseLong(s.substring(3));
                break;
            }
        }

        if (c2 <= 0) {
            System.err.println("SKIP: No c2 for " + key);
            return;
        }
        
        if (!N.containsKey(decade)) {
            System.err.println("SKIP: No N for decade " + decade);
            return;
        }
        
        long n = N.get(decade);

        int processed = 0;
        for (String s : valueList) {
            if (s.startsWith("partial:")) {
                String payload = s.substring(8);
                String[] parts = payload.split(",");
                if (parts.length < 3) continue;

                String w1 = parts[0];
                long c1 = Long.parseLong(parts[1]);
                long c12 = Long.parseLong(parts[2]);

                double llr = LLRUtils.calculateLLR(c1, c2, c12, n);
                
                if (Double.isNaN(llr) || Double.isInfinite(llr)) {
                    continue;
                }

                topByDecade.putIfAbsent(decade, new PriorityQueue<>(TOP_K + 1));
                PriorityQueue<Result> pq = topByDecade.get(decade);
                pq.offer(new Result(w1, w2, llr));
                if (pq.size() > TOP_K) {
                    pq.poll();
                }
                
                processed++;
            }
        }
        
        if (Math.random() < 0.001) {
            System.err.println("PROCESSED: " + key + " → " + processed + " bigrams, c2=" + c2 + ", N=" + n);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        System.err.println("=== JOB3 REDUCER CLEANUP ===");
        System.err.println("Outputting top-100 for each decade...");
        
        for (Map.Entry<Integer, PriorityQueue<Result>> entry : topByDecade.entrySet()) {
            int decade = entry.getKey();
            List<Result> results = new ArrayList<>(entry.getValue());
            
            results.sort((a, b) -> Double.compare(b.llr, a.llr));
            
            System.err.println("Decade " + decade + ": outputting " + results.size() + " results");
            
            for (Result r : results) {
                context.write(
                    new Text("Decade: " + decade + ", Pair: " + r.w1 + "-" + r.w2),
                    new Text(String.valueOf(r.llr))
                );
            }
        }
        
        System.err.println("Total decades processed: " + topByDecade.size());
        System.err.println("===========================");
    }
}
