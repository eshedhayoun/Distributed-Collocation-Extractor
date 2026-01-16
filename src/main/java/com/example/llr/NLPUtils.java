package com.example.llr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

public class NLPUtils {
    private static final int MIN_LENGTH = 2;

    public static Set<String> loadStopWords(Configuration conf) {
        String path = conf.get("stop.words.path");
        Set<String> stopWords = new HashSet<>();
        if (path == null) return stopWords;
        
        try {
            FileSystem fs = new Path(path).getFileSystem(conf);
            FSDataInputStream in = fs.open(new Path(path));
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    stopWords.add(trimmed.toLowerCase(Locale.ROOT));
                }
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stopWords;
    }

    public static boolean isValid(String word, Set<String> stopWords) {
        if (word == null || word.length() < MIN_LENGTH) return false;
        
        String lower = word.toLowerCase(Locale.ROOT);
        if (stopWords.contains(lower)) return false;
        
        return word.matches(".*\\p{L}.*");
    }
}
