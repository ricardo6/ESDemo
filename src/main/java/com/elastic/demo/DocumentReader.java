package com.elastic.demo;
import com.google.common.collect.Lists;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class DocumentReader {
    public List<String> getDocumentsFromFile(String filename) {
        List<String> lines = Lists.newArrayList();
        try {
            ClassLoader classloader = Thread.currentThread().getContextClassLoader();
            InputStream inputStream = classloader.getResourceAsStream(filename);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            for(String line; (line = reader.readLine()) != null; ) {
                lines.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
    }
}
