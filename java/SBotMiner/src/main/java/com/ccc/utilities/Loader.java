package com.ccc.utilities;

import com.ccc.SBotMiner.Pipeline.MatrixBuilder;
import com.ccc.writables.LongPairWritable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author Tom
 */
public class Loader {

    public static HashSet<Long> loadUserSet(Configuration conf, String path, int lines) throws IOException {
        HashSet<Long> set;
        if (lines > 0) {
            set = new HashSet<Long>(lines);
        } else {
            set = new HashSet<Long>();
        }
        BufferedReader br;
        String line;
        String[] data;
        Path pt = new Path(path);
        FileSystem hdfs = FileSystem.get(conf);
        for (FileStatus fs : hdfs.listStatus(pt)) {
            if (fs.isFile()) {
                br = new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath())));
                while ((line = br.readLine()) != null) {
                    data = line.split("\t");
                    set.add(Long.parseLong(data[0]));
                }
                br.close();
            }
        }

        return set;

    }

    /*
     * Will load data in format 'uid_hash query_hash1:result_click11 query_hash2:result_click21'
     */
    public static HashMap<Long, LongPairWritable[]> loadUserQCMap(Configuration conf, String path, int lines) throws IOException {
        HashMap<Long, LongPairWritable[]> map;
        if (lines > 0) {
            map = new HashMap<Long, LongPairWritable[]>(lines);
        } else {
            map = new HashMap<Long, LongPairWritable[]>();
        }
        BufferedReader br;
        String line;
        String[] data;
        JSONParser parser = new JSONParser();
        JSONObject qcdata;
        JSONArray queries, urls;
        FileSystem hdfs = FileSystem.get(conf);
        Path pt = new Path(path);
        for (FileStatus fs : hdfs.listStatus(pt)) {
            if (fs.isFile()) {
                br = new BufferedReader(new InputStreamReader(hdfs.open(fs.getPath())));
                while ((line = br.readLine()) != null) {
                    try {
                        data = line.split("\t");
                        qcdata = (JSONObject) parser.parse(data[1]);
                        queries = (JSONArray) qcdata.get("queries");
                        urls = (JSONArray) qcdata.get("urls");
                        LongPairWritable[] qcSet = new LongPairWritable[queries.size()];
                        for (int i = 0; i < qcSet.length; i++) {
                            qcSet[i] = new LongPairWritable((Long) queries.get(i), (Long) urls.get(i));
                        }
                        map.put(Long.parseLong(data[0]), qcSet); // K: user V: qcpairs 

                    } catch (ParseException ex) {
                        Logger.getLogger(MatrixBuilder.class
                                .getName()).log(Level.SEVERE, null, ex);
                    }
                }
                br.close();
            }
        }

        return map;

    }
    /*
     * Will load qc pair from file in format 'query_hash url_hash1 url_hash2'
     */

    public static HashMap<Long, HashSet<Long>> loadQCMap(Configuration conf, String path) throws IOException, ParseException {
        HashMap<Long, HashSet<Long>> map = new HashMap<Long, HashSet<Long>>();
        JSONParser p = new JSONParser();
        JSONObject lineData;
        Path pt = new Path(path);
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        Long url;
        while ((line = br.readLine()) != null) {
            lineData = (JSONObject) p.parse(line);
            Long query = (Long) lineData.get("query");
            url = (Long) lineData.get("url");
            HashSet<Long> urls = map.get(query);
            if (urls == null) {
                urls = new HashSet<Long>();
            }
            urls.add(url);
            map.put(query, urls);
        }
        br.close();

        return map;
    }
}
