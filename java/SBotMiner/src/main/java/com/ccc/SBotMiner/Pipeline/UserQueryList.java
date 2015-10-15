package com.ccc.SBotMiner.Pipeline;

import com.ccc.utilities.FilesUtils;
import com.ccc.utilities.Loader;
import com.ccc.utilities.MurmurHash;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * {@link UserQueryList} extracts all queries submitted by respective suspicious
 * users. By providing the mapping suspicious_user-> queries we can build the
 * click matrix in later stages. By providing the mapping query_hash->
 * query_text we can translate the query hashes to the original string in later
 * stages.
 *
 *
 *
 */
public class UserQueryList extends Configured implements Tool {
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new UserQueryList(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "UserQueryList");
        
        job.setJarByClass(UserQueryList.class);
        job.setMapperClass(UserQueryList.UserQueryMapper.class);
        job.setReducerClass(UserQueryList.UserQueryReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(LazyOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        
        MultipleOutputs.addNamedOutput(job, "userqueries", LazyOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "queries", LazyOutputFormat.class, Text.class, Text.class);
        
        Path currentDataPath = new Path(conf.get(Config.PIPELINE_INPUT_CURRENT, "currentdata/"));
        Path pipelinePath = new Path(conf.get(Config.PIPELINE_MAINPATH, "sbotminer/"));
        
        Path tempOutputPath = new Path("userQueryList/");
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(tempOutputPath)) {
            hdfs.delete(tempOutputPath);
        }
        FileInputFormat.addInputPath(job, currentDataPath);
        FileOutputFormat.setOutputPath(job, tempOutputPath);
        
        int res = job.waitForCompletion(true) ? 0 : 1;
        
        if (res == 0) {
            FilesUtils.copySubdirectories(tempOutputPath, pipelinePath, hdfs);
        }
        
        return res;
    }
    
    private static class UserQueryMapper extends Mapper<Text, Text, LongWritable, Text> {
        
        private JSONParser parser = new JSONParser();
        
        private LongWritable outputKey = new LongWritable();
        private Text outputValue = new Text();
        
        private HashSet<Long> userSet;
        
        private double testPeriodStart;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String mainPath = conf.get(Config.PIPELINE_MAINPATH, "sbotminer/");
            String suspuserQCsPath = conf.get(Config.PIPELINE_SUSPUSERLIST, "suspuserlist/");
            int lines = conf.getInt("suspuserqc_lines", 30000000);
            userSet = Loader.loadUserSet(conf, mainPath + suspuserQCsPath, lines);
            testPeriodStart = conf.getDouble(Config.PIPELINE_CURRENT_PERIOD_START, 1412114400);
        }
        
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            
            try {
                JSONObject json = (JSONObject) parser.parse(key.toString());
                JSONObject userData = (JSONObject) json.get("user");
                Long uid = MurmurHash.hash64(userData.get("id").toString());
                String query = json.get("query").toString();
                Double time = (Double) json.get("time");
                if (userSet.contains(uid) && time >= testPeriodStart) {
                    outputKey.set(uid);
                    outputValue.set(query);
                    context.write(outputKey, outputValue);
                }
                
            } catch (ParseException ex) {
                Logger.getLogger(UserQueryList.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private static class UserQueryReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
        
        LongWritable outputKey = new LongWritable();
        private Text outputValue = new Text();
        
        private MultipleOutputs<LongWritable, Text> mu;
        
        private String userqueryPath, queryPath;
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mu = new MultipleOutputs<LongWritable, Text>(context);
            Configuration conf = context.getConfiguration();
            userqueryPath = conf.get(Config.PIPELINE_USERQUERY_DATA, "userquery/");
            queryPath = conf.get(Config.PIPELINE_QUERY_DATA, "query/");
        }
        
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<Long, Integer> submitCounts = new HashMap<Long, Integer>();
            Integer oldCount;
            // count number of numSubmits of every query
            for (Text query : values) {
                Long qHash = MurmurHash.hash64(query.toString());                
                oldCount = submitCounts.get(qHash);
                if (oldCount != null) {
                    submitCounts.put(qHash, oldCount + 1);
                } else {
                    submitCounts.put(qHash, 1);
                }
                
                
                outputKey.set(qHash);
                outputValue.set(query.toString());
                mu.write(outputKey, outputValue, queryPath);
            }

            // extract the <query, numSubmits> pairs to 
            int i = 0;
            Long queries[] = new Long[submitCounts.size()];
            Integer[] numSubmits = new Integer[submitCounts.size()];
            for (Map.Entry<Long, Integer> e : submitCounts.entrySet()) {
                queries[i] = e.getKey();
                numSubmits[i] = e.getValue();
                i += 1;
            }
            outputValue.set(String.format("{\"queries\": %s,\"submits\": %s}", Arrays.toString(queries), Arrays.toString(numSubmits)));
            mu.write(key, outputValue, userqueryPath);
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mu.close();
        }
        
    }
}
