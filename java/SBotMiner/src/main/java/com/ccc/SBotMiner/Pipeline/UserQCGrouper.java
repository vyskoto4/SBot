package com.ccc.SBotMiner.Pipeline;

import com.ccc.writables.LongPairWritable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * {@link UserQCGrouper} parses the raw suspicious user list from {@link QCPairExtractor}
 * and groups all qc pairs to which a suspicious user belongs to.
 * 
 */
public class UserQCGrouper extends Configured implements Tool {

    public static enum SUSPICIOUS_USER {

        NUM_SUSPICIOUS_USERS,
        NUM_SUSPICIOUS_USERS_QCPAIRS
    };

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new UserQCGrouper(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "SuspiciousUserGrouper");

        job.setJarByClass(UserQCGrouper.class);
        job.setMapperClass(UserQCGrouper.UserMapper.class);
        job.setReducerClass(UserQCGrouper.UserReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongPairWritable.class);

        String mainPath = conf.get(Config.PIPELINE_MAINPATH, "sbotminer/");
        String nonUniqueSuspuserListInput = mainPath + conf.get(Config.PIPELINE_SUSPUSERLIST_RAW, "suspuserlist_/");
        String suspuserListOutput = mainPath + conf.get(Config.PIPELINE_SUSPUSERLIST, "suspuserlist/");

        FileInputFormat.addInputPath(job, new Path(nonUniqueSuspuserListInput));
        FileOutputFormat.setOutputPath(job, new Path(suspuserListOutput));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class UserMapper extends Mapper<Text, Text, LongWritable, LongPairWritable> {

        private JSONParser parser = new JSONParser();

        private LongPairWritable qcPair = new LongPairWritable();
        private LongWritable uid = new LongWritable();

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            try {
                JSONObject json = (JSONObject) parser.parse(value.toString());
                Long uidHash = Long.parseLong(key.toString());
                Long queryHash = Long.parseLong(json.get("query").toString());
                Long urlHash = Long.parseLong(json.get("url").toString());
                uid.set(uidHash);
                qcPair.setLongs(queryHash, urlHash);
                context.write(uid, qcPair);
            } catch (ParseException ex) {
                Logger.getLogger(UserQCGrouper.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    private static class UserReducer extends Reducer<LongWritable, LongPairWritable, Text, Text> {

        private Text outputValue = new Text();
        private Text outputKey = new Text();

        @Override
        protected void reduce(LongWritable uid, Iterable<LongPairWritable> qcPairs, Context context) throws IOException, InterruptedException {
            ArrayList<Long> targetQueries = new ArrayList<Long>();
            ArrayList<Long> targetUrls= new ArrayList<Long>();
            for (LongPairWritable l : qcPairs) {
                targetQueries.add(l.getFirst());
                targetUrls.add(l.getSecond());
                context.getCounter(SUSPICIOUS_USER.NUM_SUSPICIOUS_USERS_QCPAIRS).increment(1);
            }
            outputKey.set(String.format("%d", uid.get()));
            outputValue.set(String.format("{\"target_queries\": %s, \"target_urls\": %s}", Arrays.toString(targetQueries.toArray()), Arrays.toString(targetUrls.toArray())));
            context.write(outputKey, outputValue);
            context.getCounter(SUSPICIOUS_USER.NUM_SUSPICIOUS_USERS).increment(1);
        }
    }

}
