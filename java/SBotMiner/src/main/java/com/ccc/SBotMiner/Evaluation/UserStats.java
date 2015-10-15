package com.ccc.SBotMiner.Evaluation;

import com.ccc.SBotMiner.Pipeline.Config;
import com.ccc.utilities.Loader;
import com.ccc.utilities.MurmurHash;
import com.ccc.writables.UserStatsWritable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
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
 * @author Tom
 */
public class UserStats extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new UserStats(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "SuspiciousUserStats");

        job.setJarByClass(UserStats.class);
        job.setMapperClass(UserStats.StatsMapper.class);
        job.setReducerClass(UserStats.StatsReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(UserStatsWritable.class);

        FileSystem hdfs = FileSystem.get(conf);
        Path inputPath = new Path(conf.get(Config.PIPELINE_INPUT_CURRENT, "currentdata/"));
        Path outputPath = new Path(conf.get(Config.PIPELINE_USER_STATS), "userstats/");

        if (hdfs.exists(outputPath)) {
            hdfs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        int res = job.waitForCompletion(true) ? 0 : 1;

        return res;
    }

    private static class StatsMapper extends Mapper<Text, Text, LongWritable, UserStatsWritable> {

        private JSONParser parser = new JSONParser();

        private LongWritable outputKey = new LongWritable();
        private UserStatsWritable outputValue = new UserStatsWritable();

        private HashSet<Long> userSet;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int lines = conf.getInt(Config.PIPELINE_SUSPUSERLIST_LINES, 20000000);
            String mainPath = conf.get(Config.PIPELINE_MAINPATH, "sbotminer/");
            String suspuserQCsPath = conf.get(Config.PIPELINE_SUSPUSERLIST, "suspuserlist/");
            userSet = Loader.loadUserSet(conf, mainPath + suspuserQCsPath, lines);
        }

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            boolean rss = false, js = false, limit = false;
            String ua, ip;
            Long creationDate;
            Long uidHash;
            try {
                JSONObject json = (JSONObject) parser.parse(key.toString());
                JSONObject userData = (JSONObject) json.get("user");
                String query = json.get("query").toString();
                String uid = userData.get("id").toString();
                uidHash = MurmurHash.hash64(uid);
                if (userSet.contains(uidHash)) {
                    Double reqTime = Double.valueOf(json.get("time").toString());
                    if (json.containsKey("ua")) {
                        ua = json.get("ua").toString();
                    } else {
                        ua = "";
                    }
                    ip = json.get("ip").toString();
                    creationDate = (Long) userData.get("created");
                    if (json.get("rss") != null) {
                        rss = true;
                    }
                    if (!json.get("limit").toString().equals("10")) {
                        limit = true;
                    }
                    if (Long.parseLong(json.get("js_events").toString()) == 0) {
                        js = true;
                    }
                    outputKey.set(uidHash);
                    outputValue.set(query, js, limit, rss, ip, ua, creationDate, reqTime);
                    context.write(outputKey, outputValue);
                }
            } catch (ParseException ex) {
                Logger.getLogger(UserStats.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private static class StatsReducer extends Reducer<LongWritable, UserStatsWritable, Text, Text> {

        private Text outputValue = new Text();
        private Text outputKey = new Text();

        @Override
        protected void reduce(LongWritable key, Iterable<UserStatsWritable> values, Context context) throws IOException, InterruptedException {
            double rss = 0, js = 0, limit = 0;
            HashSet<Long> uas = new HashSet<Long>();
            HashSet<Long> ips = new HashSet<Long>();
            HashSet<Long> queries = new HashSet<Long>();
            Long creationDate = -1L;
            Double lastReqTime = 0.0;
            int reqs = 0;
            for (UserStatsWritable stats : values) {
                queries.add(MurmurHash.hash64(stats.getQuery()));
                js += stats.isJs() ? 1 : 0;
                rss += stats.isRss() ? 1 : 0;
                limit += stats.isLimit() ? 1 : 0;
                uas.add(MurmurHash.hash64(stats.getUa()));
                ips.add(MurmurHash.hash64(stats.getIp()));
                creationDate = stats.getCreationDate();
                reqs += 1;
                if (lastReqTime < stats.getRequestDate()) {
                    lastReqTime = stats.getRequestDate();
                }
            }
            js /= reqs;
            limit /= reqs;
            rss /= reqs;
            outputKey.set(String.valueOf(key.get()));
            outputValue.set(String.format("{\"js\": %.3f,\"rss\": %.3f,\"limit\": %.3f,\"queries\": %s,\"ua\": %s,\"ip\": %s,\"created\": %d, \"reqs\": %d, \"tspan\": %.2f}", js, rss, limit, Arrays.toString(queries.toArray()), Arrays.toString(uas.toArray()), Arrays.toString(ips.toArray()), creationDate, reqs, lastReqTime - creationDate));
            context.write(outputKey, outputValue);
        }
    }

}
