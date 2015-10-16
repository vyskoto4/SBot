package com.ccc.SBotMiner.Pipeline;

import com.ccc.SERPHistogram.HistElement;
import com.ccc.utilities.MurmurHash;
import com.ccc.SERPHistogram.SERPHistogram;
import com.ccc.utilities.FilesUtils;
import com.ccc.writables.ResultClickWritable;
import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import java.util.Map.Entry;
import org.apache.hadoop.fs.FileSystem;
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
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author Tom
 *
 *
 *
 */
public class QCPairExtractor extends Configured implements Tool {

    public static enum QC_GROUPS {

        NUM_QCGROUPS,
        NUM_NON_UNIQUE_GROUP_USERS
    };

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new QCPairExtractor(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "QCPairExtractor");

        job.setJarByClass(QCPairExtractor.class);
        job.setMapperClass(QCPairExtractor.QueryResultClickMapper.class);
        job.setReducerClass(QCPairExtractor.QueryResultClickReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(LazyOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
        job.setOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ResultClickWritable.class);
        job.setNumReduceTasks(100);

        MultipleOutputs.addNamedOutput(job, "suspusers", LazyOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "qcpairs", LazyOutputFormat.class, Text.class, Text.class);

        Path historyDataPath = new Path(conf.get(Config.PIPELINE_INPUT_HISTORY, "historydata/"));
        Path currenDataPath = new Path(conf.get(Config.PIPELINE_INPUT_CURRENT, "currentdata/"));

        Path pipelinePath = new Path(conf.get(Config.PIPELINE_MAINPATH, "sbotminer/"));
        Path tempOutputPath = new Path("qcpairextractor/");

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(tempOutputPath)) {
            hdfs.delete(tempOutputPath);
        }

        FileInputFormat.addInputPath(job, historyDataPath);
        FileInputFormat.addInputPath(job, currenDataPath);
        FileOutputFormat.setOutputPath(job, tempOutputPath);

        int res = job.waitForCompletion(true) ? 0 : 1;
        if (res == 0) {
            FilesUtils.copySubdirectories(tempOutputPath, pipelinePath, hdfs);
            hdfs.delete(tempOutputPath);
        }
        return res;
    }

    private static class QueryResultClickMapper extends Mapper<Text, Text, Text, ResultClickWritable> {

        private JSONParser parser = new JSONParser();

        private Text outputKey = new Text();
        private ResultClickWritable outputValue = new ResultClickWritable();

        /**
         * The map method processes the input query logs in JSON format. If a
         * SERP result was clicked, the method emits Text-ResultClickWritable
         * key-value pairs. The text represents the submitted query, the value
         * represents hash of clicked URL, hash of user ID and timestamp. If no
         * result was clicked, the above described key-value pair is also
         * emitted, but with the URL hash replaced by 0.
         *
         * @param key the JSON format line
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            try {
                JSONObject json = (JSONObject) parser.parse(key.toString());
                Double time = Double.valueOf(json.get("time").toString());
                String query = json.get("query").toString();
                JSONObject userData = (JSONObject) json.get("user");
                Long uidHash = MurmurHash.hash64(userData.get("id").toString());
                Long urlHash;

                outputKey.set(query);

                boolean resultsClicked = false;
                JSONArray results = (JSONArray) json.get("result");
                for (Object resultObject : results) {
                    JSONObject result = (JSONObject) resultObject;
                    JSONArray clicks = (JSONArray) result.get("clicks");
                    if (clicks != null) {
                        resultsClicked = true;
                        urlHash = MurmurHash.hash64(result.get("url").toString());
                        outputValue.set(uidHash, urlHash, time);
                        for (int j = 0; j < clicks.size(); j++) {  // we add all clicks associated with value
                            context.write(outputKey, outputValue);
                        }
                    }
                }

                if (!resultsClicked) {
                    urlHash = Config.NO_CLICK;
                    outputValue.set(uidHash, urlHash, time);
                    context.write(outputKey, outputValue);
                }

            } catch (ParseException ex) {
                Logger.getLogger(QCPairExtractor.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    private static class QueryResultClickReducer extends Reducer<Text, ResultClickWritable, NullWritable, Text> {

        private Text outputValue = new Text();
        private Text outputKey = new Text();
        private double currentPeriodStart, epsilon, minKLD, minUsrs;

        MultipleOutputs<Text, Text> suspuserOutput;
        MultipleOutputs<NullWritable, Text> qcPairsOutput;
        String suspuserlistPath, qcPairsPath;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            currentPeriodStart = conf.getDouble(Config.PIPELINE_CURRENT_PERIOD_START, 1412114400);
            epsilon = conf.getDouble(Config.PIPELINE_EPSILON, 0.001);
            minKLD = conf.getDouble(Config.PIPELINE_KLD, 1);
            minUsrs = conf.getDouble(Config.PIPELINE_MIN_USERS, 100);
            suspuserOutput = new MultipleOutputs(context);
            qcPairsOutput = new MultipleOutputs<NullWritable, Text>(context);
            suspuserlistPath = conf.get(Config.PIPELINE_SUSPUSERLIST_RAW, "suspuserlist_/");
            qcPairsPath = conf.get(Config.PIPELINE_QCPAIRS, "qcpairs/");
        }

        /**
         * The reduce method processes the query data of every query Per every
         * ResultClickWritable it adds the UID, URL, TIME data to the SERP
         * histogram. If the number of current period clicks is greater than 0,
         * the Kl Divergence of the histogram is computed. If the KL Divergence
         * is greater than the minKLD threshold we iterate over the SERP
         * histogram elements. If the element (which represents a SERP URL) has
         * been clicked by more then "minUsers" users and it has received more
         * relative or absolute clicks than in the history period, we emit it as
         * a new QC pair.
         *
         * @param query
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text query, Iterable<ResultClickWritable> values, Context context) throws IOException, InterruptedException {

            Long queryHash = MurmurHash.hash64(query.toString());
            SERPHistogram hist = new SERPHistogram(currentPeriodStart);
            Long uidHash, urlHash;
            Double time;

            for (ResultClickWritable clickData : values) {
                uidHash = clickData.getUidHash();
                time = clickData.getTime();
                urlHash = clickData.getUrlHash();
                hist.addToHist(urlHash, time, uidHash);
            }

            String message;
            if (hist.getCurClicks() > 0) {
                HistElement el;
                double kld = hist.kld(epsilon);
                if (kld > minKLD) {
                    double relChange, absChange;
                    int docClickers;
                    for (Entry<Long, HistElement> entry : hist.entrySet()) {
                        urlHash = entry.getKey();
                        el = entry.getValue();
                        docClickers = el.users.size();
                        relChange = el.relCurClicks - el.relHistClicks;
                        absChange = el.curClicks - el.histClicks;

                        if (docClickers > minUsrs && (relChange > 0 || absChange > 0)) {
                            // conditions were met, we emit a new QC PAIR
                            message = String.format("{\"url\": %d,\"query\": %d,\"kld\": %.3f, \"usrs\": %d, \"relchange\": %.3f,"
                                    + " \"abschange\": %.3f, \"users\": %s, \"curclicks\": %.1f}",
                                    urlHash, queryHash, kld, docClickers, relChange, absChange, Arrays.toString(el.users.toArray()), el.curClicks);
                            outputValue.set(message);
                            qcPairsOutput.write("qcpairs", NullWritable.get(), outputValue, qcPairsPath);
                            // increment the count of all QC GROUPS 
                            context.getCounter(QC_GROUPS.NUM_QCGROUPS).increment(1);
                            // we also want to have users -> qc pairs mapping and so we them for every user who has clicked on the SERP histogram element (URL)
                            for (Long user : el.users) {
                                message = String.format("{\"query\": %d,\"url\": %d}", queryHash, urlHash);
                                outputKey.set(user.toString());
                                outputValue.set(message);
                                suspuserOutput.write("suspusers", outputKey, outputValue, suspuserlistPath);
                                context.getCounter(QC_GROUPS.NUM_NON_UNIQUE_GROUP_USERS).increment(1);
                            }
                        }
                    }

                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            suspuserOutput.close();
            qcPairsOutput.close();//To change body of generated methods, choose Tools | Templates.
        }

    }

}
