package com.ccc.SBotMiner.Pipeline;

import com.ccc.utilities.FilesUtils;
import com.ccc.utilities.StringBytesOutputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.ccc.writables.LongPairWritable;
import com.ccc.writables.UserQDataWritable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
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
public class MatrixBuilder extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new MatrixBuilder(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "SBotMatrixBuilder");

        job.setJarByClass(MatrixBuilder.class);
        job.setMapperClass(MatrixBuilder.SBotMapper.class);
        job.setReducerClass(MatrixBuilder.SBotReducer.class);
        job.setPartitionerClass(SBotPartitioner.class);
        job.setGroupingComparatorClass(QueryURLGroupingComparator.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(byte[].class);
        job.setMapOutputKeyClass(LongPairWritable.class);
        job.setMapOutputValueClass(UserQDataWritable.class);
        job.setOutputFormatClass(StringBytesOutputFormat.class);
        LazyOutputFormat.setOutputFormatClass(job, StringBytesOutputFormat.class);
        job.setNumReduceTasks(10);

        Path mainPath = new Path(conf.get(Config.PIPELINE_MAINPATH, "sbotminer/"));
        Path suspuserDataPath = new Path(mainPath, conf.get(Config.PIPELINE_SUSPUSER_DATA, "suspuserdata/"));
        Path tempOutputPath = new Path(mainPath, "matrixBuilderOutput/");

        conf.set("mapreduce.reduce.java.opts", "-Xmx28732M");
        conf.set("mapreduce.reduce.memory.mb", "28732");

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(tempOutputPath)) {
            hdfs.delete(tempOutputPath);
        }

        FileInputFormat.addInputPath(job, suspuserDataPath);
        FileOutputFormat.setOutputPath(job, tempOutputPath);
        int res = job.waitForCompletion(true) ? 0 : 1;
        if (res == 0) {
            FilesUtils.copySubdirectories(tempOutputPath, mainPath, FileSystem.get(conf));
            hdfs.delete(tempOutputPath);
        }
        return res;
    }

    private static class SBotMapper extends Mapper<Text, Text, LongPairWritable, UserQDataWritable> {

        private JSONParser parser = new JSONParser();

        private LongPairWritable outputKey = new LongPairWritable(0, 0);
        private UserQDataWritable outputValue = new UserQDataWritable();

        /*
         * Map reads data containing all queries of a users
         * If the current user belongs to some QCpair group, he has a record in userQCPairs
         * For every QCPair a user belongs to, we emit his query data as <QCPair_hash, user_query_data> pair
         */
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            JSONObject qdata;
            try {
                qdata = (JSONObject) parser.parse(key.toString());
                Long uid = (Long) qdata.get("uid");
                // extract user query data
                JSONArray queryHashes = (JSONArray) qdata.get("queries");
                JSONArray querySubmits = (JSONArray) qdata.get("submits");
                Long[] queries = new Long[queryHashes.size()];
                Integer[] submits = new Integer[queryHashes.size()];
                for (int i = 0; i < queries.length; i++) {
                    queries[i] = (Long) queryHashes.get(i);
                    submits[i] = ((Long) querySubmits.get(i)).intValue();
                }
                outputValue.set(uid, queries, submits);

                JSONArray qcQueries = (JSONArray) qdata.get("target_queries");
                JSONArray qcUrls = (JSONArray) qdata.get("target_urls");
                for (int i = 0; i < qcUrls.size(); i++) {
                    outputKey.setLongs((Long) qcQueries.get(i), (Long) qcUrls.get(i));
                    context.write(outputKey, outputValue);
                }
            } catch (ParseException ex) {
                Logger.getLogger(MatrixBuilder.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

    private static class SBotReducer extends Reducer<LongPairWritable, UserQDataWritable, NullWritable, byte[]> {

        private static final String utf8 = "UTF-8";
        private MultipleOutputs<NullWritable, byte[]> mu;
        private double focusnessThreshold;
        private String focusnessGroupsPath, allGroupsPath;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mu = new MultipleOutputs<NullWritable, byte[]>(context);
            Configuration conf = context.getConfiguration();
            focusnessThreshold = conf.getDouble(Config.PIPELINE_FOCUSNESS, 0.9);
            focusnessGroupsPath = conf.get(Config.PIPELINE_FOCUSNESS_GROUPS, "focusness_groups/");
            allGroupsPath = conf.get(Config.PIPELINE_ALL_GROUPS, "all_groups/");
        }

        /*
         * Receives key-value pairs in format QC_pair_hash, <user_data>
         * where each user_data element contains a user_id and its submitted queries
         * Builds the query*user matrix in sparse format
         */
        @Override
        protected void reduce(LongPairWritable key, Iterable<UserQDataWritable> values, Context context) throws IOException, InterruptedException {

            ArrayList<Integer> cols = new ArrayList<Integer>(); // columns = users
            ArrayList<Integer> data = new ArrayList<Integer>();
            ArrayList<Integer> rows = new ArrayList<Integer>(); // rows = queries
            ArrayList<Long> uids = new ArrayList<Long>();
            QueryIndex queryIndex = new QueryIndex();
            Integer userIndex = 0;
            Long query;
            int queryInd, alltraffic = 0, onlyQtraffic = 0, count = 0;
            Long[] queries;
            Integer[] submits;
            for (UserQDataWritable userQdata : values) {
                queries = userQdata.getQueries();
                submits = userQdata.getClicks();
                for (int i = 0; i < submits.length; i++) {
                    query = queries[i];
                    count = submits[i];
                    alltraffic += count;
                    queryInd = queryIndex.get(query);
                    cols.add(userIndex);
                    rows.add(queryInd);
                    data.add(submits[i]);
                }
                if (queries.length == 1) { // usr searched only for one query - the suspicious one
                    onlyQtraffic += count;
                }
                uids.add(userQdata.getUID());
                userIndex++;
            }
            Double focusness = (double) onlyQtraffic / alltraffic;

            if (focusness > focusnessThreshold) {
                writeObjectToMultipleOutput("{\"query\":", focusnessGroupsPath);
                writeObjectToMultipleOutput(key.getFirst(), focusnessGroupsPath);
                writeObjectToMultipleOutput(",\"url\":", focusnessGroupsPath);
                writeObjectToMultipleOutput(key.getSecond(), focusnessGroupsPath);
                writeObjectToMultipleOutput(",\"foc\":", focusnessGroupsPath);
                writeObjectToMultipleOutput(focusness, focusnessGroupsPath);
                writeObjectToMultipleOutput(",", focusnessGroupsPath);
                writeListToMultipleOutput(uids, "uids", focusnessGroupsPath);
                writeObjectToMultipleOutput(",", focusnessGroupsPath);
                writeListToMultipleOutput(cols, "cols", focusnessGroupsPath);
                writeObjectToMultipleOutput(",", focusnessGroupsPath);
                writeListToMultipleOutput(rows, "rows", focusnessGroupsPath);
                writeObjectToMultipleOutput(",", focusnessGroupsPath);
                writeListToMultipleOutput(data, "data", focusnessGroupsPath);
                writeObjectToMultipleOutput(",", focusnessGroupsPath);
                writeListToMultipleOutput(queryIndex.toList(), "qhashes", focusnessGroupsPath);
                writeObjectToMultipleOutput("}\n", focusnessGroupsPath);
            } else {
                writeObjectToMultipleOutput("{\"query\":", allGroupsPath);
                writeObjectToMultipleOutput(key.getFirst(), allGroupsPath);
                writeObjectToMultipleOutput(",\"url\":", allGroupsPath);
                writeObjectToMultipleOutput(key.getSecond(), allGroupsPath);
                writeObjectToMultipleOutput(",\"foc\":", allGroupsPath);
                writeObjectToMultipleOutput(focusness, allGroupsPath);

                writeObjectToMultipleOutput(",", allGroupsPath);
                writeListToMultipleOutput(uids, "uids", allGroupsPath);
                writeObjectToMultipleOutput(",", allGroupsPath);
                writeListToMultipleOutput(cols, "cols", allGroupsPath);
                writeObjectToMultipleOutput(",", allGroupsPath);
                writeListToMultipleOutput(rows, "rows", allGroupsPath);
                writeObjectToMultipleOutput(",", allGroupsPath);
                writeListToMultipleOutput(data, "data", allGroupsPath);
                writeObjectToMultipleOutput(",", allGroupsPath);
                writeListToMultipleOutput(queryIndex.toList(), "qhashes", allGroupsPath);
                writeObjectToMultipleOutput("}\n", allGroupsPath);
            }
        }

        private void writeObjectToMultipleOutput(Object o, String path) throws IOException, InterruptedException {
            mu.write(NullWritable.get(), o.toString().getBytes(utf8), path);
        }

        private void writeListToMultipleOutput(ArrayList<?> list, String fieldname, String path) throws IOException, InterruptedException {
            fieldname = "\"" + fieldname + "\":";
            mu.write(NullWritable.get(), fieldname.getBytes(utf8), path);

            if (list.isEmpty()) {
                mu.write(NullWritable.get(), "[]".getBytes(utf8), path);
                return;
            }
            mu.write(NullWritable.get(), "[".getBytes(utf8), path);
            mu.write(NullWritable.get(), list.get(0).toString().getBytes(utf8), path);
            
            for (int i = 1; i < list.size(); i++) {
                mu.write(NullWritable.get(), ", ".getBytes(utf8), path);
                mu.write(NullWritable.get(), list.get(i).toString().getBytes(utf8), path);
            }
            mu.write(NullWritable.get(), "]".getBytes(utf8), path);
            list = null;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mu.close(); 
        }
        
    }

    private static class SBotPartitioner extends Partitioner<LongPairWritable, UserQDataWritable> {

        public SBotPartitioner() {
        }

        @Override
        public int getPartition(LongPairWritable key, UserQDataWritable value, int numPartitions) {
            return (int) (Math.abs(key.getFirst()) % numPartitions);
        }
    }

    private static class QueryURLGroupingComparator extends WritableComparator {

        public QueryURLGroupingComparator() {
            super(LongPairWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            LongPairWritable x = (LongPairWritable) a;
            LongPairWritable y = (LongPairWritable) b;

            int res = x.compareTo(y);
            return res;
        }

    }

    private static class QueryIndex {

        private HashMap<Long, Integer> queryIndices;
        private int index;

        public QueryIndex() {
            queryIndices = new HashMap<Long, Integer>();
            index = 0;
        }

        public Integer get(Long query) {
            Integer ret = queryIndices.get(query);
            if (ret == null) {
                queryIndices.put(query, index);
                ret = index;
                index++;
            }
            return ret;

        }

        public ArrayList<Long> toList() {
            Long[] queries = new Long[queryIndices.size()];
            for (Map.Entry<Long, Integer> e : queryIndices.entrySet()) {
                queries[e.getValue()] = e.getKey();
            }
            return new ArrayList<Long>(Arrays.asList(queries));
        }

    }
}
