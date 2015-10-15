package com.ccc.SBotMiner.Pipeline;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Tom
 *
 *
 *
 */
public class UserDataGrouper extends Configured implements Tool {
/**
 * 
 * @param args
 * @throws Exception 
 */
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new UserDataGrouper(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = new Job(conf, "DataGrouper");

        job.setJarByClass(UserDataGrouper.class);
        job.setMapperClass(UserDataGrouper.DataMapper.class);
        job.setReducerClass(UserDataGrouper.DataReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileSystem hdfs = FileSystem.get(conf);

        String suspuserListPath = conf.get(Config.PIPELINE_SUSPUSERLIST, "suspuserlist/");
        String suspuserqueryPath = conf.get(Config.PIPELINE_USERQUERY_DATA, "user_queries/");
        String mainPath = conf.get(Config.PIPELINE_MAINPATH, "sbotminer/");
        
        String suspuserDataPath = conf.get(Config.PIPELINE_SUSPUSER_DATA);
        

        FileInputFormat.addInputPath(job, new Path(mainPath+suspuserqueryPath));
        FileInputFormat.addInputPath(job, new Path(mainPath+suspuserListPath));
        FileOutputFormat.setOutputPath(job, new Path(mainPath+suspuserDataPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static class DataMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

            context.write(key, value);
        }
    }

    private static class DataReducer extends Reducer<Text, Text, NullWritable, Text> {

        private Text outputValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> vals = new ArrayList<String>();
            for (Text t : values) {
                vals.add(t.toString());
            }
            if (vals.size() == 2) {

                StringBuilder sb = new StringBuilder("{");
                sb.append("\"uid\":").append(key.toString()).append(",");
                String data = vals.get(0);
                sb.append(data.substring(1, data.length() - 1)).append(",");
                data = vals.get(1);
                sb.append(data.substring(1));
                outputValue.set(sb.toString());

                context.write(NullWritable.get(), outputValue);
            }

        }

    }
}
