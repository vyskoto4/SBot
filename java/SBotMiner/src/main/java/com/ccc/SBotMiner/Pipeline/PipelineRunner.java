package com.ccc.SBotMiner.Pipeline;

import com.ccc.SBotMiner.Evaluation.UserStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PipelineRunner extends Configured implements Tool {

    /**
     *
     *
     * The purpose of {@link PipelineRunner} is to run the whole pipeline at
     * once.
     *
     * The pipeline can be launched with the following commnad: "hadoop jar
     * SBotMiner-1.0.jar com.ccc.SBotMiner.Pipeline.PipelineRunner -conf
     * conf.xml"
     *
     *
     * The following tasks are being executed: First the {@link QCPairExtractor}
     * takes the input history and current data, then generates the list of
     * suspicious QCGroups and also (more importantly) the mapping user=>qc_Pair
     * and thus the list of suspicious users. Secondly {@link UserQCGrouper}
     * takes the list of suspicious users and groups the users qcPairs and thus
     * creating a list of user->qc_pairs. Thirdly {@link UserQueryList} loads
     * the list of suspicious users, and by parsing the whole current data
     * extracts all their submitted queries. Its output is the mapping
     * suspicious_user->submitted_queries. Then {@link UserDataGrouper} takes
     * the outputs from 2nd and 3rd job and creates the mapping
     * suspicious_user->[qc_pairs, queries_submitted] Lastly the
     * {@link MatrixBuilder} processes the mappings from 4 and builds the QC
     * group click matrices, computes their focusness
     */
    public static void main(String[] args) throws Exception {

        ToolRunner.run(new PipelineRunner(), args);

    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = this.getConf();

        Path outputPath = new Path(conf.get("pipeline.output.mainpath", "sbotminer/"));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputPath)) {
            Path backupPath = new Path("backup_" + conf.get(Config.PIPELINE_MAINPATH, "sbotminer/"));
            hdfs.rename(outputPath, backupPath);
        }
        hdfs.mkdirs(new Path(conf.get(Config.PIPELINE_MAINPATH, "sbotminer/")));
        int res = 0;

        System.out.println("Running qc pair extractor");
        res = ToolRunner.run(conf, new QCPairExtractor(), args);

        System.out.println("Running user qc pair grouper");
        res = ToolRunner.run(conf, new UserQCGrouper(), args);

        System.out.println("Running user query extraction job");
        res = ToolRunner.run(conf, new UserQueryList(), args);

        System.out.println("Running user data grouper");
        res = ToolRunner.run(conf, new UserDataGrouper(), args);

        System.out.println("Running the qc group click matrix builder");
        res = ToolRunner.run(conf, new MatrixBuilder(), args);

        System.out.println("Running the user statistics extractor job");
        res = ToolRunner.run(conf, new UserStats(), args);

        return res;
    }
}
