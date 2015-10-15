package com.ccc.SBotMiner.Pipeline;

/**
 *
 * @author Tom
 */
public class Config {

    public static String PIPELINE_INPUT_HISTORY = "pipeline.input.history";
    public static String PIPELINE_INPUT_CURRENT = "pipeline.input.current";
    public static String PIPELINE_MAINPATH = "pipeline.path.mainpath";
    public static String PIPELINE_EPSILON = "pipeline.const.epsilon";
    public static String PIPELINE_KLD = "pipeline.threshold.kld";
    public static String PIPELINE_MIN_USERS = "pipeline.threshold.minusers";
    public static String PIPELINE_FOCUSNESS = "pipeline.threshold.focusness";
    public static String PIPELINE_CURRENT_PERIOD_START = "pipeline.const.currentperiodstart";

    public static String PIPELINE_SUSPUSERLIST_RAW = "pipeline.path.rawsuspuserlist";
    public static String PIPELINE_SUSPUSERLIST = "pipeline.path.suspuserlist";
    public static String PIPELINE_SUSPUSERLIST_LINES = "pipeline.path.suspuserlist.lines";

    public static String PIPELINE_QCPAIRS = "pipeline.path.qcpairs";
    public static String PIPELINE_USERQUERY_DATA = "pipeline.path.userquery";
    public static String PIPELINE_QUERY_DATA = "pipeline.path.query";

    
    public static String PIPELINE_SUSPUSER_DATA = "pipeline.path.suspuserdata";
    public static String PIPELINE_FOCUSNESS_GROUPS = "pipeline.path.focusnessgroups";
    public static String PIPELINE_ALL_GROUPS = "pipeline.path.allgroups";

    public static Long NO_CLICK = 0L;
    public static String PIPELINE_USER_STATS="pipeline.path.userstats";
}
