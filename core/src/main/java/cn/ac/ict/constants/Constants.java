package cn.ac.ict.constants;

/**
 * Created by jiecxy on 2017/3/14.
 */
public class Constants {

    public static final String CONFIG_PRE = "-";

    public static final String SN = "sn";
    public static final String SN_DOC = "The number of streams.";

    public static final String NAME = "name";
    public static final String NAME_DOC = "The prefix name of stream.";

    // For Writer
    public static final String W = "w";
    public static final String W_DOC = "The number of writer.";
    public static final String SYNC = "";
    public static final String SYNC_DOC = "The write mode: 0: Async; 1 Sync";

    // For Reader
    public static final String R = "r";
    public static final String R_DOC = "The number of reader.";
    public static final String FROM = "from";
    public static final String FROM_DOC = "The read strategies of reader. n: from offset n; 0: from oldest; -1: from latest; -2: each has different offset.";


    public static final String MS = "ms";
    public static final String MS_DOC = "Message Size (Byte).";

    public static final String TR = "tr";
    public static final String TR_DOC = "Test Time (seconds).";

    public static final String HOSTS = "hosts";
    public static final String HOSTS_DOC = "The hosts used to be clients. Writer or Reader threads will be assigned to these hosts by round robin.";

    // For Throughput
    public static final String TP = "tp";
    public static final String TP_DOC = "The Initial throughput. -1: No limit throughput. For the throughput strategy: NoLimitThroughput / GradualChangeThroughput";
    public static final String FTP = "ftp";
    public static final String FTP_DOC = "The final throughput (messages/second). For the throughput strategy: GradualChangeThroughput";
    public static final String CTP = "ctp";
    public static final String CTP_DOC = "The change throughput every interval (messages/second). For the throughput strategy: GradualChangeThroughput";
    public static final String CTPS = "ctps";
    public static final String CTPS_DOC = "The change interval (second). For the throughput strategy: GivenRandomChangeThroughputList / GradualChangeThroughput";
    public static final String RTPL = "rtpl";
    public static final String RTPL_DOC = "The random throughput list (messages/second). For the throughput strategy: GivenRandomChangeThroughputList.";

    public static final String CF = "cf";
    public static final String CF_DOC = "The config file for the specific message system client.";
}
