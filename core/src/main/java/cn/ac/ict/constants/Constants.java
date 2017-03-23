package cn.ac.ict.constants;

/**
 * Created by jiecxy on 2017/3/14.
 */
public class Constants {

    public static final String CONFIG_PRE = "-";

    // For global variables
    public static final String SYSTEM = "sys";
    public static final String SYSTEM_DOC = "Indicate the message system class which extends the class MESSAGE_SIZE.";
    public static final String SYSTEM_DESCRPTION = "This tool is used to verify the message system performance." + "\n"
            + "Start Master: " + "\n"
            + "\t" + "-tr 1000 -M 1.1.1.1:9999 -P master -w 1 -r 1 -sn 1 -name topic" + "\n"
            + "Start Reader: " + "\n"
            + "\t" + "-tr 1000 -M 1.1.1.1:9999 -P reader -W 2.2.2.2 -sys com.apache.kafka.KafkaClient -cf ./kafka.config -sname topic0 -from 0" + "\n"
            + "Start Writer:" + " there are four rate mode for writer. Argument requirements:" + "\n"
            + "\t" + "NoLimitThroughput: -tr 1000 -M 1.1.1.1:9999 -P writer -W 2.2.2.2 -sys com.apache.kafka.KafkaClient -cf ./kafka.config -sname topic0 -ms 10  -tp -1" + "\n"
            + "\t" + "ConstantThroughput: -tr 1000 -M 1.1.1.1:9999 -P writer -W 2.2.2.2 -sys com.apache.kafka.KafkaClient -cf ./kafka.config -sname topic0 -ms 10 -tp 1000" + "\n"
            + "\t" + "GradualChangeThroughput: -tr 1000 -M 1.1.1.1:9999 -P writer -W 2.2.2.2 -sys com.apache.kafka.KafkaClient -cf ./kafka.config -sname topic0 -ms 10 -tp 1000 -ftp 2000 -ctp 100 -ctps 5" + "\n"
            + "\t" + "GivenRandomChangeThroughputList: -tr 1000 -M 1.1.1.1:9999 -P writer -W 2.2.2.2 -sys com.apache.kafka.KafkaClient -cf ./kafka.config -sname topic0 -ms 10 -rtpl 100,200,300,400 -ctps 5";
    public static final String RUN_TIME = "tr";
    public static final String RUN_TIME_DOC = "Test Time (seconds).";
    //public static final String HOSTS = "hosts";
    //public static final String HOSTS_DOC = "The hosts used to be clients. Writer or Reader threads will be assigned to these hosts by round robin.";
    public static final String CONFIG_FILE = "cf";
    public static final String CONFIG_FILE_DOC = "The config file for the specific message system client.";
    public static final String MASTER_ADDRESS = "M";
    public static final String MASTER_ADDRESS_DOC = "Indicate master's ip and port.";

    public static final String MASTER = "master";
    public static final String WRITER = "writer";
    public static final String READER = "reader";
    public static final String PROCESS = "P";
    public static final String PROCESS_DOC = MASTER + ", " + WRITER + ", or " + READER + ".";

    // For Stream
    public static final String STREAM_NUM = "sn";
    public static final String STREAM_NUM_DOC = "The number of streams.";
    public static final String STREAM_NAME = "sname";
    public static final String STREAM_NAME_DOC = "The stream name.";
    public static final String STREAM_NAME_PREFIX = "name";
    public static final String STREAM_NAME_PREFIX_DOC = "The prefix name of stream.";

    // For master process
    //public static final String WRITER_LIST = "W";
    //public static final String WRITER_LIST_DOC = "Indicate the writers and its hosts.";
    //public static final String READER_LIST = "R";
    //public static final String READER_LIST_DOC = "Indicate the readers and its hosts.";


    // For worker process
    public static final String WORKER_ADDRESS = "W";
    public static final String WORKER_ADDRESS_DOC = "The ip of worker.";
    //    For Writer
    public static final String WRITER_NUM = "w";
    public static final String WRITER_NUM_DOC = "The number of writer.";
    public static final String SYNC = "sync";
    public static final String SYNC_DOC = "The write mode: Sync. Default mode is Async.";
    public static final String MESSAGE_SIZE = "ms";
    public static final String MESSAGE_SIZE_DOC = "Message Size (Byte).";
    //        For Writer Throughput
    /**
     * Arguments required By different write mode
     *   NoLimitThroughput: -tp -1
     *   ConstantThroughput: -tp 1000
     *   GradualChangeThroughput: -tp 1000 -ftp 2000 -ctp 100 -ctps 5
     *   GivenRandomChangeThroughputList: -rtpl 100,200,300,400 -ctps 5
     */
    public static final String THROUGHPUT = "tp";
    public static final String THROUGHPUT_DOC = "The Initial throughput. -1: No limit throughput. For the throughput strategy: NoLimitThroughput / ConstantThroughput(only this property is set) / GradualChangeThroughput";
    public static final String FINAL_THROUGHPUT = "ftp";
    public static final String FINAL_THROUGHPUT_DOC = "The final throughput (messages/second). For the throughput strategy: GradualChangeThroughput";
    public static final String CHANGE_THROUGHPUT = "ctp";
    public static final String CHANGE_THROUGHPUT_DOC = "The change throughput every interval (messages/second). For the throughput strategy: GradualChangeThroughput";
    public static final String CHANGE_THROUGHPUT_SECONDS = "ctps";
    public static final String CHANGE_THROUGHPUT_SECONDS_DOC = "The change interval (second). For the throughput strategy: GivenRandomChangeThroughputList / GradualChangeThroughput";
    public static final String RANDOM_THROUGHPUT_LIST = "rtpl";
    public static final String RANDOM_THROUGHPUT_LIST_DOC = "The random throughput list (messages/second). For the throughput strategy: GivenRandomChangeThroughputList.";

    //    For Reader
    public static final String READER_NUM = "r";
    public static final String READER_NUM_DOC = "The number of reader.";
    public static final String READ_FROM = "from";
    public static final String READ_FROM_DOC = "The read strategies of reader. n: from offset n; 0: from oldest; -1: from latest; -2: each has different offset.";
}
