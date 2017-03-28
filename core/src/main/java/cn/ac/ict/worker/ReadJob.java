package cn.ac.ict.worker;

/**
 * Created by jiecxy on 2017/3/27.
 */
public class ReadJob {

    public String system;
    public String host;
    public int runTime;
    public int statInterval;
    public String streamName;
    public int from;

    public ReadJob(String system, String host, int runTime, int statInterval, String streamName, int from) {
        this.system = system;
        this.host = host;
        this.runTime = runTime;
        this.statInterval = statInterval;
        this.streamName = streamName;
        this.from = from;
    }
}
