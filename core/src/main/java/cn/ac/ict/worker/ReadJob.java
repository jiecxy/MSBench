package cn.ac.ict.worker;

/**
 * Created by jiecxy on 2017/3/27.
 */
public class ReadJob {

    public int runTime;
    public String streamName;
    public int from;

    public ReadJob(int runTime, String streamName, int from) {
        this.runTime = runTime;
        this.streamName = streamName;
        this.from = from;
    }
}
