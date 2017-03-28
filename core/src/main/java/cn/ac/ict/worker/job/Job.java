package cn.ac.ict.worker.job;

import java.io.Serializable;

/**
 * Created by jiecxy on 2017/3/28.
 */
public class Job implements Serializable {

    public String system;
    public String host;
    public int runTime;
    public int statInterval;
    public String streamName;

    public boolean isWriter;

    public Job(String system, String host, int runTime, String streamName) {
        this.system = system;
        this.host = host;
        this.runTime = runTime;
        this.streamName = streamName;
    }
}
