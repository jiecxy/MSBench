package cn.ac.ict.msbench.worker.job;

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
    public long delayStartSec = 0;

    public boolean isWriter;

    public Job(String system, String host, int runTime, String streamName, long delayStartSec) {
        this.system = system;
        this.host = host;
        this.runTime = runTime;
        this.streamName = streamName;
        this.delayStartSec = delayStartSec;
    }
}
