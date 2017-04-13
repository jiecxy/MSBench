package cn.ac.ict.msbench.worker.job;

import java.io.Serializable;

/**
 * Created by jiecxy on 2017/3/28.
 */
public class Job implements Serializable {

    public String system;
    public String host;
    public int runTimeInSec;
    public int statIntervalInSec;
    public String streamName;
    public long delayStartSec = 0;

    public boolean isWriter;

    public Job(String system, String host, int runTimeInSec, String streamName, long delayStartSec) {
        this.system = system;
        this.host = host;
        this.runTimeInSec = runTimeInSec;
        this.streamName = streamName;
        this.delayStartSec = delayStartSec;
    }
}
