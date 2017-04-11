package cn.ac.ict.msbench.worker.job;

import java.io.Serializable;

/**
 * Created by jiecxy on 2017/3/27.
 */
public class ReadJob extends Job implements Serializable {

    public int from;

    public ReadJob(String system, String host, int runTime, String streamName, int from, long delayStartSec) {
        super(system, host, runTime, streamName, delayStartSec);
        this.from = from;
        isWriter = false;
    }

    public ReadJob(String system, String host, int runTime, int statInterval, String streamName, int from, long delayStartSec) {
        super(system, host, runTime, streamName, delayStartSec);
        super.statIntervalInSec = statInterval;
        this.from = from;
        isWriter = false;
    }

    @Override
    public String toString() {
        return "Job{" +
                "system='" + system + '\'' +
                ", host='" + host + '\'' +
                ", runTimeInSec=" + runTimeInSec +
                ", statIntervalInSec=" + statIntervalInSec +
                ", streamName='" + streamName + '\'' +
                ", isWriter=" + isWriter +
                ", delayStartSec=" + delayStartSec +
                ", from=" + from +
                '}';
    }
}
