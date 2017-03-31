package cn.ac.ict.msbench.worker.job;

import java.io.Serializable;

/**
 * Created by jiecxy on 2017/3/27.
 */
public class ReadJob extends Job implements Serializable {

    public int from;

    public ReadJob(String system, String host, int runTime, String streamName, int from) {
        super(system, host, runTime, streamName);
        this.from = from;
        isWriter = false;
    }

    public ReadJob(String system, String host, int runTime, int statInterval, String streamName, int from) {
        super(system, host, runTime, streamName);
        super.statInterval = statInterval;
        this.from = from;
        isWriter = false;
    }

    @Override
    public String toString() {
        return "Job{" +
                "system='" + system + '\'' +
                ", host='" + host + '\'' +
                ", runTime=" + runTime +
                ", statInterval=" + statInterval +
                ", streamName='" + streamName + '\'' +
                ", isWriter=" + isWriter +
                ", from=" + from +
                '}';
    }
}
