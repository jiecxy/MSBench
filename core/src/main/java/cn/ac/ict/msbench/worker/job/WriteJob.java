package cn.ac.ict.msbench.worker.job;

import cn.ac.ict.msbench.generator.Generator;
import cn.ac.ict.msbench.utils.SimpleGenerator;
import cn.ac.ict.msbench.worker.throughput.ThroughputStrategy;

import java.io.Serializable;

/**
 * Created by jiecxy on 2017/3/27.
 */
public class WriteJob extends Job implements Serializable {

    public int messageSize;
    public boolean isSync;
    public ThroughputStrategy strategy;
    transient public Generator generator;

    public WriteJob(String system, String host, int runTime, String streamName, int messageSize, boolean isSync, ThroughputStrategy strategy, long delayStartSec) {
        super(system, host, runTime, streamName, delayStartSec);
        this.messageSize = messageSize;
        this.isSync = isSync;
        this.strategy = strategy;
        isWriter = true;
        if (messageSize != -1) {
            generator = new SimpleGenerator(messageSize);
        }
    }

    public WriteJob(String system, String host, int runTime, int statInterval, String streamName, int messageSize, boolean isSync, ThroughputStrategy strategy, long delayStartSec) {
        super(system, host, runTime, streamName, delayStartSec);
        super.statIntervalInSec = statInterval;
        this.messageSize = messageSize;
        this.isSync = isSync;
        this.strategy = strategy;
        isWriter = true;
        if (messageSize != -1) {
            generator = new SimpleGenerator(messageSize);
        }
    }

    @Override
    public String toString() {
        return "Job{" +
                "system='" + system + '\'' +
                ", host='" + host + '\'' +
                ", runTimeInSec=" + runTimeInSec +
                ", statIntervalInSec=" + statIntervalInSec +
                ", streamName='" + streamName + '\'' +
                ", delayStartSec=" + delayStartSec  +
                ", isWriter=" + isWriter +
                ", messageSize=" + messageSize +
                ", isSync=" + isSync +
                ", strategy=" + strategy +
                '}';
    }
}
