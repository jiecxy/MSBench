package cn.ac.ict.worker;

import cn.ac.ict.MS;
import cn.ac.ict.generator.Generator;
import cn.ac.ict.utils.SimpleGenerator;
import cn.ac.ict.worker.throughput.ThroughputStrategy;

/**
 * Created by jiecxy on 2017/3/27.
 */
public class WriteJob {

    public String system;
    public String host;
    public int runTime;
    public int statInterval;
    public String streamName;
    public int messageSize;
    public boolean isSync;
    public ThroughputStrategy strategy;
    public Generator generator;

    public WriteJob(String system, String host, int runTime, int statInterval, String streamName, int messageSize, boolean isSync, ThroughputStrategy strategy) {
        this.system = system;
        this.host = host;
        this.runTime = runTime;
        this.statInterval = statInterval;
        this.streamName = streamName;
        this.messageSize = messageSize;
        this.isSync = isSync;
        this.strategy = strategy;

        if (messageSize != -1) {
            generator = new SimpleGenerator(messageSize);
        }
    }

    public WriteJob(String system, String host, int runTime, int statInterval, String streamName, int messageSize, boolean isSync, ThroughputStrategy strategy, Generator generator) {
        this.system = system;
        this.host = host;
        this.runTime = runTime;
        this.statInterval = statInterval;
        this.streamName = streamName;
        this.messageSize = messageSize;
        this.isSync = isSync;
        this.strategy = strategy;
        this.generator = generator;
    }
}
