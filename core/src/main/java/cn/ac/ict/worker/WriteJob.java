package cn.ac.ict.worker;

import cn.ac.ict.MS;
import cn.ac.ict.generator.Generator;
import cn.ac.ict.utils.SimpleGenerator;
import cn.ac.ict.worker.throughput.ThroughputStrategy;

/**
 * Created by jiecxy on 2017/3/27.
 */
public class WriteJob {

    public int runTime;
    public String streamName;
    public int messageSize;
    public boolean isSync;
    public ThroughputStrategy strategy;
    public Generator generator;

    public WriteJob(int runTime, String streamName, int messageSize, boolean isSync, ThroughputStrategy strategy) {
        this.runTime = runTime;
        this.streamName = streamName;
        this.messageSize = messageSize;
        this.isSync = isSync;
        this.strategy = strategy;


        if (messageSize != -1) {
            generator = new SimpleGenerator(messageSize);
        }
    }
}
