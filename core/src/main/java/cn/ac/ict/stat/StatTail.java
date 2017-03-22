package cn.ac.ict.stat;

import java.io.Serializable;

public class StatTail implements Serializable {
    public String finishTime;
    public int messagesSent;
    public int dataSent;
    public double avgTps;
    public double avgLatency;
    public double maxLatency;
    public double percentile50;
    public double percentile95;
    public double percentile99;
    public double percentile999;
    public int messagesReseived;
    public int dataReseived;
}
