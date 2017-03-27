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

    public StatTail() {
    }

    public StatTail(String finishTime, int messagesSent, int dataSent, double avgTps, double avgLatency, double maxLatency, double percentile50, double percentile95, double percentile99, double percentile999, int messagesReseived, int dataReseived) {
        this.finishTime = finishTime;
        this.messagesSent = messagesSent;
        this.dataSent = dataSent;
        this.avgTps = avgTps;
        this.avgLatency = avgLatency;
        this.maxLatency = maxLatency;
        this.percentile50 = percentile50;
        this.percentile95 = percentile95;
        this.percentile99 = percentile99;
        this.percentile999 = percentile999;
        this.messagesReseived = messagesReseived;
        this.dataReseived = dataReseived;
    }
}
