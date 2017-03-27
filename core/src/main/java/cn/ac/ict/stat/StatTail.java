package cn.ac.ict.stat;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StatTail implements Serializable {
    public String finishTime;
    public long messagesSent;
    public long dataSent;
    public double avgTps;
    public double avgLatency;
    public double maxLatency;
    public double percentile50;
    public double percentile95;
    public double percentile99;
    public double percentile999;
    public long messagesReseived;
    public long dataReseived;

    public StatTail() {
    }

    public StatTail(long messagesSent, long dataSent, double avgTps, double avgLatency, double maxLatency,
                    double percentile50, double percentile95, double percentile99, double percentile999,
                    long messagesReseived, long dataReseived) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss,S");//设置日期格式
        finishTime=df.format(new Date());// new Date()为获取当前系统时间
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

    @Override
    public String toString() {
        return "StatTail{" +
                "finishTime='" + finishTime + '\'' +
                ", messagesSent=" + messagesSent +
                ", dataSent=" + dataSent +
                ", avgTps=" + avgTps +
                ", avgLatency=" + avgLatency +
                ", maxLatency=" + maxLatency +
                ", percentile50=" + percentile50 +
                ", percentile95=" + percentile95 +
                ", percentile99=" + percentile99 +
                ", percentile999=" + percentile999 +
                ", messagesReseived=" + messagesReseived +
                ", dataReseived=" + dataReseived +
                '}';
    }
}
