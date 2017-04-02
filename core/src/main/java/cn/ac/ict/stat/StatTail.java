package cn.ac.ict.stat;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StatTail implements Serializable {


    public long finishTime; // milliseconds

    public double avgTps; // MB/s
    public double avgLatency;  // ms
    public double maxLatency;  // ms
    public double percentile50;  // ms
    public double percentile95;  // ms
    public double percentile99;  // ms
    public double percentile999;  // ms

    // writer
    public long messagesSentOrReceived;
    public long dataSentOrReceived;  // MB

    public boolean isWriter;

    public StatTail(long finishTime, double avgTps, double avgLatency, double maxLatency, double percentile50, double percentile95, double percentile99, double percentile999, long messagesSentOrReceived, long dataSentOrReceived, boolean isWriter) {
        this.finishTime = finishTime;
        this.avgTps = avgTps;
        this.avgLatency = avgLatency;
        this.maxLatency = maxLatency;
        this.percentile50 = percentile50;
        this.percentile95 = percentile95;
        this.percentile99 = percentile99;
        this.percentile999 = percentile999;
        this.messagesSentOrReceived = messagesSentOrReceived;
        this.dataSentOrReceived = dataSentOrReceived;
        this.isWriter = isWriter;
    }

    public String getFinishTime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss,S");
        return df.format(new Date(finishTime));
    }

    @Override
    public String toString() {
        String str = "Aggregated Stats: " + "\n" +
                "\t" + "FinishTime: " + getFinishTime() + "\n";
        if (isWriter) {
            str += "\t" + "Messages Sent: " + messagesSentOrReceived + "\n" +
                    "\t" + "Data Sent: " + dataSentOrReceived + " MB" + "\n";
        } else {
            str += "\t" + "Messages Received: " + messagesSentOrReceived + "\n" +
                    "\t" + "Data Received: " + dataSentOrReceived + " MB" + "\n";
        }

        return str + "\t" + "Avg Tps: " + formatFloat(avgTps) + " MB/s" +  "\n" +
                    "\t" + "Latency: " +  "\n" +
                    "\t\t" + "Avg Latency: " + formatFloat(avgLatency) + " ms" + "\n" +
                    "\t\t" + "Max Latency: " + formatFloat(maxLatency) + " ms"  + "\n" +
                    "\t\t" + "50 percentile: " + formatFloat(percentile50) + " ms"  + "\n" +
                    "\t\t" + "95 percentile: " + formatFloat(percentile95) + " ms"  + "\n" +
                    "\t\t" + "99 percentile: " + formatFloat(percentile99) + " ms"  + "\n" +
                    "\t\t" + "99.9 percentile: " + formatFloat(percentile999) + " ms"  + "\n";
    }

    private String formatFloat(double d) {
        return String.format("%.3f", d);
    }
}
