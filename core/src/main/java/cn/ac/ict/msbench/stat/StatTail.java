package cn.ac.ict.msbench.stat;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StatTail implements Serializable {

    public boolean isEndToEnd;
    public long finishTime; // milliseconds

    public double avgTps; // MB/s
    public double avgLatency;  // ms
    public double maxLatency;  // ms
    public double percentile50;  // ms
    public double percentile95;  // ms
    public double percentile99;  // ms
    public double percentile999;  // ms

    public long messagesSentOrReceived;
    public double dataSentOrReceived;  // MB

    // end to end latency
    public double endToEndAvgLatency; // end to end
    public double endToEndMaxLatency; // end to end
    public double endToEndPercentile50;  // end to end
    public double endToEndPercentile95;  // end to end
    public double endToEndPercentile99;  // end to end
    public double endToEndPercentile999;  // end to end

    public boolean isWriter;

    public StatTail(long finishTime, double avgTps, double avgLatency, double maxLatency, double percentile50, double percentile95, double percentile99, double percentile999, long messagesSentOrReceived, double dataSentOrReceived, boolean isWriter) {
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
        isEndToEnd = false;
    }

    public StatTail(long finishTime, double avgTps, double avgLatency, double maxLatency, double percentile50, double percentile95, double percentile99, double percentile999, long messagesSentOrReceived, double dataSentOrReceived,
                    double endToEndAvgLatency, double endToEndMaxLatency, double endToEndPercentile50, double endToEndPercentile95, double endToEndPercentile99, double endToEndPercentile999, boolean isWriter) {
        this(finishTime, avgTps, avgLatency, maxLatency, percentile50, percentile95, percentile99, percentile999, messagesSentOrReceived, dataSentOrReceived, isWriter);
        this.endToEndAvgLatency = endToEndAvgLatency;
        this.endToEndMaxLatency = endToEndMaxLatency;
        this.endToEndPercentile50 = endToEndPercentile50;
        this.endToEndPercentile95 = endToEndPercentile95;
        this.endToEndPercentile99 = endToEndPercentile99;
        this.endToEndPercentile999 = endToEndPercentile999;
        isEndToEnd = true;
    }

    public String getFinishTime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss,S");
        return df.format(new Date(finishTime));
    }

    @Override
    public String toString() {
        String str = "\nAggregated Stats: " + "\n" +
                "\t" + "FinishTime: " + getFinishTime() + "\n";
        if (isWriter) {
            str += "\t" + "Messages Sent: " + messagesSentOrReceived + "\n" +
                    "\t" + "Data Sent: " + formatFloat(dataSentOrReceived) + " MB" + "\n";
        } else {
            str += "\t" + "Messages Received: " + messagesSentOrReceived + "\n" +
                    "\t" + "Data Received: " + formatFloat(dataSentOrReceived) + " MB" + "\n";
        }

        str += "\t" + "Avg Tps: " + formatFloat(avgTps) + " MB/s" +  "\n" +
                    "\t" + "Latency: " +  "\n" +
                    "\t\t" + "Avg Latency: " + formatFloat(avgLatency) + " ms" + "\n" +
                    "\t\t" + "Max Latency: " + formatFloat(maxLatency) + " ms"  + "\n" +
                    "\t\t" + "50 percentile: " + formatFloat(percentile50) + " ms"  + "\n" +
                    "\t\t" + "95 percentile: " + formatFloat(percentile95) + " ms"  + "\n" +
                    "\t\t" + "99 percentile: " + formatFloat(percentile99) + " ms"  + "\n" +
                    "\t\t" + "99.9 percentile: " + formatFloat(percentile999) + " ms";
        if (isEndToEnd) {
            return str + "\t" + "End To End Latency: " +  "\n" +
                    "\t\t" + "Avg Latency: " + formatFloat(endToEndAvgLatency) + " ms" + "\n" +
                    "\t\t" + "Max Latency: " + formatFloat(endToEndMaxLatency) + " ms"  + "\n" +
                    "\t\t" + "50 percentile: " + formatFloat(endToEndPercentile50) + " ms"  + "\n" +
                    "\t\t" + "95 percentile: " + formatFloat(endToEndPercentile95) + " ms"  + "\n" +
                    "\t\t" + "99 percentile: " + formatFloat(endToEndPercentile99) + " ms"  + "\n" +
                    "\t\t" + "99.9 percentile: " + formatFloat(endToEndPercentile999) + " ms";
        } else {
            return str;
        }
    }

    private String formatFloat(double d) {
        return String.format("%.3f", d);
    }
}
