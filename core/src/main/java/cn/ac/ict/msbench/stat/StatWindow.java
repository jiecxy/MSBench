package cn.ac.ict.msbench.stat;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


public class StatWindow implements Serializable {

    public boolean isWriter;

    public long version = -1;
    public long time;  // ms
    public double rate; // msg/s
    public long records;  // msgs
    public double tps; // Byte/s
    public double avgLatency; // writer or reader  - ms
    public double maxLatency; // writer or reader - ms
//    public double endToEndAvgLatency; // end to end - ms
//    public double endToEndMaxLatency; // end to end - ms


    public StatWindow(long time, double rate, long records, double tps, double avgLatency, double maxLatency, boolean isWriter) {
        this.time = time;
        this.rate = rate;
        this.records = records;
        this.tps = tps;
        this.avgLatency = avgLatency;
        this.maxLatency = maxLatency;
        this.isWriter = isWriter;
    }

    public static String printReadHead() {
        String header = "Reporting Window:\n";
        return header + String.format("%-5s  %-24s  %-12s  %-12s  %-12s  %-24s  %-24s",
                "Seq", "Time", "Rate(msg/s)", "Records", "Tps(MB/s)", "EndToEndAvgLatency(ms)", "EndToEndMaxLatency(ms)") + "\n";
    }

    public static String printWriteHead() {
        String header = "Reporting Window:\n";
        return header + String.format("%-5s  %-24s  %-12s  %-12s  %-12s  %-14s  %-14s",
                "Seq", "Time", "Rate(msg/s)", "Records", "Tps(MB/s)", "AvgLatency(ms)", "MaxLatency(ms)") + "\n";
    }

    public String getTime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss,S");
        return df.format(new Date(time));
    }

    @Override
    public String toString() {
        if (isWriter) {
            return String.format("%-5s  %-24s  %-12.6f  %-12d  %-12.6f  %-24.6f  %-24.6f", " " + version, getTime(), rate, records, tps, avgLatency, maxLatency);
        } else {
            return String.format("%-5s  %-24s  %-12.6f  %-12d  %-12.6f  %-14.6f  %-14.6f", " " + version, getTime(), rate, records, tps, avgLatency, maxLatency);
        }
    }
}
