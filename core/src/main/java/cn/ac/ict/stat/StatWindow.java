package cn.ac.ict.stat;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


public class StatWindow implements Serializable {


    public long time;  // ms
    public double rate; // msg/s
    public long records;  // number of msg
    public double tps; // mb/s
    public double avgLatency;
    public double maxLatency;


    public StatWindow(long time, double rate, long records, double tps, double avgLatency, double maxLatency) {
        this.time = time;
        this.rate = rate;
        this.records = records;
        this.tps = tps;
        this.avgLatency = avgLatency;
        this.maxLatency = maxLatency;
    }

    public String printHead() {
        return "Reporting Window:\n" +
                String.format("%-22s  %-12s  %-12s  %-12s  %-14s  %-14s", "Time", "Rate(msg/s)", "Records", "Tps(MB/s)", "AvgLatency(ms)", "MaxLatency(ms)") + "\n";
    }

    public String getTime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss,S");
        return df.format(new Date(time));
    }

    @Override
    public String toString() {
        return String.format("%-22s  %-12f  %-12d  %-12f  %-14f  %-14f", getTime(), rate, records, tps, avgLatency, maxLatency);
    }
}
