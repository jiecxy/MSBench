package cn.ac.ict.stat;

import java.io.Serializable;


public class StatWindow implements Serializable {
    public String time;
    public int rate;
    public int records;
    public double tps;
    public double avgLatency;
    public double maxLatency;
}
