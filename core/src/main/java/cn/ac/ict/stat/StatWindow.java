package cn.ac.ict.stat;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


public class StatWindow implements Serializable {
    public String time;
    public int rate;
    public int records;
    public double tps;
    public double avgLatency;
    public double maxLatency;
    public StatWindow()
    {
    }
    public StatWindow(int numMsg,int numSize,int statInterval)
    {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        time=df.format(new Date());// new Date()为获取当前系统时间
        rate=numSize;
        records=numMsg;
        tps=records/statInterval;
        avgLatency=0 ;
    }
    public StatWindow(String time, int rate, int records, double tps, double avgLatency, double maxLatency) {
        this.time = time;
        this.rate = rate;
        this.records = records;
        this.tps = tps;
        this.avgLatency = avgLatency;
        this.maxLatency = maxLatency;
    }
}
