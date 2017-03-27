package cn.ac.ict.stat;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


public class StatWindow implements Serializable {
    public String time;
    public double rate; //msg/s
    public long records;  //number os msg
    public double tps; // mb/s
    public double avgLatency;
    public double maxLatency;


    public StatWindow()
    {
    }
    public StatWindow(double rate, long records, double tps, double avgLatency, double maxLatency) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss,S");//设置日期格式
        time=df.format(new Date());// new Date()为获取当前系统时间
        this.rate = rate;
        this.records = records;
        this.tps = tps;
        this.avgLatency = avgLatency;
        this.maxLatency = maxLatency;
    }

    @Override
    public String toString() {
        return "StatWindow{" +
                "time='" + time + '\'' +
                ", rate=" + rate +
                ", records=" + records +
                ", tps=" + tps +
                ", avgLatency=" + avgLatency +
                ", maxLatency=" + maxLatency +
                '}';
    }
}
