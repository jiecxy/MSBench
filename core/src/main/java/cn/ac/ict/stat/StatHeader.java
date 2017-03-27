package cn.ac.ict.stat;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


public class StatHeader implements Serializable {
    public String system;
    public String streamName;
    public int messageSize;
    public int runTime;
    public String startTime;
    public int reportingInterval;
    public String hostAndPort;
    public String writeMode;
    public int rate;
    public int initialRate;
    public int targetRate;
    public int changeRateperInterval;
    public int changeInterval;
    public String readFrom;

    public StatHeader() {
        system="None";
        streamName="None";
        messageSize=0;
        runTime=0;
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss,S");//设置日期格式
        startTime=df.format(new Date());// new Date()为获取当前系统时间
        reportingInterval=0;
        hostAndPort="None";
        writeMode="None";
        rate=0;
        initialRate=0;
        targetRate=0;
        changeInterval=0;
        readFrom="None";
    }

    public StatHeader(String system, String streamName, int messageSize, int runTime, int reportingInterval, String hostAndPort, String writeMode, int rate, int initialRate, int targetRate, int changeRateperInterval, int changeInterval, String readFrom) {
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss,S");//设置日期格式
        startTime=df.format(new Date());// new Date()为获取当前系统时间
        this.system = system;
        this.streamName = streamName;
        this.messageSize = messageSize;
        this.runTime = runTime;
        this.reportingInterval = reportingInterval;
        this.hostAndPort = hostAndPort;
        this.writeMode = writeMode;
        this.rate = rate;
        this.initialRate = initialRate;
        this.targetRate = targetRate;
        this.changeRateperInterval = changeRateperInterval;
        this.changeInterval = changeInterval;
        this.readFrom = readFrom;
    }

    @Override
    public String toString() {
        return "StatHeader{" +
                "system='" + system + '\'' +
                ", streamName='" + streamName + '\'' +
                ", messageSize=" + messageSize +
                ", runTime=" + runTime +
                ", startTime='" + startTime + '\'' +
                ", reportingInterval=" + reportingInterval +
                ", hostAndPort='" + hostAndPort + '\'' +
                ", writeMode='" + writeMode + '\'' +
                ", rate=" + rate +
                ", initialRate=" + initialRate +
                ", targetRate=" + targetRate +
                ", changeRateperInterval=" + changeRateperInterval +
                ", changeInterval=" + changeInterval +
                ", readFrom='" + readFrom + '\'' +
                '}';
    }
}
