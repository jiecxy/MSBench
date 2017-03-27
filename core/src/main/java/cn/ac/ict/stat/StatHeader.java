package cn.ac.ict.stat;

import cn.ac.ict.worker.throughput.ThroughputStrategy;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


public class StatHeader implements Serializable {

    public String system;
    public String streamName;
    public int runTime;
    public long startTime;   // milliseconds
    public int reportingInterval;
    public String host;

    // writer
    public int messageSize;
    public ThroughputStrategy strategy;
    public boolean writeMode;  // sync - true; async - false

    // reader
    public int readFrom;

    public boolean isWriter;

    public StatHeader(String system, String streamName, int runTime, long startTime, int reportingInterval, String host, int messageSize, ThroughputStrategy strategy, Boolean writeMode) {
        this.system = system;
        this.streamName = streamName;
        this.runTime = runTime;
        this.startTime = startTime;
        this.reportingInterval = reportingInterval;
        this.host = host;
        this.messageSize = messageSize;
        this.strategy = strategy;
        this.writeMode = writeMode;
        isWriter = true;
    }

    public StatHeader(String system, String streamName, int runTime, long startTime, int reportingInterval, String host, int readFrom) {
        this.system = system;
        this.streamName = streamName;
        this.runTime = runTime;
        this.startTime = startTime;
        this.reportingInterval = reportingInterval;
        this.host = host;
        this.readFrom = readFrom;
        isWriter = false;
    }

    public String getReadFrom() {
        if (-1 == readFrom)
            return "lateset";
        else if (0 == readFrom)
            return "oldest";
        else
            return "UNKNOWN";
    }

    public String getStartTime() {
        SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss,S");
        return df.format(new Date(startTime));
    }

    public String getWriteMode() {
        if (writeMode)
            return "Sync";
        else
            return "Async";
    }

    @Override
    public String toString() {
        String str = "\t" + "System: '" + system + "\n" +
                "\t" + "Stream Name: " + streamName + "\n" +
                "\t" + "Run Time: " + runTime + "s" + "\n" +
                "\t\t" + "Start Time: " + getStartTime() + "\n" +
                "\t" + "Reporting Interval: " + reportingInterval + "s" + "\n" +
                "\t" + "Host: " + host;
        if (isWriter)
            return "Writer Stats:" + "\n" +
                    str + "\n" +
                    "\t" + ",Message Size: " + messageSize + "\n" +
                    "\t" + "Rate: " + strategy + "\n" +
                    "\t" + "Write Mode: " + getWriteMode() + "\n";
        else
            return "Reader Stats:" +
                    str + "\n" +
                    "\t" + ", readFrom: " + getReadFrom();
    }

}
