package cn.ac.ict.msbench.stat;

import cn.ac.ict.msbench.worker.throughput.ThroughputStrategy;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;


public class StatHeader implements Serializable {

    public String system;
    public String streamName;
    public int runTime;
    public long startTime;   // milliseconds
    public int reportingInterval; // s
    public String host;
    public long delayStartSec;

    // writer
    public int messageSize;
    public ThroughputStrategy strategy;
    public boolean writeMode;  // sync - true; async - false

    // reader
    public int readFrom;

    public boolean isWriter;

    public StatHeader(String system, String streamName, int runTime, long startTime, int reportingInterval, String host, int messageSize, ThroughputStrategy strategy, Boolean writeMode, long delayStartSec) {
        this.system = system;
        this.streamName = streamName;
        this.runTime = runTime;
        this.startTime = startTime;
        this.reportingInterval = reportingInterval;
        this.host = host;
        this.messageSize = messageSize;
        this.strategy = strategy;
        this.writeMode = writeMode;
        this.delayStartSec = delayStartSec;
        isWriter = true;
    }

    public StatHeader(String system, String streamName, int runTime, long startTime, int reportingInterval, String host, int readFrom, long delayStartSec) {
        this.system = system;
        this.streamName = streamName;
        this.runTime = runTime;
        this.startTime = startTime;
        this.reportingInterval = reportingInterval;
        this.host = host;
        this.readFrom = readFrom;
        this.delayStartSec = delayStartSec;
        isWriter = false;
    }

    public String getReadFrom() {
        if (-1 == readFrom)
            return "Latest";
        else if (0 == readFrom)
            return "Oldest";
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
        String str = "\t" + "System: " + system + "\n" +
                "\t" + "Stream Name: " + streamName + "\n" +
                "\t" + "Run Time: " + runTime + " s" + "\n" +
                "\t\t" + "Start Time: " + getStartTime() + "\n" +
                "\t\t" + "Delay Time: " + delayStartSec + " s" + "\n" +
                "\t" + "Reporting Interval: " + reportingInterval + " s" + "\n" +
                "\t" + "Host: " + host;
        if (isWriter)
            return "\nWriter Stats:" + "\n" +
                    str + "\n" +
                    "\t" + "Message Size: " + messageSize + " Byte" + "\n" +
                    "\t" + "Rate: " + strategy + "\n" +
                    "\t" + "Write Mode: " + getWriteMode();
        else
            return "Reader Stats:" + "\n" +
                    str + "\n" +
                    "\t" + "Read From: " + getReadFrom();
    }

}
