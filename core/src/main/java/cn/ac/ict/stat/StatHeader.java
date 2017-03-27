package cn.ac.ict.stat;

import java.io.Serializable;


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
    }

    public StatHeader(String system, String streamName, int messageSize, int runTime, String startTime, int reportingInterval, String hostAndPort, String writeMode, int rate, int initialRate, int targetRate, int changeRateperInterval, int changeInterval, String readFrom) {
        this.system = system;
        this.streamName = streamName;
        this.messageSize = messageSize;
        this.runTime = runTime;
        this.startTime = startTime;
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
}
