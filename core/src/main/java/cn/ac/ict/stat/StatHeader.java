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
}
