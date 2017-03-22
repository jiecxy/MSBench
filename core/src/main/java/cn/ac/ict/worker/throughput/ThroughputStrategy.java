package cn.ac.ict.worker.throughput;


import java.io.Serializable;

public class ThroughputStrategy {

    /**
     * Arguments required By different write mode
     *   NoLimitThroughput: -tp -1
     *   ConstantThroughput: -tp 1000
     *   GradualChangeThroughput: -tp 1000 -ftp 2000 -ctp 100 -ctps 5
     *   GivenRandomChangeThroughputList: -rtpl 100,200,300,400 -ctps 5
     */


    public TPMODE mode;

    public enum TPMODE implements Serializable {
        NoLimit, Constant, GradualChange, GivenRandomChangeList;
    }

    public ThroughputStrategy(TPMODE mode) {
        this.mode = mode;
    }
}
