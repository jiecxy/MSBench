package cn.ac.ict.worker.throughput;


import java.io.Serializable;

public class NoLimitThroughput extends ThroughputStrategy implements Serializable {

    public NoLimitThroughput() {
        super(TPMODE.NoLimit);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
