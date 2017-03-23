package cn.ac.ict.worker.throughput;


public class NoLimitThroughput extends ThroughputStrategy {

    public NoLimitThroughput() {
        super(TPMODE.NoLimit);
    }

    @Override
    public String toString() {
        return super.toString() + "; ";
    }
}
