package cn.ac.ict.worker.throughput;


public class ConstantThroughput extends ThroughputStrategy {

    public int tp = -1;
    public ConstantThroughput(int tp) {
        super(TPMODE.Constant);
        this.tp = tp;
    }

    @Override
    public String toString() {
        return super.toString() + "; "
                + "  tp=" + tp;
    }
}
