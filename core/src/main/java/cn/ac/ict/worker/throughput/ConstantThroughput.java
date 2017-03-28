package cn.ac.ict.worker.throughput;


import java.io.Serializable;

public class ConstantThroughput extends ThroughputStrategy implements Serializable {

    public int tp = -1;
    public ConstantThroughput(int tp) {
        super(TPMODE.Constant);
        this.tp = tp;
    }

    @Override
    public String toString() {
        return super.toString() + ": "
                + "  tp=" + tp;
    }
}
