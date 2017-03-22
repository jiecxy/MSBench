package cn.ac.ict.worker.throughput;


public class GivenRandomChangeThroughputList extends ThroughputStrategy {

    public int[] rtpl = null;
    public int ctps = -1;

    public GivenRandomChangeThroughputList(int[] rtpl, int ctps) {
        super(TPMODE.GivenRandomChangeList);
        this.rtpl = rtpl;
        this.ctps = ctps;
    }
}