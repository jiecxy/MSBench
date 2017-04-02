package cn.ac.ict.worker.throughput;


import java.io.Serializable;

public class GivenRandomChangeThroughputList extends ThroughputStrategy implements Serializable {

    public int[] rtpl = null;
    public int ctps = -1;

    public GivenRandomChangeThroughputList(int[] rtpl, int ctps) {
        super(TPMODE.GivenRandomChangeList);
        this.rtpl = rtpl;
        this.ctps = ctps;
    }

    @Override
    public String toString() {
        String list = "[";
        for (int t : rtpl) {
            list += " " + t;
        }
        list += " ]";
        return super.toString() + ": "
                + "  rtpl=" + list
                + "  ctps=" + ctps;
    }
}