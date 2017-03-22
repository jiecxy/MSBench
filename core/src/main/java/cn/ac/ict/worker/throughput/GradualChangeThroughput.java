package cn.ac.ict.worker.throughput;

public class GradualChangeThroughput extends ThroughputStrategy {

    public int tp = -1;
    public int ftp = -1;
    public int ctp = -1;
    public int ctps = -1;

    public GradualChangeThroughput(int tp, int ftp, int ctp, int ctps) {
        super(TPMODE.GradualChange);
        this.tp = tp;
        this.ftp = ftp;
        this.ctp = ctp;
        this.ctps = ctps;
    }
}
