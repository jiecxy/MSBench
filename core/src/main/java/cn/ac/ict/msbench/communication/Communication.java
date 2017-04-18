package cn.ac.ict.msbench.communication;

import akka.actor.UntypedActor;

public abstract class Communication extends UntypedActor {

    protected final boolean NEED_INITIALIZE_MS = false;
    protected final boolean NEED_FINALIZE_MS = false;

//    protected int CHECK_TIMEOUT_SEC = 8;
    protected final int WORKER_TIMEOUT_MS = 30*1000;
    protected final int STATS_INTERVAL_MS = 5*1000;
//    protected int runTimeInSec = 0;
    protected String masterIP;
    protected int masterPort;

    public Communication(String masterIP, int masterPort) {
//        this.runTimeInSec = runTimeInSec;
        this.masterIP = masterIP;
        this.masterPort = masterPort;
    }
}
