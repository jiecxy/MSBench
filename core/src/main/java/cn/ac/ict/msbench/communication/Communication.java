package cn.ac.ict.msbench.communication;

import akka.actor.UntypedActor;

public abstract class Communication extends UntypedActor {

    protected final boolean NEED_INITIALIZE_MS = false;
    protected final boolean NEED_FINALIZE_MS = false;

//    protected int CHECK_TIMEOUT_SEC = 8;
    protected final int WORKER_TIMEOUT_MS = 8*1000;
    protected final int STATS_INTERVAL = 5;
//    protected int runTime = 0;
    protected String masterIP;
    protected int masterPort;

    public Communication(String masterIP, int masterPort) {
//        this.runTime = runTime;
        this.masterIP = masterIP;
        this.masterPort = masterPort;
    }
}
