package cn.ac.ict.communication;

import akka.actor.Actor;
import akka.actor.UntypedActor;
import cn.ac.ict.MS;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

public abstract class Communication extends UntypedActor {

    protected int CHECK_TIMEOUT_SEC = 8;
    protected int STATS_INTERVAL = 5;
//    protected int runTime = 0;
    protected String masterIP;
    protected int masterPort;

    public Communication(String masterIP, int masterPort) {
//        this.runTime = runTime;
        this.masterIP = masterIP;
        this.masterPort = masterPort;
    }
}
