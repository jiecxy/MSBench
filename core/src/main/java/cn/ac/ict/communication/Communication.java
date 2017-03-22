package cn.ac.ict.communication;

import akka.actor.Actor;
import akka.actor.UntypedActor;
import cn.ac.ict.MS;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

public abstract class Communication extends UntypedActor {

    protected int CHECK_TIMEOUT_SEC = 8;
    protected MS ms = null;
    protected ArrayList<String> streams = null;
    protected int runTime = 0;
    protected Properties props = null;
    protected int heartbeat = 0;

//    public Communication(MS ms, ArrayList<String> streams, int runTime, Properties props) {
//        this.ms = ms;
//        this.streams = streams;
//        this.runTime = runTime;
//        this.props = props;
//    }
}
