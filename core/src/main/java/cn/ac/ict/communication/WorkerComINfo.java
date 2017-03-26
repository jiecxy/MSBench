package cn.ac.ict.communication;

import akka.actor.ActorRef;

/**
 * Created by jiecxy on 2017/3/21.
 */
public class WorkerComINfo {

    public static enum STATUS {
        RUNNING, TERMINATED;
    }

    public ActorRef ref = null;
    public long lastHeartbeat = 0;
    public STATUS status = STATUS.RUNNING;

    public WorkerComINfo(ActorRef ref, long lastHeartbeat) {
        this.ref = ref;
        this.lastHeartbeat = lastHeartbeat;
        status = STATUS.RUNNING;
    }
}