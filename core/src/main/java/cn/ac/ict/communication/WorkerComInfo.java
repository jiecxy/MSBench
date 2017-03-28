package cn.ac.ict.communication;

import akka.actor.ActorRef;
import cn.ac.ict.stat.MSBWorkerStat;

/**
 * Created by jiecxy on 2017/3/21.
 */
public class WorkerComInfo {

    public static enum STATUS {
        RUNNING, TERMINATED, DONE, TIMEOUT;
    }

    public ActorRef ref = null;
    public long lastHeartbeat = 0;
    public STATUS status = STATUS.RUNNING;

    public MSBWorkerStat stat = new MSBWorkerStat();

    public WorkerComInfo(ActorRef ref, long lastHeartbeat) {
        this.ref = ref;
        this.lastHeartbeat = lastHeartbeat;
        status = STATUS.RUNNING;
    }
}
