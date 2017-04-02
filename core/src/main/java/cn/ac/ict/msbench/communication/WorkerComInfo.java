package cn.ac.ict.msbench.communication;

import akka.actor.ActorRef;
import cn.ac.ict.msbench.stat.MSBWorkerStat;
import cn.ac.ict.msbench.worker.job.Job;

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
    public Job job;

    public MSBWorkerStat stat = new MSBWorkerStat();

    public WorkerComInfo(ActorRef ref, Job job, long lastHeartbeat) {
        this.ref = ref;
        this.job = job;
        this.lastHeartbeat = lastHeartbeat;
        status = STATUS.RUNNING;
    }
}
