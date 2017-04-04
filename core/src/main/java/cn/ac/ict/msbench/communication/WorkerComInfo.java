package cn.ac.ict.msbench.communication;

import akka.actor.ActorRef;
import cn.ac.ict.msbench.exporter.Exporter;
import cn.ac.ict.msbench.stat.MSBWorkerStat;
import cn.ac.ict.msbench.stat.StatHeader;
import cn.ac.ict.msbench.stat.StatTail;
import cn.ac.ict.msbench.stat.StatWindow;
import cn.ac.ict.msbench.worker.job.Job;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static cn.ac.ict.msbench.communication.Command.METRICS_HEAD;
import static cn.ac.ict.msbench.communication.Command.METRICS_TAIL;
import static cn.ac.ict.msbench.communication.Command.METRICS_WINDOW;

/**
 * Created by jiecxy on 2017/3/21.
 */
public class WorkerComInfo {

    public static enum STATUS {
        RUNNING, TERMINATED, DONE, TIMEOUT;
    }

    public String workerID;
    public ActorRef ref = null;
    public long lastHeartbeat = 0;
    public STATUS status = STATUS.RUNNING;
    public Job job;

    private MSBWorkerStat stat = new MSBWorkerStat();

    public WorkerComInfo(String workerID, ActorRef ref, Job job, long lastHeartbeat) {
        this.workerID = workerID;
        this.ref = ref;
        this.job = job;
        this.lastHeartbeat = lastHeartbeat;
        status = STATUS.RUNNING;
    }

    public void insertHeader(Exporter exporter, StatHeader header) {
        stat.head = header;
        if (exporter != null) {
            exporter.write(workerID, METRICS_HEAD, header.toString());
        }
    }

    //, int version
    public void insertWindow(Exporter exporter, StatWindow window) {
        stat.statWindow.add(window);
        if (exporter != null) {
            exporter.write(workerID, METRICS_WINDOW, window.toString());
        }
//        return true;
    }



    public void insertTail(Exporter exporter, StatTail tail) {
        stat.tail = tail;
        if (exporter != null) {
            exporter.write(workerID, METRICS_TAIL, tail.toString());
        }
    }
}
