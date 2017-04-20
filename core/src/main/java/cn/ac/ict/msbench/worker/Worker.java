package cn.ac.ict.msbench.worker;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.communication.CallBack;

import org.HdrHistogram.Recorder;


public abstract class Worker implements Runnable {


    CallBack cb;
    boolean isRunning = true;
    MS msClient = null;

    //stat variables
    long startTime;       // ns
    long lastStatTime;    // ns
    long numMsg;
    long numByte;
    long totalNumMsg;
    long totalNumByte;
//    long requestTime;     // ns
//    Recorder recorder = null;
//    Recorder cumulativeRecorder = null;

    public Worker(CallBack cb) {
        this.cb = cb;
    }

    public abstract void stopWork();
}
