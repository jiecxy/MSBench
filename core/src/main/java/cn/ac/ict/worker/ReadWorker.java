package cn.ac.ict.worker;

import cn.ac.ict.MS;
import cn.ac.ict.communication.CallBack;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;


public class ReadWorker extends Worker {
    int StartPoint;
    public ReadWorker(CallBack cb,int runTime, String stream, int from, MS ms) {
        super(cb);
        RunTime=runTime;
        streamName=stream;
        StartPoint=from;
        msClient=ms;
    }

    @Override
    public void run() {
        cb.onSendStatHeader(new StatHeader());
        startTime=System.nanoTime();
        while (isGO) {
            if((System.nanoTime()-startTime)/1e9>RunTime)
                break;
            if((System.nanoTime()-statTime)/1e9>statInterval)
            {
                cb.onSendStatWindow(new StatWindow());
                statTime=System.nanoTime();
            }
            msClient.read(streamName);
        }
        cb.onSendStatTail(new StatTail());
    }

    @Override
    public void stopWork() {
        if(msClient!=null)
            msClient.close();
        return;
    }
}
