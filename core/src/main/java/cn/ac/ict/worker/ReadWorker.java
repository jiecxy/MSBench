package cn.ac.ict.worker;

import cn.ac.ict.MS;
import cn.ac.ict.communication.CallBack;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;
import cn.ac.ict.utils.SimpleCallBack;
import cn.ac.ict.utils.SimpleMS;
import cn.ac.ict.worker.throughput.NoLimitThroughput;


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
        statTime=startTime; 
        //todo set MS's read mode
        while (isGO) {
            if((System.nanoTime()-startTime)/1e9>RunTime)
            {
                isGO=false;
                break;
            }
            if((System.nanoTime()-statTime)/1e9>statInterval)
            {
                cb.onSendStatWindow(new StatWindow());
                statTime=System.nanoTime();
            }
            msClient.read(streamName,this);
        }
        cb.onSendStatTail(new StatTail());
    }

    @Override
    public void stopWork() {
        if(msClient!=null)
            msClient.close();
        return;
    }
    public static void main(String[] args)
    {
        ReadWorker wk=new ReadWorker(new SimpleCallBack(),10,"stream-1",0,new SimpleMS());
        wk.run();
    }
}
