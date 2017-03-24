package cn.ac.ict.worker;

import cn.ac.ict.MS;
import cn.ac.ict.communication.CallBack;
import cn.ac.ict.communication.WorkerCallBack;
import cn.ac.ict.generator.Generator;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;
import cn.ac.ict.utils.SimpleGenerator;
import cn.ac.ict.utils.SimpleMS;
import cn.ac.ict.worker.throughput.ThroughputStrategy;

import static cn.ac.ict.worker.throughput.ThroughputStrategy.TPMODE.*;


public class WriteWorker extends Worker {

    Generator generator=null;
    int msgSize=1024;
    ThroughputStrategy writeStrategy;
    boolean IsSync;

    public WriteWorker(CallBack cb,int runTime, String stream, MS ms, int messageSize, boolean isSync, ThroughputStrategy strategy) {
        super(cb);
        generator=new SimpleGenerator(messageSize);
        RunTime=runTime;
        streamName=stream;
        msClient=ms;
        IsSync=isSync;
        writeStrategy=strategy;
        init();
    }
    private void init()
    {
        if(writeStrategy.mode==NoLimit)
        {
            //Ratelimiter=null;
        }
        else if(writeStrategy.mode==Constant)
        {
            //Ratelimiter=new RateLimiter(writeStrategy.tp);
        }
        else if(writeStrategy.mode==GradualChange)
        {
            //Ratelimiter=new RateLimiter(writeStrategy.tp,writeStrategy.ftp,writeStrategy.ctp,writeStrategy.ctps);
        }
        else if(writeStrategy.mode==GivenRandomChangeList)
        {
            //Ratelimiter=new RateLimiter(writeStrategy.rtpl,writeStrategy.ctps);
        }
        return;
    }
    public void run()
    {
        cb.onSendStatHeader(new StatHeader());
        startTime=System.nanoTime();
        statTime=startTime;
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
            msClient.send(IsSync,(byte[])generator.nextValue(),streamName,this);
        }
        cb.onSendStatTail(new StatTail());
    }
    public void stopWork()
    {
        isGO=false;
        if(msClient!=null)
            msClient.close();
        return;
    }
    public void main()
    {

    }

}
