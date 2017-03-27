package cn.ac.ict.worker;

import cn.ac.ict.MS;
import cn.ac.ict.communication.CallBack;
import cn.ac.ict.generator.Generator;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;
import cn.ac.ict.utils.ShiftableRateLimiter;
import cn.ac.ict.utils.SimpleCallBack;
import cn.ac.ict.utils.SimpleGenerator;
import cn.ac.ict.utils.SimpleMS;
import cn.ac.ict.worker.callback.WriteCallBack;
import cn.ac.ict.worker.throughput.GivenRandomChangeThroughputList;
import cn.ac.ict.worker.throughput.ThroughputStrategy;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


public class WriteWorker extends Worker implements WriteCallBack {

    Generator generator=null;
    int msgSize=1024;
    ThroughputStrategy writeStrategy;
    boolean isSync;
    ShiftableRateLimiter rateLimiter;

    public WriteWorker(CallBack cb,int runTime, String stream, MS ms, int messageSize, boolean isSync, ThroughputStrategy strategy) {
        super(cb);
        generator=new SimpleGenerator(messageSize);
        this.runTime=runTime;
        this.stream=stream;
        msClient=ms;
        this.isSync=isSync;
        writeStrategy=strategy;
        rateLimiter=new ShiftableRateLimiter(writeStrategy);

        numMsg=0;
        numSize=0;
        totalNumMsg=0;
        totalNumSize=0;
        recorder=new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
        cumulativeRecorder=new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
    }

    public void run()
    {
        cb.onSendStatHeader(new StatHeader());
        startTime=System.nanoTime();
        statTime=startTime;
        while (isGO) {
            if((System.nanoTime()-startTime)/1e9>runTime)
            {
                isGO=false;
                break;
            }
            if((System.nanoTime()-statTime)/1e9>statInterval)
            {
                cb.onSendStatWindow(new StatWindow());
                statTime=System.nanoTime();
            }
            if(rateLimiter.getLimiter()!=null)
                rateLimiter.getLimiter().acquire();
            requestTime=System.nanoTime();
            msClient.send(isSync,(byte[])generator.nextValue(),stream,this);

        }
        cb.onSendStatTail(new StatTail());
        rateLimiter.close();
    }
    public void stopWork()
    {
        isGO=false;
        if(msClient!=null)
            msClient.close();
        return;
    }
    public static void main(String[] args)
    {
        WriteWorker wk=new WriteWorker(new SimpleCallBack(),10,"stream-1",new SimpleMS(),10,true,
                new GivenRandomChangeThroughputList(new int[]{1,2,3,4,5,6,7,8,9,10,11},1));
        wk.run();
    }

    @Override
    public void handleSentMessage(byte[] msg) {
        long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - requestTime);
        recorder.recordValue(latencyMicros);
        cumulativeRecorder.recordValue(latencyMicros);
        System.out.println("received sned ack for msg "+new String(msg));
        numMsg++;
        numSize+=msg.length;
        totalNumMsg++;
        totalNumSize+=msg.length;
    }
}
