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
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


public class WriteWorker extends Worker implements WriteCallBack {

    private Generator generator = null;
    private int msgSize = 1024;
    private ThroughputStrategy writeStrategy;
    private boolean isSync;
    private ShiftableRateLimiter rateLimiter;

    //TODO 抽象数据参数类
    public WriteWorker(CallBack cb, int runTime, String stream, MS ms, int messageSize, boolean isSync, ThroughputStrategy strategy) {
        super(cb);
        generator=new SimpleGenerator(messageSize);
        this.runTime=runTime;
        this.stream=stream;
        msClient=ms;
        this.isSync=isSync;
        writeStrategy=strategy;
        rateLimiter=new ShiftableRateLimiter(writeStrategy);

        numMsg=0;
        numByte =0;
        totalNumMsg=0;
        totalNumByte =0;
        recorder=new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
        cumulativeRecorder=new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
    }

    @Override
    public void run()
    {
        cb.onSendStatHeader(new StatHeader());
        startTime=System.nanoTime();
        lastStatTime =startTime;
        while (isGO) {
            if((System.nanoTime()-startTime)/1e9>runTime)
            {
                isGO=false;
                break;
            }
            if((System.nanoTime()- lastStatTime)/1e9>statInterval)
            {
                Histogram reportHist = null;
                double elapsed=(System.nanoTime()-startTime)/1e9;
                reportHist=recorder.getIntervalHistogram(reportHist);
                cb.onSendStatWindow(new StatWindow(numMsg/elapsed,numMsg,numByte/elapsed,reportHist.getMean()/1000.0,reportHist.getMaxValue()/1000.0));
                reportHist.reset();
                lastStatTime =System.nanoTime();
            }
            if(rateLimiter.getLimiter()!=null)
                rateLimiter.getLimiter().acquire();
            requestTime=System.nanoTime();
            msClient.send(isSync,(byte[])generator.nextValue(),stream,this);

        }
        Histogram reportHist=cumulativeRecorder.getIntervalHistogram();
        double elapsed=(System.nanoTime()-startTime)/1e9;
        cb.onSendStatTail(
                new StatTail(totalNumMsg, totalNumByte,totalNumMsg/elapsed,reportHist.getMean()/1000.0,reportHist.getMaxValue()/1000.0,
                        reportHist.getValueAtPercentile(50)/1000.0,reportHist.getValueAtPercentile(95)/1000.0,
                        reportHist.getValueAtPercentile(99)/1000.0,reportHist.getValueAtPercentile(99.9)/1000.0,
                        0,0)
        );
        if(rateLimiter!=null)
            rateLimiter.close();
    }
    public void stopWork()
    {
        isGO=false;
        if(rateLimiter!=null)
            rateLimiter.close();
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
        numByte +=msg.length;
        totalNumMsg++;
        totalNumByte +=msg.length;
    }
}
