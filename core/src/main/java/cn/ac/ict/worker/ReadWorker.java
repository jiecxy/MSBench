package cn.ac.ict.worker;

import cn.ac.ict.MS;
import cn.ac.ict.communication.CallBack;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;
import cn.ac.ict.utils.SimpleCallBack;
import cn.ac.ict.utils.SimpleMS;
import cn.ac.ict.worker.callback.ReadCallBack;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


public class ReadWorker extends Worker implements ReadCallBack {
    int StartPoint;

    public ReadWorker(CallBack cb,int runTime, String stream, int from, MS ms) {
        super(cb);
        this.runTime=runTime;
        this.stream=stream;
        StartPoint=from;
        msClient=ms;

        numMsg=0;
        numByte =0;
        totalNumMsg=0;
        totalNumByte =0;
        recorder=new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
        cumulativeRecorder=new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
    }

    @Override
    public void run() {
        cb.onSendStatHeader(new StatHeader());
        startTime=System.nanoTime();
        statTime=startTime; 
        //todo set MS's read mode
        while (isGO) {
            if((System.nanoTime()-startTime)/1e9>runTime)
            {
                isGO=false;
                break;
            }
            if((System.nanoTime()-statTime)/1e9>statInterval)
            {
                Histogram reportHist=null;
                double elapsed=(System.nanoTime()-startTime)/1e9;
                reportHist=recorder.getIntervalHistogram(reportHist);
                cb.onSendStatWindow(new StatWindow(numMsg/elapsed,numMsg,numByte/elapsed,reportHist.getMean()/1000.0,reportHist.getMaxValue()/1000.0));
                reportHist.reset();
                statTime=System.nanoTime();
            }
            requestTime=System.nanoTime();
            msClient.read(stream,this);
        }
        Histogram reportHist=cumulativeRecorder.getIntervalHistogram();
        double elapsed=(System.nanoTime()-startTime)/1e9;
        cb.onSendStatTail(
                new StatTail(0,0,totalNumMsg/elapsed,reportHist.getMean()/1000.0,reportHist.getMaxValue()/1000.0,
                        reportHist.getValueAtPercentile(50)/1000.0,reportHist.getValueAtPercentile(95)/1000.0,
                        reportHist.getValueAtPercentile(99)/1000.0,reportHist.getValueAtPercentile(99.9)/1000.0,
                        totalNumMsg, totalNumByte)
        );
    }

    @Override
    public void stopWork() {
        isGO=false;
        if(msClient!=null)
            msClient.close();
        return;
    }
    public static void main(String[] args)
    {
        ReadWorker wk=new ReadWorker(new SimpleCallBack(),10,"stream-1",0,new SimpleMS());
        wk.run();
    }

    @Override
    public void handleReceivedMessage(byte[] msg) {
        System.out.println("received msg "+new String(msg));
        long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - requestTime);
        recorder.recordValue(latencyMicros);
        cumulativeRecorder.recordValue(latencyMicros);
        numMsg++;
        numByte +=msg.length;
        totalNumMsg++;
        totalNumByte +=msg.length;
    }
}
