package cn.ac.ict.worker;

import cn.ac.ict.MS;
import cn.ac.ict.communication.CallBack;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;
import cn.ac.ict.utils.SimpleCallBack;
import cn.ac.ict.utils.SimpleMS;
import cn.ac.ict.worker.callback.ReadCallBack;
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
        numSize=0;
        totalNumMsg=0;
        totalNumSize=0;
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
                cb.onSendStatWindow(new StatWindow());
                statTime=System.nanoTime();
            }
            requestTime=System.nanoTime();
            msClient.read(stream,this);
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

    @Override
    public void handleReceivedMessage(byte[] msg) {
        System.out.println("received msg "+new String(msg));
        long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - requestTime);
        recorder.recordValue(latencyMicros);
        cumulativeRecorder.recordValue(latencyMicros);
        numMsg++;
        numSize+=msg.length;
        totalNumMsg++;
        totalNumSize+=msg.length;
    }
}
