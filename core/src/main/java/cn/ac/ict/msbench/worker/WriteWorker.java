package cn.ac.ict.msbench.worker;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.communication.CallBack;
import cn.ac.ict.msbench.generator.Generator;
import cn.ac.ict.msbench.stat.StatHeader;
import cn.ac.ict.msbench.stat.StatTail;
import cn.ac.ict.msbench.stat.StatWindow;
import cn.ac.ict.msbench.utils.ShiftableRateLimiter;
import cn.ac.ict.msbench.utils.SimpleCallBack;
import cn.ac.ict.msbench.utils.SimpleMS;
import cn.ac.ict.msbench.worker.callback.WriteCallBack;
import cn.ac.ict.msbench.worker.job.Job;
import cn.ac.ict.msbench.worker.job.WriteJob;
import cn.ac.ict.msbench.worker.throughput.ConstantThroughput;
import cn.ac.ict.msbench.worker.throughput.GivenRandomChangeThroughputList;
import cn.ac.ict.msbench.worker.throughput.GradualChangeThroughput;
import cn.ac.ict.msbench.worker.throughput.NoLimitThroughput;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


public class WriteWorker extends Worker implements WriteCallBack {

    private Generator generator = null;
    private ShiftableRateLimiter rateLimiter;
    private WriteJob job;

    public WriteWorker(CallBack cb, MS ms, Job job) {
        super(cb);
        this.job = (WriteJob) job;
        this.generator = this.job.generator;//new SimpleGenerator(messageSize);
        msClient = ms;
        rateLimiter = new ShiftableRateLimiter(this.job.strategy);

        // stat parameters init
        numMsg = 0;
        numByte = 0;
        totalNumMsg = 0;
        totalNumByte = 0;
        recorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
        cumulativeRecorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);//man length is 120000*1000
    }

    public static void main(String[] args) {
        WriteWorker randomWK = new WriteWorker(
                new SimpleCallBack(),
                new SimpleMS("stream-1",true,new Properties(), 0),
                new WriteJob("SimpleMS","localhost",10,5,"stream-1",10,true,
                        new GivenRandomChangeThroughputList(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},1)));
        randomWK.run();
        WriteWorker noLimitWK = new WriteWorker(
                new SimpleCallBack(),
                new SimpleMS("stream-1",true,new Properties(), 0),
                new WriteJob("SimpleMS","localhost",10,5,"stream-1",10,true,
                        new NoLimitThroughput()));
        noLimitWK.run();
        WriteWorker constantWK = new WriteWorker(
                new SimpleCallBack(),
                new SimpleMS("stream-1",true,new Properties(), 0),
                new WriteJob("SimpleMS","localhost",10,5,"stream-1",10,true,
                        new ConstantThroughput(5)));
        constantWK.run();
        WriteWorker gradualWK = new WriteWorker(
                new SimpleCallBack(),
                new SimpleMS("stream-1",true,new Properties(), 0),
                new WriteJob("SimpleMS","localhost",10,5,"stream-1",10,true,
                        new GradualChangeThroughput(1,10,2,1)));
        gradualWK.run();
    }

    @Override
    public void run() {

        startTime = System.nanoTime();
        lastStatTime = startTime;

        cb.onSendStatHeader(new StatHeader(job.system, job.streamName, job.runTime, (long) (startTime / 1e6), job.statInterval, job.host, job.messageSize, job.strategy, job.isSync));

        while (isRunning) {
            if ((System.nanoTime() - startTime) / 1e9 > job.runTime) {
                isRunning = false;
                break;
            }

            if ((System.nanoTime() - lastStatTime) / 1e9 > job.statInterval) {

                Histogram reportHist = null;
                double elapsed = (System.nanoTime() - lastStatTime) / 1e9;
                reportHist = recorder.getIntervalHistogram(reportHist);
                // TODO 需不需要将stats存在worker上
                cb.onSendStatWindow(new StatWindow((long) ((System.nanoTime()) / 1e6), numMsg / elapsed, numMsg, numByte / elapsed/1024/1024,
                        reportHist.getMean() / 1000.0, reportHist.getMaxValue() / 1000.0));

                numMsg=0;
                numByte=0;
                reportHist.reset();
                lastStatTime = System.nanoTime();
            }

            if (rateLimiter.getLimiter() != null)
                rateLimiter.getLimiter().acquire();

            requestTime = System.nanoTime();
            msClient.send(job.isSync, (byte[]) generator.nextValue(),  this,requestTime);

        }

        Histogram reportHist = cumulativeRecorder.getIntervalHistogram();
        double elapsed = (System.nanoTime() - startTime) / 1e9;

        cb.onSendStatTail(
                new StatTail((long) ((System.nanoTime()) / 1e6), (totalNumByte/1024/1024) / elapsed, reportHist.getMean() / 1000.0, reportHist.getMaxValue() / 1000.0,
                        reportHist.getValueAtPercentile(50) / 1000.0, reportHist.getValueAtPercentile(95) / 1000.0,
                        reportHist.getValueAtPercentile(99) / 1000.0, reportHist.getValueAtPercentile(99.9) / 1000.0,
                        totalNumMsg, (long)(totalNumByte/1024/1024), job.isSync)
        );

        if (rateLimiter != null)
            rateLimiter.close();
    }

    public void stopWork() {
        isRunning = false;
        if (rateLimiter != null)
            rateLimiter.close();
        if (msClient != null)
            msClient.close();
    }

    @Override
    public void handleSentMessage(byte[] msg,long requestTime) {
        long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - requestTime);
        recorder.recordValue(latencyMicros);
        cumulativeRecorder.recordValue(latencyMicros);
//        System.out.println("received sned ack for msg " + new String(msg));
        numMsg++;
        numByte += msg.length;
        totalNumMsg++;
        totalNumByte += msg.length;
    }
    private static final Logger log = LoggerFactory.getLogger(ReadWorker.class);
}
