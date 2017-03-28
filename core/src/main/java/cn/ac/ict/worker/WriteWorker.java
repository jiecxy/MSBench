package cn.ac.ict.worker;

import cn.ac.ict.MS;
import cn.ac.ict.communication.CallBack;
import cn.ac.ict.generator.Generator;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;
import cn.ac.ict.utils.ShiftableRateLimiter;
import cn.ac.ict.utils.SimpleCallBack;
import cn.ac.ict.utils.SimpleMS;
import cn.ac.ict.worker.callback.WriteCallBack;
import cn.ac.ict.worker.job.Job;
import cn.ac.ict.worker.job.WriteJob;
import cn.ac.ict.worker.throughput.GivenRandomChangeThroughputList;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

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
        WriteWorker wk = new WriteWorker(
                new SimpleCallBack(),
                new SimpleMS(),
                new WriteJob("SimpleMS","localhost",10,5,"stream-1",10,true,
                        new GivenRandomChangeThroughputList(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},1)));
        wk.run();
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
                cb.onSendStatWindow(new StatWindow((long) ((System.nanoTime()) / 1e6), numMsg / elapsed, numMsg, numByte / elapsed,
                        reportHist.getMean() / 1000.0, reportHist.getMaxValue() / 1000.0));

                numMsg=0;
                numByte=0;
                reportHist.reset();
                lastStatTime = System.nanoTime();
            }

            if (rateLimiter.getLimiter() != null)
                rateLimiter.getLimiter().acquire();

            requestTime = System.nanoTime();
            msClient.send(job.isSync, (byte[]) generator.nextValue(), job.streamName, this);

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
    public void handleSentMessage(byte[] msg) {
        long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - requestTime);
        recorder.recordValue(latencyMicros);
        cumulativeRecorder.recordValue(latencyMicros);
        System.out.println("received sned ack for msg " + new String(msg));
        numMsg++;
        numByte += msg.length;
        totalNumMsg++;
        totalNumByte += msg.length;
    }
}
