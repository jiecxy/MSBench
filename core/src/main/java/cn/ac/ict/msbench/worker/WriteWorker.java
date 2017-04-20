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

    private static final Logger log = LoggerFactory.getLogger(ReadWorker.class);
    private Generator generator = null;
    private ShiftableRateLimiter rateLimiter;
    private WriteJob job;
    private Recorder recorder = null;
    private Recorder cumulativeRecorder = null;
    private long requestTime;     // ns

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
                new SimpleMS("stream-1", true, new Properties(), 0),
                new WriteJob("SimpleMS", "localhost", 20, 5, "stream-1", 10, true,
                        new GivenRandomChangeThroughputList(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, 1), 0));
        randomWK.run();
        WriteWorker noLimitWK = new WriteWorker(
                new SimpleCallBack(),
                new SimpleMS("stream-1", true, new Properties(), 0),
                new WriteJob("SimpleMS", "localhost", 10, 5, "stream-1", 10, true,
                        new NoLimitThroughput(), 0));
        noLimitWK.run();
        WriteWorker constantWK = new WriteWorker(
                new SimpleCallBack(),
                new SimpleMS("stream-1", true, new Properties(), 0),
                new WriteJob("SimpleMS", "localhost", 10, 5, "stream-1", 10, true,
                        new ConstantThroughput(5), 0));
        constantWK.run();
        WriteWorker gradualWK = new WriteWorker(
                new SimpleCallBack(),
                new SimpleMS("stream-1", true, new Properties(), 0),
                new WriteJob("SimpleMS", "localhost", 10, 5, "stream-1", 10, true,
                        new GradualChangeThroughput(1, 10, 2, 1), 0));
        gradualWK.run();
    }

    @Override
    public void run() {

        try {
            log.info("Writer delay start with " + job.delayStartSec +  " s.");
            Thread.sleep(job.delayStartSec * 1000);
        } catch (InterruptedException e) {
            log.error("Writer delay start got Exception " + e);
            e.printStackTrace();
        }

        log.info("Worker starting writing");
        startTime = System.nanoTime();
        lastStatTime = startTime;
        cb.onSendStatHeader(new StatHeader(job.system, job.streamName, job.runTimeInSec, System.currentTimeMillis(), job.statIntervalInSec, job.host, job.messageSize, job.strategy, job.isSync, job.delayStartSec));

        while (isRunning) {
            if (System.nanoTime() - startTime > job.runTimeInSec * 1e9) {
                isRunning = false;
                break;
            }

            if (System.nanoTime() - lastStatTime > job.statIntervalInSec * 1e9) {

                Histogram reportHist = null;
                long now = System.nanoTime();
                double elapsedInNano = now - lastStatTime;
                reportHist = recorder.getIntervalHistogram(reportHist);

                cb.onSendStatWindow(new StatWindow(System.currentTimeMillis(),
                        numMsg*1.0 / (elapsedInNano / 1e9),
                        numMsg,
                        numByte / (elapsedInNano / 1e9) / 1024.0 / 1024.0,
                        reportHist.getMean()/1000.0,
                        reportHist.getMaxValue()/1000.0,
                        true));

                numMsg = 0;
                numByte = 0;
                reportHist.reset();
                lastStatTime = System.nanoTime();
            }

            if (rateLimiter.getLimiter() != null)
                rateLimiter.getLimiter().acquire();

            requestTime = System.nanoTime();
            //System.out.println("worker start sending a msg");
            msClient.send(job.isSync, (byte[]) generator.nextValue(), this, requestTime);

        }

        Histogram reportHist = cumulativeRecorder.getIntervalHistogram();
        long now = System.nanoTime();
        double elapsedInNano = now - startTime;

        cb.onSendStatTail(
                new StatTail(System.currentTimeMillis(),
                        totalNumByte / 1024.0 / 1024.0 / (elapsedInNano / 1e9),
                        reportHist.getMean()/1000.0,
                        reportHist.getMaxValue()/1000.0,
                        reportHist.getValueAtPercentile(50)/1000.0,
                        reportHist.getValueAtPercentile(95)/1000.0,
                        reportHist.getValueAtPercentile(99)/1000.0,
                        reportHist.getValueAtPercentile(99.9)/1000.0,
                        totalNumMsg,
                        totalNumByte / 1024.0 / 1024.0,
                        true)
        );

        if (rateLimiter != null)
            rateLimiter.close();
    }

    public void stopWork() {
        log.info("Stopping writer thread");
        try {
            isRunning = false;
            if (rateLimiter != null)
                rateLimiter.close();
            if (msClient != null)
                msClient.close();
        } catch (Exception e) {
            log.error("Writer failed to stop! " + e);
        }
    }

    @Override
    public void handleSentMessage(byte[] msg, long requestTimeInNano) {
        //long latencyNano = System.nanoTime() - requestTimeInNano;
        long latencyMicros=NANOSECONDS.toMicros(System.nanoTime() - requestTimeInNano);
        recorder.recordValue(latencyMicros);
        cumulativeRecorder.recordValue(latencyMicros);
        numMsg++;
        numByte += msg.length;
        totalNumMsg++;
        totalNumByte += msg.length;
    }
}
