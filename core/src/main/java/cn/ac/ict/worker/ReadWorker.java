package cn.ac.ict.worker;

import cn.ac.ict.MS;
import cn.ac.ict.communication.CallBack;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;
import cn.ac.ict.utils.SimpleCallBack;
import cn.ac.ict.utils.SimpleMS;
import cn.ac.ict.worker.callback.ReadCallBack;
import cn.ac.ict.worker.job.Job;
import cn.ac.ict.worker.job.ReadJob;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


public class ReadWorker extends Worker implements ReadCallBack {

    private ReadJob job;

    public ReadWorker(CallBack cb, MS ms, Job job) {
        super(cb);
        this.job = (ReadJob) job;
        msClient = ms;

        numMsg = 0;
        numByte = 0;
        totalNumMsg = 0;
        totalNumByte = 0;
        recorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
        cumulativeRecorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
    }

    public static void main(String[] args) {
        ReadWorker wk = new ReadWorker(
                new SimpleCallBack(),
                new SimpleMS(),
                new ReadJob("SimpleMS", "localhost", 10, 5, "stream-1", 0));
        wk.run();
    }

    @Override
    public void run() {

        startTime = System.nanoTime();
        lastStatTime = startTime;

        cb.onSendStatHeader(new StatHeader(job.system, job.streamName, job.runTime, (long) (startTime / 1e6), job.statInterval, job.host, job.from));

        //todo set MS's read mode

        while (isRunning) {

            if ((System.nanoTime() - startTime) / 1e9 > job.runTime) {
                isRunning = false;
                break;
            }

            if ((System.nanoTime() - lastStatTime) / 1e9 > job.statInterval) {
                Histogram reportHist = null;
                double elapsed = (System.nanoTime() - lastStatTime) / 1e9;
                reportHist = recorder.getIntervalHistogram(reportHist);

                cb.onSendStatWindow(new StatWindow((long) ((System.nanoTime()) / 1e6), numMsg / elapsed, numMsg, numByte / elapsed,
                        reportHist.getMean() / 1000.0, reportHist.getMaxValue() / 1000.0));

                numMsg = 0;
                numByte = 0;
                reportHist.reset();
                lastStatTime = System.nanoTime();
            }

            requestTime = System.nanoTime();
            msClient.read(job.streamName, this);
        }

        Histogram reportHist = cumulativeRecorder.getIntervalHistogram();
        double elapsed = (System.nanoTime() - startTime) / 1e9;

        cb.onSendStatTail(
                new StatTail((long) ((System.nanoTime()) / 1e6), (totalNumByte / 1024 / 1024) / elapsed, reportHist.getMean() / 1000.0, reportHist.getMaxValue() / 1000.0,
                        reportHist.getValueAtPercentile(50) / 1000.0, reportHist.getValueAtPercentile(95) / 1000.0,
                        reportHist.getValueAtPercentile(99) / 1000.0, reportHist.getValueAtPercentile(99.9) / 1000.0,
                        totalNumMsg, (long) (totalNumByte / 1024 / 1024), false)
        );
    }

    @Override
    public void stopWork() {
        isRunning = false;
        if (msClient != null)
            msClient.close();
        return;
    }

    @Override
    public void handleReceivedMessage(byte[] msg) {
        System.out.println("received msg " + new String(msg));
        long latencyMicros = NANOSECONDS.toMicros(System.nanoTime() - requestTime);
        recorder.recordValue(latencyMicros);
        cumulativeRecorder.recordValue(latencyMicros);
        numMsg++;
        numByte += msg.length;
        totalNumMsg++;
        totalNumByte += msg.length;
    }
}
