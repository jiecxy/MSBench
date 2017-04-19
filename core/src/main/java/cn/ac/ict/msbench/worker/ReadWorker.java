package cn.ac.ict.msbench.worker;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.communication.CallBack;
import cn.ac.ict.msbench.stat.StatHeader;
import cn.ac.ict.msbench.stat.StatTail;
import cn.ac.ict.msbench.stat.StatWindow;
import cn.ac.ict.msbench.utils.SimpleCallBack;
import cn.ac.ict.msbench.utils.SimpleMS;
import cn.ac.ict.msbench.worker.callback.ReadCallBack;
import cn.ac.ict.msbench.worker.job.Job;
import cn.ac.ict.msbench.worker.job.ReadJob;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.NANOSECONDS;


public class ReadWorker extends Worker implements ReadCallBack {

    private static final Logger log = LoggerFactory.getLogger(ReadWorker.class);
    private ReadJob job;
    private Recorder end2endRecorder;
    private Recorder cumulativeEnd2endRecorder;
    private long lastReadTime;

    public ReadWorker(CallBack cb, MS ms, Job job) {
        super(cb);
        this.job = (ReadJob) job;
        msClient = ms;

        numMsg = 0;
        numByte = 0;
        totalNumMsg = 0;
        totalNumByte = 0;
//        recorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
//        cumulativeRecorder = new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
        end2endRecorder=new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
        cumulativeEnd2endRecorder=new Recorder(TimeUnit.SECONDS.toMillis(120000), 5);
    }

    public static void main(String[] args) {
        ReadWorker wk = new ReadWorker(
                new SimpleCallBack(),
                new SimpleMS("stream-1",false,new Properties(), -1),
                new ReadJob("SimpleMS", "localhost", 20,  "stream-1",0 ,0));
        wk.run();
        //log.info("hello");
    }

    @Override
    public void run() {

        try {
            log.info("Reader delay start with " + job.delayStartSec +  " s.");
            Thread.sleep(job.delayStartSec * 1000);
        } catch (InterruptedException e) {
            log.error("Reader delay start got Exception " + e);
            e.printStackTrace();
        }

        log.info("Worker starting reading");
        startTime = System.nanoTime();
        lastStatTime = startTime;
//        log.debug("onSendStatHeader");
        cb.onSendStatHeader(new StatHeader(job.system, job.streamName, job.runTimeInSec, System.currentTimeMillis(), job.statIntervalInSec, job.host, job.from, job.delayStartSec));

        log.debug("ReadWork starts running");
        ReadWork rw = new ReadWork();
        rw.start();
        while (isRunning) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (System.nanoTime() - startTime > job.runTimeInSec * 1e9) {
//                log.debug("isRunning = false");

                isRunning = false;
                break;
            }

            if (System.nanoTime() - lastStatTime > job.statIntervalInSec * 1e9) {
//                Histogram reportHist = null;
                Histogram end2endReportHist = null;
                long now = System.nanoTime();
                double elapsedInNano = now - lastStatTime;
//                reportHist = recorder.getIntervalHistogram(reportHist);
                end2endReportHist = end2endRecorder.getIntervalHistogram(end2endReportHist);

//                log.debug("onSendStatWindow");
                cb.onSendStatWindow(
                        new StatWindow(System.currentTimeMillis(),
                                numMsg*1.0 / (elapsedInNano / 1e9),
                                numMsg,
                                numByte / (elapsedInNano /1e9) / 1024.0 / 1024.0,
                                end2endReportHist.getMean(),
                                end2endReportHist.getMaxValue(),
                                false));

                numMsg = 0;
                numByte = 0;
//                reportHist.reset();
                end2endReportHist.reset();
                lastStatTime = System.nanoTime();
            }
//            log.debug("msClient read requestTime" + requestTime);
        }
        rw.shutdown();
//        Histogram reportHist = cumulativeRecorder.getIntervalHistogram();
        Histogram end2endReportHist = cumulativeEnd2endRecorder.getIntervalHistogram();
        double elapsedInNano = lastReadTime - startTime;

//        log.debug("onSendStatTail");
        cb.onSendStatTail(
                new StatTail(System.currentTimeMillis(),
                        totalNumByte / 1024.0 / 1024.0 / (elapsedInNano / 1e9),
                        end2endReportHist.getMean(),
                        end2endReportHist.getMaxValue(),
                        end2endReportHist.getValueAtPercentile(50),
                        end2endReportHist.getValueAtPercentile(95),
                        end2endReportHist.getValueAtPercentile(99),
                        end2endReportHist.getValueAtPercentile(99.9),
                        totalNumMsg,
                        totalNumByte / 1024.0 / 1024.0,
                        false)
        );
    }

    @Override
    public void stopWork() {
        log.info("Stopping reader thread");
        isRunning = false;
        if (msClient != null)
            msClient.close();
        return;
    }

    @Override
    public void handleReceivedMessage(byte[] msg, long publishTimeInMillis) {

        //long latencyNano = System.nanoTime() - requestTimeInNano;
        //long latencyInMicros=NANOSECONDS.toMicros(System.nanoTime() - requestTimeInNano);
        // Note:!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        // since System.nanoTime() return the time based on JVM, not the real time, and the system always  use System.currentTimeMillis() to be the publish time of message, be care
        long now = System.currentTimeMillis();
        long end2endLatencyMillis = now - publishTimeInMillis;
//        log.debug("handleReceivedMessage: requestTimeInNano(" + requestTimeInNano + ") end2endLatencyMillis=" + end2endLatencyMillis + " " + now + " - " + publishTimeInMillis);
        //System.out.println("end2endLatencyNano=" + end2endLatencyNano + " now=" + now  + " publishTimeInNano=" + end2endLatencyNano);
        try {
            //recorder.recordValue(latencyInMicros);
            //cumulativeRecorder.recordValue(latencyInMicros);
            end2endRecorder.recordValue(end2endLatencyMillis);
            cumulativeEnd2endRecorder.recordValue(end2endLatencyMillis);
        } catch (ArrayIndexOutOfBoundsException e) {
            log.error("Invalid Latency! Please synchronize client's time! \n" + e);
            stopWork();
        }
        numMsg++;
        numByte += msg.length;
        totalNumMsg++;
        totalNumByte += msg.length;
        lastReadTime = System.nanoTime();
    }

    class ReadWork extends Thread {
        @Override
        public void run() {
            super.run();
            msClient.read(ReadWorker.this::handleReceivedMessage);
        }

        public void shutdown() {
            msClient.stopRead();
        }
    }
}
