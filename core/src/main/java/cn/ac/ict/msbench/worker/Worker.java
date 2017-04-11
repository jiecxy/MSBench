package cn.ac.ict.msbench.worker;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.communication.CallBack;

import org.HdrHistogram.Recorder;


public class Worker implements Runnable {


    CallBack cb;
    boolean isRunning = true;
    MS msClient = null;

    //stat variables
    long startTime;       // ns
    long lastStatTime;    // ns
    long numMsg;
    long numByte;
    long totalNumMsg;
    long totalNumByte;
    long requestTime;     // ns
    Recorder recorder = null;
    Recorder cumulativeRecorder = null;

    public Worker(CallBack cb) {
        this.cb = cb;
    }

    public void stopWork() {
        isRunning = false;
    }

    //TODO 这个为demo， 在worker父类中不具体实现
    public void run() {
        try {
            //TODO 发送头部
            //cb.onSendStatHeader(new StatHeader());

            // TODO 在测试时间内运行
            // TODO 需要记录时间来发送 window
            while (isRunning) {
                System.out.println("Worker run - 1");
                Thread.sleep(1000);
                System.out.println("Worker run - 2");

                // TODO 调用发消息
                //cb.onSendStatWindow(new StatWindow());

                System.out.println("Worker run - 3");
                Thread.sleep(1000);
                System.out.println("Worker run - 4");
            }

            //TODO 发送尾部
            //cb.onSendStatTail(new StatTail());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


}
