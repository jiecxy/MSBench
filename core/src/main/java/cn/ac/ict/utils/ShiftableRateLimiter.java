package cn.ac.ict.utils;

import cn.ac.ict.worker.throughput.ConstantThroughput;
import cn.ac.ict.worker.throughput.GivenRandomChangeThroughputList;
import cn.ac.ict.worker.throughput.GradualChangeThroughput;
import cn.ac.ict.worker.throughput.ThroughputStrategy;
import com.google.common.util.concurrent.RateLimiter;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static cn.ac.ict.worker.throughput.ThroughputStrategy.TPMODE.*;

/**
 * Created by kangsiqi on 2017/3/22 0022.
 */
public class ShiftableRateLimiter implements Runnable {
    private RateLimiter rateLimiter=null;
    private ScheduledExecutorService executor=null;
    private int InitialThroughput=-1, FinalThroughput=-1, changeThroughput=-1;
    private int[] RandomThroughputChange=null;
    private long changeInterval=0;
    private int nextThroughput=0;
    private int i = 0;
    ThroughputStrategy writeStrategy;
    public ShiftableRateLimiter(ThroughputStrategy strategy)
    {
        i=0;
        if(strategy.mode==NoLimit)
        {
            rateLimiter=null;
        }
        else if(strategy.mode==Constant)
        {
            this.InitialThroughput=((ConstantThroughput)strategy).tp;
            this.rateLimiter = RateLimiter.create(InitialThroughput);
        }
        else if(strategy.mode==GradualChange)
        {
            this.InitialThroughput=((GradualChangeThroughput)strategy).tp;
            this.FinalThroughput=((GradualChangeThroughput)strategy).ftp;
            this.changeThroughput=((GradualChangeThroughput)strategy).ctp;
            this.changeInterval=((GradualChangeThroughput)strategy).ctps;
            this.rateLimiter=RateLimiter.create(InitialThroughput);
            this.executor = Executors.newSingleThreadScheduledExecutor();
            this.executor.scheduleAtFixedRate(this, changeInterval, changeInterval, TimeUnit.SECONDS);
        }
        else if(strategy.mode==GivenRandomChangeList)
        {
            this.RandomThroughputChange=((GivenRandomChangeThroughputList)strategy).rtpl;
            this.changeInterval=((GivenRandomChangeThroughputList)strategy).ctps;
            this.rateLimiter=RateLimiter.create(RandomThroughputChange[i]);
            this.executor = Executors.newSingleThreadScheduledExecutor();
            this.executor.scheduleAtFixedRate(this, changeInterval, changeInterval, TimeUnit.SECONDS);
        }
        writeStrategy=strategy;
    }
    public ShiftableRateLimiter(ThroughputStrategy strategy,TimeUnit changeIntervalUnit)
    {
        if(strategy.mode==NoLimit)
        {
            rateLimiter=null;
        }
        else if(strategy.mode==Constant)
        {
            this.InitialThroughput=((ConstantThroughput)strategy).tp;
            this.rateLimiter = RateLimiter.create(InitialThroughput);
        }
        else if(strategy.mode==GradualChange)
        {
            this.InitialThroughput=((GradualChangeThroughput)strategy).tp;
            this.FinalThroughput=((GradualChangeThroughput)strategy).ftp;
            this.changeThroughput=((GradualChangeThroughput)strategy).ctp;
            this.changeInterval=((GradualChangeThroughput)strategy).ctps;
            this.rateLimiter=RateLimiter.create(InitialThroughput);
            this.executor = Executors.newSingleThreadScheduledExecutor();
            this.executor.scheduleAtFixedRate(this, changeInterval, changeInterval, changeIntervalUnit);
        }
        else if(strategy.mode==GivenRandomChangeList)
        {
            this.RandomThroughputChange=((GivenRandomChangeThroughputList)strategy).rtpl;
            this.changeInterval=((GivenRandomChangeThroughputList)strategy).ctps;
            this.rateLimiter=RateLimiter.create(RandomThroughputChange[i]);
            this.executor = Executors.newSingleThreadScheduledExecutor();
            this.executor.scheduleAtFixedRate(this, changeInterval, changeInterval, changeIntervalUnit);
        }
        writeStrategy=strategy;
    }

    public void run() {
        if (writeStrategy.mode == GradualChange && InitialThroughput < FinalThroughput) {
            this.nextThroughput = Math.min(nextThroughput + changeThroughput, FinalThroughput);
        } else if (writeStrategy.mode == GradualChange && InitialThroughput > FinalThroughput) {
            this.nextThroughput = Math.max(nextThroughput - changeThroughput, FinalThroughput);
        } else if (writeStrategy.mode == GivenRandomChangeList) {
            this.nextThroughput = RandomThroughputChange[i++ % RandomThroughputChange.length];
            i=i%RandomThroughputChange.length;
        }
        this.rateLimiter.setRate(nextThroughput);
        //System.out.println("ratelimiter change to "+Integer.toString(nextThroughput));
    }
    public void close()
    {
        rateLimiter=null;
        if(executor!=null)
            executor.shutdown();
    }

    public RateLimiter getLimiter() {
        return this.rateLimiter;
    }
}
