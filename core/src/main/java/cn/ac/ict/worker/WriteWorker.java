package cn.ac.ict.worker;

import cn.ac.ict.communication.CallBack;
import cn.ac.ict.generator.Generator;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;
import cn.ac.ict.utils.SimpleGenerator;
import cn.ac.ict.utils.SimpleMS;


public class WriteWorker extends Worker {
    long startTime;
    Generator generator=null;
    public WriteWorker(CallBack cb,int statTime) {
        super(cb);
        startTime=System.nanoTime();
        statInterval=statTime;
        msClient=new SimpleMS();
        generator=new SimpleGenerator();
    }
    public void run()
    {
        cb.onSendStatHeader(new StatHeader());
        startTime=System.nanoTime();
        while (isGO) {
            if((System.nanoTime()-startTime)/1e9>statInterval)
            {
                cb.onSendStatWindow(new StatWindow());
                startTime=System.nanoTime();
            }
            msClient.send(generator.nextString());
        }
        cb.onSendStatTail(new StatTail());
    }
    public void stopWork()
    {
        isGO=false;
        if(msClient!=null)
            msClient.close();
        return;
    }
}
