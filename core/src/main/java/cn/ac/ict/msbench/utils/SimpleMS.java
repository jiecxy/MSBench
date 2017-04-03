package cn.ac.ict.msbench.utils;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.exception.MSException;
import cn.ac.ict.msbench.worker.callback.ReadCallBack;
import cn.ac.ict.msbench.worker.callback.WriteCallBack;

import java.util.ArrayList;
import java.util.Properties;

public class SimpleMS extends MS {

    public SimpleMS(String streamName, boolean isProducer, Properties p, int from) {
        super(streamName, isProducer, p, from);
    }

    @Override
    public void initializeMS(ArrayList<String> streams) throws MSException {
        String str="";
        for (String stream : streams)
        {
            str+=stream+",";
        }
        System.out.println("create streams: "+str.substring(0,str.length()-1));
    }

    @Override
    public void finalizeMS(ArrayList<String> streams) throws MSException {
        String str="";
        for (String stream : streams)
        {
            str+=stream+",";
        }
        System.out.println("delete streams"+str.substring(0,str.length()-1));
    }

    @Override
    public void send(boolean isSync, byte[] msg, WriteCallBack sentCallBack, long requestTime) {
        if(isSync)
        {
            System.out.println("Sync sending message " + new String(msg) );
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Sync received SEND ack");
            sentCallBack.handleSentMessage(msg , requestTime);
        }
        else {
            System.out.println("Async sending message " + new String(msg) );
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Async received SEND ack");
            sentCallBack.handleSentMessage(msg,requestTime);
        }
        return;
    }

    @Override
    public void read(ReadCallBack readCallBack, long requestTime) {
        //System.out.println("receiving message from ");
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //System.out.println("received SEND ack");
        readCallBack.handleReceivedMessage("my-message".getBytes(),requestTime);
        return;
    }

    public void close() {
        System.out.println("close MSclient");
        return;
    }
}