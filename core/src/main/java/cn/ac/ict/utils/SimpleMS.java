package cn.ac.ict.utils;

import cn.ac.ict.MS;
import cn.ac.ict.worker.callback.ReadCallBack;
import cn.ac.ict.worker.callback.WriteCallBack;

public class SimpleMS extends MS {

    @Override
    public void send(boolean isSync,byte[] msg, String stream, WriteCallBack sentCallBack) {
        if(isSync)
        {
            System.out.println("Sync sending message " + new String(msg) + " to " + stream);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Sync received SEND ack");
            sentCallBack.handleSentMessage(msg);
        }
        else {
            System.out.println("Async sending message " + new String(msg) + " to " + stream);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Async received SEND ack");
            sentCallBack.handleSentMessage(msg);
        }
        return;
    }


    @Override
    public void read(String stream, ReadCallBack readCallBack) {
        System.out.println("receiving message from "+stream);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("received SEND ack");
        readCallBack.handleReceivedMessage("my-message".getBytes());
        return;
    }

    public void close() {
        System.out.println("close MSclient");
        return;
    }
}
