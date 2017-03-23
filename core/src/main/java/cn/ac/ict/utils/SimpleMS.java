package cn.ac.ict.utils;

import cn.ac.ict.MS;
import cn.ac.ict.Status;

public class SimpleMS extends MS {

    public Status send(String msg) {
        System.out.println("sending message");
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("received SEND ack");
        return null;
    }

    public Status read() {

        return null;
    }

    public Status close() {
        System.out.println("close MSclient");
        return null;
    }
}
