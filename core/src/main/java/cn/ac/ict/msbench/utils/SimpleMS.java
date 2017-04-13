package cn.ac.ict.msbench.utils;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.exception.MSException;
import cn.ac.ict.msbench.exporter.FileExporter;
import cn.ac.ict.msbench.worker.callback.ReadCallBack;
import cn.ac.ict.msbench.worker.callback.WriteCallBack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;

public class SimpleMS extends MS {

    private static final Logger log = LoggerFactory.getLogger(SimpleMS.class);

    public SimpleMS(String streamName, boolean isProducer, Properties p, int from) {
        super(streamName, isProducer, p, from);
        if (isProducer) {
            log.info("create a writer");
        } else {
            if (from == 0)
                log.info("create a reader reading from the begining");
            else
                log.info("create a reader reading from the ending");
        }
    }

    @Override
    public void initializeMS(ArrayList<String> streams) throws MSException {
        String str = "";
        for (String stream : streams) {
            str += stream + ",";
        }
        log.info("create streams: " + str.substring(0, str.length() - 1));
    }

    @Override
    public void finalizeMS(ArrayList<String> streams) throws MSException {
        String str = "";
        for (String stream : streams) {
            str += stream + ",";
        }
        log.info("delete streams" + str.substring(0, str.length() - 1));
    }

    @Override
    public void send(boolean isSync, byte[] msg, WriteCallBack sentCallBack, long requestTimeInNano) {
        if (isSync) {
//            System.out.println("Sync sending message " + new String(msg) );
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            System.out.println("Sync received SEND ack");
            sentCallBack.handleSentMessage(msg, requestTimeInNano);
        } else {
//            System.out.println("Async sending message " + new String(msg) );
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            System.out.println("Async received SEND ack");
            sentCallBack.handleSentMessage(msg, requestTimeInNano);
        }
        return;
    }

    @Override
    public void read(ReadCallBack readCallBack, long requestTimeInNano) {
//        System.out.println("receiving message from ");
        long publishTimeInMillis =System.currentTimeMillis();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //System.out.println("received SEND ack");
        readCallBack.handleReceivedMessage("my-message".getBytes(), requestTimeInNano, publishTimeInMillis);
        return;
    }

    public void close() {
//        System.out.println("close MSClient");
        return;
    }
}
