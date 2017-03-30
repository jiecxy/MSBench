package cn.ac.ict.ms;

import cn.ac.ict.MS;
import cn.ac.ict.exception.MSException;
import cn.ac.ict.worker.callback.ReadCallBack;
import cn.ac.ict.worker.callback.WriteCallBack;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.api.*;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class PulsarClient extends MS {
    com.yahoo.pulsar.client.api.PulsarClient client = null;

    Producer producer = null;
    Consumer consumer = null;
    String URL;
    //    String topic = null;
    String subscription_name = null;
    ClientConfiguration clientConf = null;
    ProducerConfiguration producerConf = null;
    ConsumerConfiguration consumerConf = null;
    PulsarAdmin admin=null;

    public PulsarClient(String streamName, boolean isProducer, Properties p) {
        super(streamName, isProducer, p);
        try {
            client = com.yahoo.pulsar.client.api.PulsarClient.create(URL, clientConf);
            if (isProducer) {
                producer = client.createProducer(streamName, producerConf);
            } else {
                consumer = client.subscribe(streamName, subscription_name, consumerConf);
            }
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void initializeMS(ArrayList<String> streams) throws MSException {
    }

    @Override
    public void finalizeMS(ArrayList<String> streams) throws MSException {

    }


    @Override
    public void send(boolean isSync, byte[] msg, WriteCallBack sentCallBack, long requestTime) {
        try {
            if (isSync) {
                producer.send(msg);
                sentCallBack.handleSentMessage(msg, requestTime);
            } else {
                producer.sendAsync(msg).thenRun(() -> {
                            sentCallBack.handleSentMessage(msg, requestTime);
                        }
                ).exceptionally(ex -> {
                    return null;
                });
            }

        } catch (Exception e) {
            System.out.println("hello");
        }
    }

    @Override
    public void read(ReadCallBack readCallBack, long requestTime) {
        consumer.receiveAsync().thenAccept((msg) -> {
            try {
                consumer.acknowledge(msg);
                readCallBack.handleReceivedMessage(msg.getData(), requestTime);
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
//        consumerConf.setMessageListener(new MessageListener() {
//            @Override
//                public void received(Consumer consumer, Message message) {
//                consumer.acknowledge(msg);
//                readCallBack.handleReceivedMessage(message.getData());
//            }
//        });
    }


    public void close() {
        try {
            if (producer != null)
                producer.close();
            if (consumer != null)
                consumer.close();
            client.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

}
