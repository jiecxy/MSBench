package cn.ac.ict.ms;

import cn.ac.ict.MS;
import cn.ac.ict.Status;
import cn.ac.ict.exception.MSException;
import cn.ac.ict.worker.callback.ReadCallBack;
import cn.ac.ict.worker.callback.WriteCallBack;
import com.yahoo.pulsar.client.api.*;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class PulsarClient extends MS {
    com.yahoo.pulsar.client.api.PulsarClient client=null;
    Producer producer=null;
    Consumer consumer=null;
    String URL;
    String topic=null;
    String subscription_name=null;
    ClientConfiguration clientConf=null;
    ProducerConfiguration producerConf=null;
    ConsumerConfiguration consumerConf=null;
    boolean isProducer=false;

    public void init() throws MSException {
        try {
            client = com.yahoo.pulsar.client.api.PulsarClient.create(URL,clientConf);
            if(isProducer)
            {
                producer = client.createProducer(topic,producerConf);
            }
            else
            {
                consumer = client.subscribe(topic,subscription_name,consumerConf);
            }
        } catch (PulsarClientException e) {
            throw new MSException(e);
        }

    }

    @Override
    public Status send(boolean isSync, byte[] msg, String stream, WriteCallBack sentCallBack)
    {
        try{
            if(isSync)
            {
                producer.send(msg);
                sentCallBack.handleSentMessage(msg);
            }
            else
            {
                producer.sendAsync(msg).thenRun(()->{
                    sentCallBack.handleSentMessage(msg);
                }
                ).exceptionally(ex->{
                    return null;
                });
            }

        }catch (Exception e)
        {
            System.out.println("hello");
        }
        return null;
    }

    @Override
    public Status read(String stream, ReadCallBack readCallBack) {
        consumer.receiveAsync().thenAccept((msg)->{
            try {
                consumer.acknowledge(msg);
                readCallBack.handleReceivedMessage(msg.getData());
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        });
        return null;
    }


    public Status close() {
            try {
                if(producer!=null)
                producer.close();
                if (consumer!=null)
                    consumer.close();
                client.close();
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }

        return null;
    }

}
