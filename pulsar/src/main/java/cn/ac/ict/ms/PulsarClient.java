package cn.ac.ict.ms;

import cn.ac.ict.MS;
import cn.ac.ict.Status;
import cn.ac.ict.exception.MSException;
import cn.ac.ict.worker.callback.ReadCallBack;
import cn.ac.ict.worker.callback.WriteCallBack;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class PulsarClient extends MS {
    //    com.yahoo.pulsar.client.api.PulsarClient client=null;
//    Producer producer=null;
//    Consumer consumer=null;
//    String URL;
//    String topic=null;
//    String subscription_name=null;
//    ClientConfiguration clientConf=null;
//    ProducerConfiguration producerConf=null;
//    ConsumerConfiguration consumerConf=null;
//    boolean IsProducer=false;
//    public void init() throws MSException, PulsarClientException {git
    public void init() throws MSException {
//        client = com.yahoo.pulsar.client.api.PulsarClient.create(URL,clientConf);
//        if(IsProducer)
//        {
//            producer = client.createProducer(topic);
//        }
//        else
//        {
//            consumer = client.subscribe(topic,subscription_name,consumerConf);
//        }
    }

    @Override
    public Status send(boolean isSync, byte[] msg, String stream, WriteCallBack sentCallBack) {
        return null;
    }

    @Override
    public Status read(String stream, ReadCallBack readCallBack) {
        return null;
    }


    public Status close() {
        return null;
    }

}
