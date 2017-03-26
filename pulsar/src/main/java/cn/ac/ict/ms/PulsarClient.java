package cn.ac.ict.ms;

import cn.ac.ict.MS;
import cn.ac.ict.Status;
<<<<<<< HEAD
import cn.ac.ict.worker.WorkerCallBack;
=======
import cn.ac.ict.communication.WorkerCallBack;
>>>>>>> 8ba886c9d5476be4b7635ab10742ff5039f3be94
import cn.ac.ict.exception.MSException;

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
    public Status send(boolean isSync, byte[] msg, String stream, WorkerCallBack sentCallBack) {
        return null;
    }

    @Override
    public Status read(String stream, WorkerCallBack readCallBack) {
        return null;
    }

    public Status close() {
        return null;
    }

}
