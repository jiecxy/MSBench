import cn.ac.ict.MS;
import cn.ac.ict.Status;
import cn.ac.ict.exception.MSException;
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
    boolean IsProducer=false;
    public void init() throws MSException {
        try {
            client = com.yahoo.pulsar.client.api.PulsarClient.create(URL,clientConf);
            if(IsProducer)
            {
                producer = client.createProducer(topic);
            }
            else
            {
                consumer = client.subscribe(topic,subscription_name,consumerConf);
            }
        } catch (Exception e) {
            throw new MSException(e);
        }

    }
    public Status send(String msg) {

        return null;
    }

    public Status read() {

        return null;
    }

    public Status close() {
        try {
            client.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        return null;
    }
}
