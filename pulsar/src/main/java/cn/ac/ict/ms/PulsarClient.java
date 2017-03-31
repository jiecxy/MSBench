package cn.ac.ict.ms;

import cn.ac.ict.MS;
import cn.ac.ict.exception.MSException;
import cn.ac.ict.worker.callback.ReadCallBack;
import cn.ac.ict.worker.callback.WriteCallBack;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.client.api.*;

import java.net.URL;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class PulsarClient extends MS {
    com.yahoo.pulsar.client.api.PulsarClient client = null;
    Producer producer = null;
    Consumer consumer = null;
    String URL="http://localhost:8080";
    String prefix="persistent://sample/standalone/ns1/";
    //    String topic = null;
    String subscription_name = null;
    ClientConfiguration clientConf = null;
    ProducerConfiguration producerConf = null;
    ConsumerConfiguration consumerConf = null;
    PulsarAdmin admin = null;

    public PulsarClient(String streamName, boolean isProducer, Properties p) {
        super(streamName, isProducer, p);
        initConfig(p);
        try {
            client = com.yahoo.pulsar.client.api.PulsarClient.create(URL, clientConf);
            if (isProducer) {
                producer = client.createProducer(prefix+streamName, producerConf);
            } else {
                consumer = client.subscribe(prefix+streamName, subscription_name, consumerConf);
            }
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

    }

    private void initConfig(Properties prop) {
        clientConf = new ClientConfiguration();
        if (isProducer)
            producerConf = new ProducerConfiguration();
        else
            consumerConf = new ConsumerConfiguration();
        //client config
        if (prop.containsKey("ioThreads"))
            clientConf.setIoThreads(Integer.valueOf(prop.getProperty("ioThreads")));
        if (prop.containsKey("connections"))
            clientConf.setConnectionsPerBroker(Integer.valueOf(prop.getProperty("connections")));
        if (prop.containsKey("listenerThreads"))
            clientConf.setListenerThreads(Integer.valueOf(prop.getProperty("listenerThreads")));
        if (prop.containsKey("tcpNoDelay"))
            clientConf.setUseTcpNoDelay(Boolean.valueOf(prop.getProperty("tcpNoDelay")));


        if (isProducer) { //producer config
            if (prop.containsKey("enableBatching"))
                producerConf.setBatchingEnabled(Boolean.valueOf(prop.getProperty("enableBatching")));
            if (prop.containsKey("batchSize"))
                producerConf.setBatchingMaxMessages(Integer.valueOf(prop.getProperty("batchSize")));
            if (prop.containsKey("batchDelayInMs"))
                producerConf.setBatchingMaxPublishDelay(Integer.valueOf(prop.getProperty("batchDelayInMs")), TimeUnit.MILLISECONDS);
            if (prop.containsKey("pendingMessages"))
                producerConf.setMaxPendingMessages(Integer.valueOf(prop.getProperty("pendingMessages")));
            if (prop.containsKey("compressionType"))
                producerConf.setCompressionType(CompressionType.valueOf(prop.getProperty("compressionType")));
            if (prop.containsKey("routingMode"))
                producerConf.setMessageRoutingMode(ProducerConfiguration.MessageRoutingMode.valueOf(prop.getProperty("routingMode")));
        } else { //consumer config
            if (prop.containsKey("receiveQueueSize"))
                consumerConf.setReceiverQueueSize(Integer.valueOf("receiveQueueSize"));
            if (prop.containsKey("subscriptionType"))
                consumerConf.setSubscriptionType(SubscriptionType.valueOf(prop.getProperty("subscriptionType")));
        }
    }

    @Override
    public void initializeMS(ArrayList<String> streams) throws MSException {
        try {
            admin=new PulsarAdmin(new URL(URL),new ClientConfiguration());
        }catch (Exception e) {
            throw new MSException(e);
        }

    }

    @Override
    public void finalizeMS(ArrayList<String> streams) throws MSException {
        try {
            if (admin != null) {
                for (String stream : streams)
                    admin.persistentTopics().delete(stream);
            }
        } catch (PulsarAdminException e) {
            throw new MSException(e);
        }
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
