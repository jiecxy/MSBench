package cn.ac.ict.msbench.ms;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.exception.MSException;
import cn.ac.ict.msbench.worker.callback.ReadCallBack;
import cn.ac.ict.msbench.worker.callback.WriteCallBack;
import com.yahoo.pulsar.client.admin.PulsarAdmin;
import com.yahoo.pulsar.client.admin.PulsarAdminException;
import com.yahoo.pulsar.client.api.*;
import com.yahoo.pulsar.client.impl.PulsarClientImpl;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang.SystemUtils;

import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
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
    String URL = "http://localhost:8080";
    String prefix = "persistent://sample/standalone/ns1/";
    //    String topic = null;
    String subscription_name = null;
    ClientConfiguration clientConf = null;
    ProducerConfiguration producerConf = null;
    ConsumerConfiguration consumerConf = null;
    PulsarAdmin admin = null;

    public PulsarClient(String streamName, boolean isProducer, Properties p, int from) {
        super(streamName, isProducer, p, from);
        initConfig(p);
        System.out.println("properties initialized");
        try {
            admin = new PulsarAdmin(new URL(URL), new ClientConfiguration());
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("pulsar admin created");
        try {
            EventLoopGroup eventLoopGroup;
            if (SystemUtils.IS_OS_LINUX) {
                eventLoopGroup = new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors(),
                        new DefaultThreadFactory("pulsar-perf-producer"));
            } else {
                eventLoopGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(),
                        new DefaultThreadFactory("pulsar-perf-producer"));
            }
            client = new PulsarClientImpl(URL, clientConf, eventLoopGroup);
            System.out.println("pulsar client created");
            if (isProducer) {
                System.out.println("creating a pulsar producer on " + prefix + streamName);
                producer = client.createProducer(prefix + streamName, producerConf);
                System.out.println("created a pulsar producer");
            } else {
                if (from == -1) {
                    admin.persistentTopics().skipAllMessages(prefix + streamName, subscription_name);
                    System.out.println("creating a pulsar consumer on " + prefix + streamName + " from end");
                } else
                    System.out.println("creating a pulsar consumer on " + prefix + streamName + " from start");

                consumer = client.subscribe(prefix + streamName, subscription_name, consumerConf);
                System.out.println("created a pulsar consumer on " + prefix + streamName);
            }
        } catch (PulsarClientException e) {
            System.out.println("pulsar client error");
            e.printStackTrace();
        } catch (PulsarAdminException e) {
            e.printStackTrace();
        }
    }

    /*public static void main(String[] args) {

    }*/
    private String getProp(Properties prop, String key) {
        return (String) (prop.getProperty(key));
    }

    private void initConfig(Properties prop) {
        if (prop.containsKey("serviceUrl"))
            URL = getProp(prop, "serviceUrl");
        if (prop.containsKey("destinationPrefix"))
            prefix = getProp(prop, "destinationPrefix");
        if (prop.containsKey("subscriptionName"))
            subscription_name = getProp(prop, "subscriptionName");
        else
            try {
                subscription_name = "my-subscription" + InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }

        clientConf = new ClientConfiguration();
        if (isProducer)
            producerConf = new ProducerConfiguration();
        else
            consumerConf = new ConsumerConfiguration();

        //client config
        if (prop.containsKey("ioThreads"))
            clientConf.setIoThreads(Integer.valueOf(getProp(prop, "ioThreads")));
        if (prop.containsKey("connections"))
            clientConf.setConnectionsPerBroker(Integer.valueOf(getProp(prop, "connections")));
        if (prop.containsKey("listenerThreads"))
            clientConf.setListenerThreads(Integer.valueOf(getProp(prop, "listenerThreads")));
        if (prop.containsKey("tcpNoDelay"))
            clientConf.setUseTcpNoDelay(Boolean.valueOf(getProp(prop, "tcpNoDelay")));


        if (isProducer) { //producer config
            if (prop.containsKey("enableBatching"))
                producerConf.setBatchingEnabled(Boolean.valueOf(getProp(prop, "enableBatching")));
            if (prop.containsKey("batchSize"))
                producerConf.setBatchingMaxMessages(Integer.valueOf(getProp(prop, "batchSize")));
            if (prop.containsKey("batchDelayInMs"))
                producerConf.setBatchingMaxPublishDelay(Integer.valueOf(getProp(prop, "batchDelayInMs")), TimeUnit.MILLISECONDS);
            if (prop.containsKey("pendingMessages"))
                producerConf.setMaxPendingMessages(Integer.valueOf(getProp(prop, "pendingMessages")));
            if (prop.containsKey("compressionType"))
                producerConf.setCompressionType(CompressionType.valueOf(getProp(prop, "compressionType")));
            if (prop.containsKey("routingMode"))
                producerConf.setMessageRoutingMode(ProducerConfiguration.MessageRoutingMode.valueOf(getProp(prop, "routingMode")));
        } else { //consumer config
            if (prop.containsKey("receiveQueueSize"))
                consumerConf.setReceiverQueueSize(Integer.valueOf(getProp(prop, "receiveQueueSize")));
            if (prop.containsKey("subscriptionType"))
                consumerConf.setSubscriptionType(SubscriptionType.valueOf(getProp(prop, "subscriptionType")));
        }
    }

    @Override
    public void initializeMS(ArrayList<String> streams) throws MSException {
    }

    @Override
    public void finalizeMS(ArrayList<String> streams) throws MSException {
        try {
            if (producer != null)
                producer.close();
            if (consumer != null) {
                consumer.unsubscribe();
                consumer.close();
            }
            if (admin != null) {
                for (String stream : streams)
                    admin.persistentTopics().delete(prefix + stream);
            }
        } catch (PulsarAdminException e) {
            throw new MSException(e);
        } catch (PulsarClientException e) {
            throw new MSException(e);
        }
    }

    @Override
    public void send(boolean isSync, byte[] msg, WriteCallBack sentCallBack, long requestTime) {
        //System.out.println("pulsar begin to send a msg");
        try {
            if (isSync) {
                producer.send(msg);
                sentCallBack.handleSentMessage(msg, requestTime);
            } else {
                producer.sendAsync(msg).thenRun(() -> {
                            //System.out.println("sent a msg");
                            sentCallBack.handleSentMessage(msg, requestTime);
                        }
                ).exceptionally(ex -> {
                    return null;
                });
            }

        } catch (Exception e) {
            System.out.println("send error "+e);
        }
    }

    @Override
    public void read(ReadCallBack readCallBack, long requestTime) {
        //System.out.println("pulsar begin to receive a msg");
        /*CompletableFuture<Message> future = consumer.receiveAsync();
        future.handle((msg, exception) ->
        {
            if (exception == null) {
                try {
                    //System.out.println("receive a msg");
                    consumer.acknowledge(msg);
                    readCallBack.handleReceivedMessage(msg.getData(), requestTime, msg.getPublishTime());
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }
            }
            return null;
        });*/
        consumer.receiveAsync().thenAccept((msg) -> {
            try {
                //System.out.println("receive a msg");
                consumer.acknowledge(msg);
                readCallBack.handleReceivedMessage(msg.getData(), requestTime, msg.getPublishTime());
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }).exceptionally(exception -> {
            System.out.println("received error" + exception);
            return null;
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
            if (consumer != null) {
                consumer.unsubscribe();
                consumer.close();
            }
            if (client != null)
                client.close();
            if (admin != null)
                admin.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

}