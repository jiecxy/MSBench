package cn.ac.ict.msbench.ms;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.exception.MSException;
import cn.ac.ict.msbench.worker.callback.ReadCallBack;
import cn.ac.ict.msbench.worker.callback.WriteCallBack;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class KafkaClient extends MS {

    private static final Logger log = LoggerFactory.getLogger(KafkaClient.class);

    private static final String CONTROLLER_IP = "controller.ip";
    private static final String CONTROLLER_PORT = "controller.port";
    private static final String TOPIC_PARTITIONS = "partition.num";
    private static final String REPLICATION_FACTOR = "replication.factor";

    private String controllerIP = "";  // controller's ip
    private int controllerPort = 9092;
    private int partitions = 12;
    private short replicationFactor = 3;
    private KafkaProducer<byte[], byte[]> producer = null;
    private KafkaConsumer<byte[], byte[]> consumer = null;

    public KafkaClient(String streamName, boolean isProducer, Properties p, int from) throws MSException {
        super(streamName, isProducer, p, from);

        if (p == null) {
            log.error("Kafka Missing the config file");
            throw new MSException("Missing the config file");
        }
//        System.out.println("Properties: " + p);

        controllerIP = (String) p.remove(CONTROLLER_IP);
        controllerPort = Integer.parseInt((String)p.remove(CONTROLLER_PORT));
        partitions = Integer.parseInt((String)p.remove(TOPIC_PARTITIONS));
        replicationFactor = Short.parseShort((String)p.remove(REPLICATION_FACTOR));

        if (isProducer) {
            producer = new KafkaProducer<>(p);
        } else {
            p.setProperty(AUTO_OFFSET_RESET_CONFIG, from == 0 ? "earliest" : "latest");
            consumer = new KafkaConsumer<>(p);
            consumer.subscribe(Collections.singletonList(streamName));
        }
    }

    private void createTopics(ArrayList<String> streams) throws IOException {
        for (String name: streams) {
            TopicUtils.createTopic(controllerIP, controllerPort, name, partitions, replicationFactor);
        }
    }

    private void deleteTopics(ArrayList<String> streams) throws IOException {
        for (String name: streams) {
            TopicUtils.deleteTopic(controllerIP, controllerPort, name);
        }
    }

    @Override
    public void initializeMS(ArrayList<String> streams) throws MSException {
        try {
            log.info("Creating Topic " + streams);
            createTopics(streams);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Creating Topic " + streams + " Failed");
            throw new MSException("Create Topics Failed");
        }
    }

    @Override
    public void send(boolean isSync, final byte[] msg, final WriteCallBack sentCallBack, final long requestTime) {
        log.debug("send message (isSync=" + isSync + "): " + msg);
        if (isSync) {
            try {

                producer.send(new ProducerRecord<>(streamName, null, System.currentTimeMillis(), null, msg)).get();
                sentCallBack.handleSentMessage(msg, requestTime);
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Failed to send message (isSync=" + isSync + "): " + msg);
            }
        } else {
            producer.send(new ProducerRecord<>(streamName, null, System.currentTimeMillis(), null, msg), new Callback() {
                            public void onCompletion(RecordMetadata metadata, Exception e) {
                                if(e != null) {
                                    e.printStackTrace();
                                    log.error("Failed to send message (isSync=" + isSync + "): " + msg);
                                } else {
                                    sentCallBack.handleSentMessage(msg, requestTime);
                                }
                            }
                    });
        }
    }
    //TODO 配置读的位置，提交offset
    @Override
    public void read(ReadCallBack readCallBack, long requestTime) {
        log.debug("read messages by poll");
        ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
        for (ConsumerRecord<byte[], byte[]> record : records) {
            readCallBack.handleReceivedMessage(record.value().toString().getBytes(), requestTime, record.timestamp());
        }
    }

    @Override
    public void finalizeMS(ArrayList<String> streams) throws MSException {
        try {
            log.info("Deleting Topic " + streams);
            deleteTopics(streams);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Deleting Topic " + streams + " Failed");
            throw new MSException("Delete Topics Failed");
        }
    }

    @Override
    public void close() {
        if (producer != null)
            producer.close();
        if (consumer != null)
            consumer.close();
    }
}
