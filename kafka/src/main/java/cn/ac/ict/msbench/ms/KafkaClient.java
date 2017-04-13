package cn.ac.ict.msbench.ms;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.exception.MSException;
import cn.ac.ict.msbench.worker.callback.ReadCallBack;
import cn.ac.ict.msbench.worker.callback.WriteCallBack;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;

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

    private static final String DEFAULT_BATCH_SIZE = "65536";
    private static final String DEFAULT_BUFFER_MEMORY = "4194304";
    private static final String DEFAULT_ACKS = "all";
    private static final String DEFAULT_LINGER_MS = "20";

    private String controllerIP = "localhost";  // controller's ip
    private int controllerPort = 9092;
    private int partitions = 18;
    private short replicationFactor = 1;
    private KafkaProducer<byte[], byte[]> producer = null;
    private KafkaConsumer<byte[], byte[]> consumer = null;

    public KafkaClient(String streamName, boolean isProducer, Properties p, int from) throws MSException {
        super(streamName, isProducer, p, from);

        if (p == null) {
            log.error("Kafka Missing the config file which should include the bootstrap.servers at least!");
            throw new MSException("Missing the config file");
        }

        if (!extractMSBasic(p)) {
            log.warn("If you disabled NEED_INITIALIZE_MS or NEED_FINALIZE_MS, you need to do them by yourself!");
        }

        if (isProducer) {
            setValue(p, ProducerConfig.BATCH_SIZE_CONFIG, DEFAULT_BATCH_SIZE);
            setValue(p, ProducerConfig.BUFFER_MEMORY_CONFIG, DEFAULT_BUFFER_MEMORY);
            setValue(p, ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS);
            setValue(p, ProducerConfig.LINGER_MS_CONFIG, DEFAULT_LINGER_MS);
            p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            log.info("Kafka Producer config: " + p.toString());
            producer = new KafkaProducer<>(p);
        } else {
            setValue(p, ConsumerConfig.GROUP_ID_CONFIG, streamName + "-" + (new Random(System.currentTimeMillis()).nextInt(10000)));
            setValue(p, ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.setProperty(AUTO_OFFSET_RESET_CONFIG, from == 0 ? "earliest" : "latest");
            log.info("Kafka Consumer config: " + p.toString());
            consumer = new KafkaConsumer<>(p);
            consumer.subscribe(Collections.singletonList(streamName));
        }
    }

    private void setValue(Properties p, String key, String value) {
        if (!p.containsKey(key)) {
            p.setProperty(key, value);
        }
    }

    private boolean extractMSBasic(Properties p) {
        try {
            controllerIP = (String) p.remove(CONTROLLER_IP);
            controllerPort = Integer.parseInt((String)p.remove(CONTROLLER_PORT));
            partitions = Integer.parseInt((String)p.remove(TOPIC_PARTITIONS));
            replicationFactor = Short.parseShort((String)p.remove(REPLICATION_FACTOR));
            return true;
        } catch (Exception e) {
            log.warn(e.toString());
            return false;
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
    public void send(boolean isSync, final byte[] msg, final WriteCallBack sentCallBack, final long requestTimeInNano) {
        log.debug("send message (isSync=" + isSync + "): " + msg);
        if (isSync) {
            try {

                producer.send(new ProducerRecord<>(streamName, null, System.currentTimeMillis(), null, msg)).get();
                sentCallBack.handleSentMessage(msg, requestTimeInNano);
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
                                    sentCallBack.handleSentMessage(msg, requestTimeInNano);
                                }
                            }
                    });
        }
    }
    //TODO 配置读的位置，提交offset
    @Override
    public void read(ReadCallBack readCallBack, long requestTimeInNano) {
        log.debug("read messages by poll");
        ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
        for (ConsumerRecord<byte[], byte[]> record : records) {
            readCallBack.handleReceivedMessage(record.value().toString().getBytes(), requestTimeInNano, record.timestamp()*1000000);
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
