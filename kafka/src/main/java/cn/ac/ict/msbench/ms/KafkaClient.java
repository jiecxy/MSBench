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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class KafkaClient extends MS {

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

    public KafkaClient(String streamName, boolean isProducer, Properties p, int from) {
        super(streamName, isProducer, p, from);

        controllerIP = (String) p.remove(CONTROLLER_IP);
        controllerPort = (int) p.remove(CONTROLLER_PORT);
        partitions = (int) p.remove(TOPIC_PARTITIONS);
        replicationFactor = (short) p.remove(REPLICATION_FACTOR);

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

    public static void main(String[] args) {

    }

    @Override
    public void initializeMS(ArrayList<String> streams) throws MSException {
        try {
            createTopics(streams);
        } catch (IOException e) {
            e.printStackTrace();
            throw new MSException("Create Topics Failed");
        }
    }

    @Override
    public void send(boolean isSync, final byte[] msg, final WriteCallBack sentCallBack, final long requestTime) {
        if (isSync) {
            try {
                producer.send(new ProducerRecord<byte[], byte[]>(streamName, null, System.currentTimeMillis(), null, msg)).get();
                sentCallBack.handleSentMessage(msg, requestTime);
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Message " + msg + " send fail!");
            }
        } else {
            producer.send(new ProducerRecord<byte[], byte[]>(streamName, null, System.currentTimeMillis(), null, msg), new Callback() {
                            public void onCompletion(RecordMetadata metadata, Exception e) {
                                if(e != null) {
                                    e.printStackTrace();
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
        ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
        for (ConsumerRecord<byte[], byte[]> record : records) {
            readCallBack.handleReceivedMessage(record.value().toString().getBytes(), requestTime);
        }
    }

    @Override
    public void finalizeMS(ArrayList<String> streams) throws MSException {
        try {
            deleteTopics(streams);
        } catch (IOException e) {
            e.printStackTrace();
            throw new MSException("Delete Topics Failed");
        }
    }

    @Override
    public void close() {
        if (producer != null)
            producer.close();
    }
}
