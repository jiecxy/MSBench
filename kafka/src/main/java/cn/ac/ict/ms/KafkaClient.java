package cn.ac.ict.ms;

import cn.ac.ict.MS;
import cn.ac.ict.exception.MSException;
import cn.ac.ict.worker.callback.ReadCallBack;
import cn.ac.ict.worker.callback.WriteCallBack;
import org.apache.kafka.clients.consumer.Consumer;
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
import java.util.concurrent.ExecutionException;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class KafkaClient extends MS {

    private String brokerIP = "";  // controller's ip
    private int brokerPort = 9092;
    private int partitions = 12;
    private short replicationFactor = 3;
    private KafkaProducer<byte[], byte[]> producer = null;
    private KafkaConsumer<byte[], byte[]> consumer = null;

    public KafkaClient(String streamName, boolean isProducer, Properties p) {
        super(streamName, isProducer, p);
        if (isProducer) {
            producer = new KafkaProducer<>(p);
        } else {
            consumer = new KafkaConsumer<>(p);
            consumer.subscribe(Collections.singletonList(streamName));
        }
    }

    private void createTopics(ArrayList<String> streams) throws IOException {
        for (String name: streams) {
            TopicUtils.createTopic(brokerIP, brokerPort, name, partitions, replicationFactor);
        }
    }

    private void deleteTopics(ArrayList<String> streams) throws IOException {
        for (String name: streams) {
            TopicUtils.deleteTopic(brokerIP, brokerPort, name);
        }
    }


    @Override
    public void initializeMS(ArrayList<String> streams) throws MSException {
        try {
            createTopics(streams);
        } catch (IOException e) {
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

    @Override
    public void read(ReadCallBack readCallBack, long requestTime) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
        for (ConsumerRecord<byte[], byte[]> record : records) {
            readCallBack.handleReceivedMessage(record.value().toString().getBytes(), requestTime);
        }
    }

    public void finalizeMS(ArrayList<String> streams) {
        try {
            deleteTopics(streams);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        if (producer != null)
            producer.close();
    }
}
