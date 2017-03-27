package cn.ac.ict.ms;

import cn.ac.ict.MS;
import cn.ac.ict.exception.MSException;
import cn.ac.ict.worker.callback.ReadCallBack;
import cn.ac.ict.worker.callback.WriteCallBack;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class KafkaClient extends MS {

    private ArrayList<String> streams = null;
    private String brokerIP = "";  // controller's ip
    private int brokerPort = 9092;
    private int partitions = 12;
    private short replicationFactor = 3;

    @Override
    public void init(ArrayList<String> streams) throws MSException {
        this.streams = streams;
        try {
            createTopics();
        } catch (IOException e) {
            throw new MSException("Create Topics Failed");
        }
    }

    private void createTopics() throws IOException {
        for (String name: streams) {
            TopicUtils.createTopic(brokerIP, brokerPort, name, partitions, replicationFactor);
        }
    }

    private void deleteTopics() throws IOException {
        for (String name: streams) {
            TopicUtils.deleteTopic(brokerIP, brokerPort, name);
        }
    }

    @Override
    public void send(boolean isSync, byte[] msg, String stream, WriteCallBack sentCallBack) {

    }

    @Override
    public void read(String stream, ReadCallBack readCallBack) {

    }

    public void close() {
        try {
            deleteTopics();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
