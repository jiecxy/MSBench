package cn.ac.ict.ms;


import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by jiecxy on 2017/3/9.
 */
public class TopicUtils {

    public static void main(String[] args) throws Exception {
//        TopicUtils utils = new TopicUtils();
//        if (args[0].toString().trim().equals("create"))
//            utils.createTopic(args[1], Integer.parseInt(args[2]), args[3], Integer.parseInt(args[4]), Short.parseShort(args[5]));
//        else if (args[0].toString().trim().equals("delete"))
//            utils.deleteTopic(args[1], Integer.parseInt(args[2]), args[3]);
    }

    /**
     * 发送请求主方法
     * @param host          目标broker的主机名
     * @param port          目标broker的端口
     * @param request       请求对象
     * @param apiKey        请求类型
     * @return              序列化后的response
     * @throws IOException
     */
    public static ByteBuffer send(String host, int port, AbstractRequest request, ApiKeys apiKey) throws IOException {
        Socket socket = connect(host, port);
        try {
            return send(request, apiKey, socket);
        } finally {
            socket.close();
        }
    }

    /**
     * 发送序列化请求并等待response返回
     * @param socket            连向目标broker的socket
     * @param request           序列化后的请求
     * @return                  序列化后的response
     * @throws IOException
     */
    private static byte[] issueRequestAndWaitForResponse(Socket socket, byte[] request) throws IOException {
        sendRequest(socket, request);
        return getResponse(socket);
    }

    /**
     * 发送序列化请求给socket
     * @param socket            连向目标broker的socket
     * @param request           序列化后的请求
     * @throws IOException
     */
    private static void sendRequest(Socket socket, byte[] request) throws IOException {
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(request.length);
        dos.write(request);
        dos.flush();
    }

    /**
     * 从给定socket处获取response
     * @param socket            连向目标broker的socket
     * @return                  获取到的序列化后的response
     * @throws IOException
     */
    private static byte[] getResponse(Socket socket) throws IOException {
        DataInputStream dis = null;
        try {
            dis = new DataInputStream(socket.getInputStream());
            byte[] response = new byte[dis.readInt()];
            dis.readFully(response);
            return response;
        } finally {
            if (dis != null) {
                dis.close();
            }
        }
    }

    /**
     * 创建Socket连接
     * @param hostName          目标broker主机名
     * @param port              目标broker服务端口, 比如9092
     * @return                  创建的Socket连接
     * @throws IOException
     */
    private static Socket connect(String hostName, int port) throws IOException {
        return new Socket(hostName, port);
    }

    /**
     * 向给定socket发送请求
     * @param request       请求对象
     * @param apiKey        请求类型, 即属于哪种请求
     * @param socket        连向目标broker的socket
     * @return              序列化后的response
     * @throws IOException
     */
    private static ByteBuffer send(AbstractRequest request, ApiKeys apiKey, Socket socket) throws IOException {
        RequestHeader header = new RequestHeader(apiKey.id, request.version(), "", 0);
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + request.sizeOf());
        header.writeTo(buffer);
        request.writeTo(buffer);
        byte[] serializedRequest = buffer.array();
        byte[] response = issueRequestAndWaitForResponse(socket, serializedRequest);
        ByteBuffer responseBuffer = ByteBuffer.wrap(response);
        ResponseHeader.parse(responseBuffer);
        return responseBuffer;
    }


    /**
     * 创建topic
     * 由于只是样例代码，有些东西就硬编码写到程序里面了(比如主机名和端口)，各位看官自行修改即可
     * @param topicName             topic名
     * @param partitions            分区数
     * @param replicationFactor     副本数
     * @throws IOException
     */
    public static boolean createTopic(String brokerIP, int brokerPort, String topicName, int partitions, short replicationFactor) throws IOException {
        Map<String, CreateTopicsRequest.TopicDetails> topics = new HashMap<>();
        // 插入多个元素便可同时创建多个topic
        topics.put(topicName, new CreateTopicsRequest.TopicDetails(partitions, replicationFactor));
        int creationTimeoutMs = 60000;
        CreateTopicsRequest request = new CreateTopicsRequest.Builder(topics, creationTimeoutMs).build();
        ByteBuffer response = send(brokerIP, brokerPort, request, ApiKeys.CREATE_TOPICS);
        CreateTopicsResponse rsp = CreateTopicsResponse.parse(response, request.version());
        CreateTopicsResponse.Error error = rsp.errors().get(topicName);
        return error.error() == Errors.NONE;
    }

    /**
     * 删除topic
     * 由于只是样例代码，有些东西就硬编码写到程序里面了(比如主机名和端口)，各位看官自行修改即可
     * @param topicName             topic名
     * @throws IOException
     */
    public static boolean deleteTopic(String brokerIP, int brokerPort, String topicName) throws IOException {
        Set<String> topics = new HashSet<>();
        // 插入多个元素便可同时创建多个topic
        topics.add(topicName);
        int creationTimeoutMs = 60000;
        DeleteTopicsRequest request = new DeleteTopicsRequest.Builder(topics, creationTimeoutMs).build();
        ByteBuffer response = send(brokerIP, brokerPort, request, ApiKeys.DELETE_TOPICS);
        DeleteTopicsResponse rsp = DeleteTopicsResponse.parse(response, request.version());
        Errors error = rsp.errors().get(topicName);
        return error == Errors.NONE;
    }
}
