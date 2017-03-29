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


    private static ByteBuffer send(String host, int port, AbstractRequest request, ApiKeys apiKey) throws IOException {
        Socket socket = connect(host, port);
        try {
            return send(request, apiKey, socket);
        } finally {
            socket.close();
        }
    }


    private static byte[] issueRequestAndWaitForResponse(Socket socket, byte[] request) throws IOException {
        sendRequest(socket, request);
        return getResponse(socket);
    }


    private static void sendRequest(Socket socket, byte[] request) throws IOException {
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(request.length);
        dos.write(request);
        dos.flush();
    }


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


    private static Socket connect(String hostName, int port) throws IOException {
        return new Socket(hostName, port);
    }


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


    public static boolean createTopic(String brokerIP, int brokerPort, String topicName, int partitions, short replicationFactor) throws IOException {
        Map<String, CreateTopicsRequest.TopicDetails> topics = new HashMap<>();
        topics.put(topicName, new CreateTopicsRequest.TopicDetails(partitions, replicationFactor));
        int creationTimeoutMs = 60000;
        CreateTopicsRequest request = new CreateTopicsRequest.Builder(topics, creationTimeoutMs).build();
        ByteBuffer response = send(brokerIP, brokerPort, request, ApiKeys.CREATE_TOPICS);
        CreateTopicsResponse rsp = CreateTopicsResponse.parse(response, request.version());
        CreateTopicsResponse.Error error = rsp.errors().get(topicName);
        return error.error() == Errors.NONE;
    }


    public static boolean deleteTopic(String brokerIP, int brokerPort, String topicName) throws IOException {
        Set<String> topics = new HashSet<>();
        topics.add(topicName);
        int creationTimeoutMs = 60000;
        DeleteTopicsRequest request = new DeleteTopicsRequest.Builder(topics, creationTimeoutMs).build();
        ByteBuffer response = send(brokerIP, brokerPort, request, ApiKeys.DELETE_TOPICS);
        DeleteTopicsResponse rsp = DeleteTopicsResponse.parse(response, request.version());
        Errors error = rsp.errors().get(topicName);
        return error == Errors.NONE;
    }
}
