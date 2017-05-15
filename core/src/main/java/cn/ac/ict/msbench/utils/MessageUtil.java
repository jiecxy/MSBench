package cn.ac.ict.msbench.utils;
import java.nio.ByteBuffer;
import java.util.Random;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


/**
 * Util for generating and parsing time contained messages, for e-2-e uniform caculation
 */
public class MessageUtil {

    static final Random RAND = new Random(System.currentTimeMillis());
    static final SerializeUtil MSG_SERIALIZER =
            new SerializeUtil();

    public static byte[] generateMessage(long requestMillis, int payLoadSize) throws Exception {
        byte[] payload = new byte[payLoadSize];
        RAND.nextBytes(payload);
        Message msg = new Message(requestMillis, payload);
        //System.out.println("Message's time" + msg.getPublishTime() );
        return MSG_SERIALIZER.serialize(msg);
    }

    public static Message parseMessage(byte[] data) throws Exception {
        Message msg ;

        msg = (Message)MSG_SERIALIZER.unserialize(data);
        //System.out.println("after unserialization Message's time" + msg.getPublishTime() );

        return msg;
    }
    public static void main(String[] args){
        long now = System.nanoTime();
        try{
            byte[] data = generateMessage(now,10);
            Message msg = parseMessage(data);

        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
class Message implements java.io.Serializable {



    public long publishTime;
    public byte[] payLoad;


    public Message(
            ){

    }
    public Message(
            long publishTime,
            byte[] payLoad)
    {
        this.publishTime = publishTime;
        this.payLoad = payLoad;
    }
    public long getPublishTime() {
        return this.publishTime;
    }

    public Message setPublishTime(long publishTime) {
        this.publishTime = publishTime;

        return this;
    }



    public byte[] getPayLoad() {
        return payLoad;
    }

    public ByteBuffer BufferForPayLoad() {
        return ByteBuffer.wrap(payLoad);
    }



}



class SerializeUtil {
    /**
     * serialization
     *
     * @param object
     * @return
     */
    public static byte[] serialize(Object object) {
        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            byte[] bytes = baos.toByteArray();
            return bytes;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * unserialization
     *
     * @param bytes
     * @return
     */
    public static Object unserialize(byte[] bytes) {
        ByteArrayInputStream bais = null;
        try {
            bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
