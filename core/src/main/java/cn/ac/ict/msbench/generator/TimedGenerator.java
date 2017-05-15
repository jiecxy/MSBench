package cn.ac.ict.msbench.generator;

import cn.ac.ict.msbench.utils.MessageUtil;




/**
 * Generator generating Message @utils.MessageUtil.Message,which contains the timestamp
 * the payloadSize = msgSize - sizeof(long)
 */
public class TimedGenerator extends Generator {

    int msgSize = -1;
    byte[] payload = null;

    public TimedGenerator(int messageSize) {

        msgSize = messageSize - 8;

        try{
            if(msgSize < 0)
                throw new SizeException("the message size is less than 8") ;
        }catch (SizeException se){
            se.printStackTrace();
        }

    }

    public Object nextValue() {
        long requestTime = System.nanoTime();
        try{
            payload =  MessageUtil.generateMessage(requestTime,msgSize);
        }catch (Exception e){
            e.printStackTrace();
        }
        return payload;
    }

    public Object lastValue() {
        return payload;
    }


}
class SizeException extends Exception{
    public SizeException(String message) {
        super(message);
    }
}

