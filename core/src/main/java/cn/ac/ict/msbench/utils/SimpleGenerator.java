package cn.ac.ict.msbench.utils;

import cn.ac.ict.msbench.generator.Generator;

import java.util.Random;

/**
 * Created by krumo on 3/23/17.
 */
public class SimpleGenerator extends Generator {

    int msgSize = -1;
    byte[] payload = null;

    public SimpleGenerator(int messageSize) {
        msgSize = messageSize;

        Random random = new Random(0);
        payload = new byte[msgSize];
        for (int i = 0; i < payload.length; ++i)
            payload[i] = (byte) (random.nextInt(26) + 65);
    }

    public Object nextValue() {
        return msgSize < 1 ? "default".getBytes() : payload;
    }

    public Object lastValue() {
        return msgSize < 1 ? "default".getBytes() : payload;
    }


}
