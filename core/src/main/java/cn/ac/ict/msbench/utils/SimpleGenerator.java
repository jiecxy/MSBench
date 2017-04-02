package cn.ac.ict.msbench.utils;

import cn.ac.ict.msbench.generator.Generator;

/**
 * Created by krumo on 3/23/17.
 */
public class SimpleGenerator extends Generator {
    int MsgSize = -1;

    public SimpleGenerator(int messageSize)
    {
        MsgSize=messageSize;
    }

    public Object nextValue() {
        return MsgSize < 1 ? "my message".getBytes() : new byte[MsgSize];
    }

    public Object lastValue() {
        return MsgSize < 1 ? "my message".getBytes() : new byte[MsgSize];
    }
}
