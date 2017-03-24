package cn.ac.ict.utils;

import cn.ac.ict.generator.Generator;
import javafx.beans.binding.ObjectExpression;

/**
 * Created by krumo on 3/23/17.
 */
public class SimpleGenerator extends Generator{
    int MsgSize=-1;
    public SimpleGenerator(int messageSize)
    {
        MsgSize=messageSize;
    }
    public Object nextValue() {
        return MsgSize<1?"my message".getBytes():new byte[MsgSize];
    }

    public Object lastValue() {
        return MsgSize<1?"my message".getBytes():new byte[MsgSize];
    }
}
