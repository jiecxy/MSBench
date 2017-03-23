package cn.ac.ict.utils;

import cn.ac.ict.generator.Generator;
import javafx.beans.binding.ObjectExpression;

/**
 * Created by krumo on 3/23/17.
 */
public class SimpleGenerator extends Generator{

    public Object nextValue() {
        return new String("my-message");
    }

    public Object lastValue() {
        return new String("my-message");
    }
}
