package cn.ac.ict.msbench;

import cn.ac.ict.msbench.exception.UnknownMSException;

import java.lang.reflect.Constructor;
import java.util.Properties;

/**
 * Creates a MESSAGE_SIZE layer by dynamically classloading the specified MESSAGE_SIZE class.
 */
public final class MSFactory {

    private MSFactory() {
        // not used
    }

    public static MS newMS(String msName, boolean isProducer, String streamName, Properties properties, Integer from) throws UnknownMSException {
//        ClassLoader classLoader = MSFactory.class.getClassLoader();
//
//        MS ret;
//
//        try {
//            Class dbclass = classLoader.loadClass(dbname);
//
//            ret = (MS) dbclass.newInstance();
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }

        //return new MSWrapper(ret);
//        return ret;

        if (from == null)
            from = -2;

        MS ms;
        try {
            Class<?> special = Class.forName(msName);
            Constructor<?> constructor = special.getConstructor(String.class, boolean.class, Properties.class, int.class);
            ms = (MS) constructor.newInstance(streamName, isProducer, properties, (int) from);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnknownMSException("Load MS class error!");
        }
        return ms;
    }

}
