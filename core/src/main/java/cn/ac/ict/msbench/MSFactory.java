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

    public static MS newMS(String msname, Boolean isProducer, String streamName, Properties properties, Integer from) throws UnknownMSException {
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
        Class<?> special = null;
        try {
            special = Class.forName(msname);
            Constructor<?> specialConstructor = special.getConstructor(String.class, boolean.class, Properties.class, int.class);
            ms = (MS) specialConstructor.newInstance(streamName, isProducer, properties, from);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnknownMSException("Load MS class error!");
        }
        return ms;
    }

}
