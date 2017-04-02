/**
 * Copyright (c) 2010-2016 Yahoo! Inc., 2017 YCSB contributors All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package cn.ac.ict;

import cn.ac.ict.exception.UnknownMSException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

/**
 * Creates a MESSAGE_SIZE layer by dynamically classloading the specified MESSAGE_SIZE class.
 */
public final class MSFactory {

    private MSFactory() {
        // not used
    }

    public static MS newMS(String msname, Boolean isProducer, String streamName, Properties properties) throws UnknownMSException {
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


        MS ms;
        Class<?> special = null;
        try {
            special = Class.forName(msname);
            Constructor<?> specialConstructor = special.getConstructor(String.class, Boolean.class, Properties.class);
            ms = (MS) specialConstructor.newInstance(streamName, isProducer, properties);
        } catch (Exception e) {
            throw new UnknownMSException("Load MS class error!");
        }
        return ms;
    }

}
