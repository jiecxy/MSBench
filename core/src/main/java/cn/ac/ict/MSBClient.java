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


import cn.ac.ict.communication.Communication;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.util.*;

import static cn.ac.ict.constants.Constants.*;
import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;


/**
 * Main class for executing MSBench.
 */
public class MSBClient {

    private String[] args = null;
    private ArgumentParser parser = null;
    private Communication com = null;
    private Worker worker = null;
    private MS ms = null;

    // For global variables
    private String systemClass = null;

    public MSBClient(String[] args) {
        this.args = args;
        parser = argParser();
    }

    private void initArguments() {
        try {
            Namespace res = parser.parseArgs(args);

            systemClass = res.getString(SYSTEM);
            // TODO 加载用户指定的MS类

            Boolean isMaster = res.getBoolean(MASTER);


            // Check is it master
            //TODO 判断是否为 master
            // 如果为master，校验需要master的参数
            // 如果为Writer 或 reader，校验相关参数


        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } else {
                parser.handleError(e);
                System.exit(1);
            }
        }
    }

    public static boolean checkRequiredProperties(Properties props) {
      return false;
    }

    public static void main(String[] args) {

        MSBClient client = new MSBClient(args);
    }


    //TODO 后期需要一个自定义场景类

    /** Get the command-line argument parser. */
    //TODO 把参数改成静态变量
    private ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("MSBench")
            .defaultHelp(true)
            .description(SYSTEM_DESCRPTION);

        // For global variables
        parser.addArgument(CONFIG_PRE + SYSTEM).action(store()).required(true).type(String.class).metavar(SYSTEM).dest(SYSTEM).help(SYSTEM_DOC);
        parser.addArgument(CONFIG_PRE + RUN_TIME).action(store()).required(true).type(Integer.class).metavar(RUN_TIME).help(RUN_TIME_DOC);
        parser.addArgument(CONFIG_PRE + HOSTS).action(store()).required(true).type(String.class).metavar(HOSTS).help(HOSTS_DOC);
        parser.addArgument(CONFIG_PRE + CONFIG_FILE).action(store()).required(true).type(String.class).metavar(CONFIG_FILE).dest(CONFIG_FILE).help(CONFIG_FILE_DOC);

        // For Stream
        parser.addArgument(CONFIG_PRE + STREAM_NUM).action(store()).required(true).type(Integer.class).metavar(STREAM_NUM).dest(STREAM_NUM).help(STREAM_NUM_DOC);
        parser.addArgument(CONFIG_PRE + STREAM_NAME_PREFIX).action(store()).required(true).type(String.class).metavar(STREAM_NAME_PREFIX).dest(STREAM_NAME_PREFIX).help(STREAM_NAME_PREFIX_DOC);

        // For master process
        parser.addArgument(CONFIG_PRE + MASTER).action(storeTrue()).required(false).metavar(MASTER).dest(MASTER).help(MASTER_DOC);
        parser.addArgument(CONFIG_PRE + WRITER_LIST).action(store()).required(false).type(String.class).metavar(WRITER_LIST).dest(WRITER_LIST).help(WRITER_LIST_DOC);
        parser.addArgument(CONFIG_PRE + READER_LIST).action(store()).required(false).type(String.class).metavar(READER_LIST).dest(READER_LIST).help(READER_LIST_DOC);

        // For worker process
        //    For Writer
        parser.addArgument(CONFIG_PRE + WRITER).action(storeTrue()).required(false).metavar(WRITER).dest(WRITER).help(WRITER_DOC);
        parser.addArgument(CONFIG_PRE + WRITER_NUM).action(store()).required(false).type(Integer.class).metavar(WRITER_NUM).dest(WRITER_NUM).help(WRITER_NUM_DOC);
        parser.addArgument(CONFIG_PRE + SYNC).action(store()).required(false).type(Integer.class).metavar(SYNC).dest(SYNC).help(SYNC_DOC);
        parser.addArgument(CONFIG_PRE + MESSAGE_SIZE).action(store()).required(true).type(Integer.class).metavar(MESSAGE_SIZE).help(MESSAGE_SIZE_DOC);
        //        For Writer Throughput
        parser.addArgument(CONFIG_PRE + THROUGHPUT).action(store()).required(false).type(Integer.class).metavar(THROUGHPUT).dest(THROUGHPUT).help(THROUGHPUT_DOC);
        parser.addArgument(CONFIG_PRE + FINAL_THROUGHPUT).action(store()).required(false).type(Integer.class).metavar(FINAL_THROUGHPUT).dest(FINAL_THROUGHPUT).help(FINAL_THROUGHPUT_DOC);
        parser.addArgument(CONFIG_PRE + CHANGE_THROUGHPUT).action(store()).required(false).type(Integer.class).metavar(CHANGE_THROUGHPUT).dest(CHANGE_THROUGHPUT).help(CHANGE_THROUGHPUT_DOC);
        parser.addArgument(CONFIG_PRE + CHANGE_THROUGHPUT_SECONDS).action(store()).required(false).type(Integer.class).metavar(CHANGE_THROUGHPUT_SECONDS).dest(CHANGE_THROUGHPUT_SECONDS).help(CHANGE_THROUGHPUT_SECONDS_DOC);
        parser.addArgument(CONFIG_PRE + RANDOM_THROUGHPUT_LIST).action(store()).required(false).type(String.class).metavar(RANDOM_THROUGHPUT_LIST).dest(RANDOM_THROUGHPUT_LIST).help(RANDOM_THROUGHPUT_LIST_DOC);
        //    For Reader
        parser.addArgument(CONFIG_PRE + READER).action(storeTrue()).required(false).metavar(READER).dest(READER).help(READER_DOC);
        parser.addArgument(CONFIG_PRE + READER_NUM).action(store()).required(false).type(Integer.class).metavar(READER_NUM).dest(READER_NUM).help(READER_NUM_DOC);
        parser.addArgument(CONFIG_PRE + READ_FROM).action(store()).required(false).type(Integer.class).metavar(READ_FROM).dest(READ_FROM).help(READ_FROM_DOC);

        return parser;
    }
}
