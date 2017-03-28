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


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import cn.ac.ict.communication.Communication;
import cn.ac.ict.communication.MasterCom;
import cn.ac.ict.communication.WorkerCom;
import cn.ac.ict.exception.UnknownMSException;
import cn.ac.ict.worker.Worker;
import cn.ac.ict.worker.throughput.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static cn.ac.ict.constants.Constants.*;
import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;


/**
 * Main class for executing MSBench.
 */
public class MSBClient {

    private MS ms = null;
    private ArgumentParser parser = null;
    private String masterIP = "";
    private int masterPort = 0;
    private int runTime = 0;

    public MSBClient(String[] args) {
        parser = argParser();
        initArguments(args);
    }

    private Integer getIntArgOrException(Namespace res, String arg) throws ArgumentParserException {
        Integer o = res.getInt(arg);
        if (o == null)
            throw new ArgumentParserException("Missing argument: " +  arg, parser);
        return o;
    }

    private String getStringArgOrException(Namespace res, String arg) throws ArgumentParserException {
        String o = res.getString(arg);
        if (o == null)
            throw new ArgumentParserException("Missing argument: " +  arg, parser);
        return o;
    }

    /**
     *
     * 1. Start Master
     * Arguments: -tr 1000 -M 1.1.1.1:9999 -P master -w 1 -r 1 -sn 1 -name topic
     *
     * 2. Start Worker
     * 2.1 Start Writer
     *   Arguments:
     *    ThroughputStrategy: NoLimitThroughput
     *      -tr 1000 -M 1.1.1.1:9999 -P writer -W 2.2.2.2 -sys com.apache.kafka.KafkaClient -cf ./kafka.config -sname topic0 -ms 10  -tp -1
     *
     *    ThroughputStrategy: ConstantThroughput
     *      -tr 1000 -M 1.1.1.1:9999 -P writer -W 2.2.2.2 -sys com.apache.kafka.KafkaClient -cf ./kafka.config -sname topic0 -ms 10 -tp 1000
     *
     *    ThroughputStrategy: GradualChangeThroughput
     *      -tr 1000 -M 1.1.1.1:9999 -P writer -W 2.2.2.2 -sys com.apache.kafka.KafkaClient -cf ./kafka.config -sname topic0 -ms 10 -tp 1000 -ftp 2000 -ctp 100 -ctps 5
     *
     *    ThroughputStrategy: GivenRandomChangeThroughputList
     *      -tr 1000 -M 1.1.1.1:9999 -P writer -W 2.2.2.2 -sys com.apache.kafka.KafkaClient -cf ./kafka.config -sname topic0 -ms 10 -rtpl 100,200,300,400 -ctps 5
     *
     *    Note: If you want the write mode to be sync(default is Async), then add -sync
     *
     * 2.2 Start Reader
     *   Arguments:
     *      -tr 1000 -M 1.1.1.1:9999 -P reader -W 2.2.2.2 -sys com.apache.kafka.KafkaClient -cf ./kafka.config -sname topic0 -from 0
     */
    private void initArguments(String[] args) {
        try {
            Namespace res = parser.parseArgs(args);
            // Get the run time
            runTime = res.getInt(RUN_TIME);

            // Get the master address
            String masterAddress = res.getString(MASTER_ADDRESS);
            String[] tmps = masterAddress.split(":");
            if (tmps.length != 2) {
                throw new ArgumentParserException("Invalid Master Address!", parser);
            }
            try {
                masterIP = tmps[0];
                masterPort = Integer.parseInt(tmps[1]);
            } catch (NumberFormatException e) {
                throw new ArgumentParserException("Invalid Master Address!", parser);
            }

            // Get the process: Master, Reader, or Writer
            String process = res.getString(PROCESS);
            if (process.equals(MASTER)) {

                ArrayList<String> streams = getStreamNames(getIntArgOrException(res, STREAM_NUM), getStringArgOrException(res, STREAM_NAME_PREFIX));
                Integer writerNum = res.getInt(WRITER_NUM);
                Integer readerNum = res.getInt(READER_NUM);
                if (writerNum == null && readerNum == null) {
                    throw new ArgumentParserException("Argument -w or -r required!", parser);
                }
                writerNum = writerNum == null ? 0 : writerNum;
                readerNum = readerNum == null ? 0 : readerNum;
                startMaster(masterIP, masterPort, runTime, streams, writerNum, readerNum);
            } else {

                // Get the workerIP
                String workerIP = getStringArgOrException(res, WORKER_ADDRESS);

                String systemClass = "";
                // Get the ms class
                try {
                    systemClass = getStringArgOrException(res, SYSTEM);
                    String configFilePath = getStringArgOrException(res, CONFIG_FILE);
                    InputStream propStream = new FileInputStream(configFilePath);
                    Properties msClientProps = new Properties();
                    msClientProps.load(propStream);
                    ms = MSFactory.newMS(systemClass, msClientProps);
                } catch (Exception e) {
                    throw new ArgumentParserException("Load MS Class Error!", parser);
                }

                String streamName = getStringArgOrException(res, STREAM_NAME);
                if (process.equals(READER)) {
                    ms.setProducer(false);

                    int from = getIntArgOrException(res, READ_FROM);
                    startReader(workerIP, masterIP, masterPort, runTime, streamName, from, ms, systemClass);
                } else if (process.equals(WRITER)) {
                    ms.setProducer(true);

                    int messageSize = getIntArgOrException(res, MESSAGE_SIZE);
                    Boolean isSync = res.getBoolean(SYNC);
                    Integer tp = res.getInt(THROUGHPUT);
                    // Get the speed mode
                    /**
                     * Arguments required By different write mode
                     *   NoLimitThroughput: -tp -1
                     *   ConstantThroughput: -tp 1000
                     *   GradualChangeThroughput: -tp 1000 -ftp 2000 -ctp 100 -ctps 5
                     *   GivenRandomChangeThroughputList: -rtpl 100,200,300,400 -ctps 5
                     */
                    if (tp == null) {  // GivenRandomChangeThroughputList
                        String randomTpListStr = getStringArgOrException(res, RANDOM_THROUGHPUT_LIST);
                        String[] tpsList = randomTpListStr.split(",");
                        int[] list = new int[tpsList.length];
                        try {
                            for (int i = 0; i < tpsList.length; i++) {
                                list[i] = Integer.parseInt(tpsList[i]);
                            }
                        } catch (NumberFormatException e) {
                            throw new ArgumentParserException("GivenRandomChangeThroughputList Mode: Invalid argument rtpl!", parser);
                        }
                        int ctps = getIntArgOrException(res, CHANGE_THROUGHPUT_SECONDS);
                        startWriter(workerIP, masterIP, masterPort, runTime, streamName, ms, systemClass, messageSize, isSync,
                                new GivenRandomChangeThroughputList(list, ctps));
                    } else {
                        Integer ftp = res.getInt(FINAL_THROUGHPUT);
                        Integer ctp = res.getInt(CHANGE_THROUGHPUT);
                        Integer ctps = res.getInt(CHANGE_THROUGHPUT_SECONDS);
                        if (ftp != null || ctp != null || ctps != null) {
                            if (ftp != null && ctp != null && ctps != null) { // GradualChangeThroughput
                                startWriter(workerIP, masterIP, masterPort, runTime, streamName, ms, systemClass, messageSize, isSync,
                                        new GradualChangeThroughput(tp, ftp, ctp, ctps));
                            } else {
                                throw new ArgumentParserException("GradualChangeThroughput Mode: Require tp ftp ctp ctps!", parser);
                            }
                        } else {
                            if (tp == -1) { // NoLimitThroughput
                                startWriter(workerIP, masterIP, masterPort, runTime, streamName, ms, systemClass, messageSize, isSync,
                                        new NoLimitThroughput());
                            } else { // ConstantThroughput
                                startWriter(workerIP, masterIP, masterPort, runTime, streamName, ms, systemClass, messageSize, isSync,
                                        new ConstantThroughput(tp));
                            }
                        }
                    }
                } else {
                    throw new ArgumentParserException("Invalid process(P)!", parser);
                }
            }
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

    private void startMaster(String masterIP, int masterPort, int runTime, ArrayList<String> streams, int writerNum, int readerNum) {

        System.out.println("startMaster:" + "\n"
                + "\t" + "masterIP" + " = " + masterIP + "\n"
                + "\t" + "masterPort" + " = " + masterPort + "\n"
                + "\t" + "runTime" + " = " + runTime + "\n"
                + "\t" + "streams" + " = " + streams + "\n"
                + "\t" + "writerNum" + " = " + writerNum + "\n"
                + "\t" + "readerNum" + " = " + readerNum);
        //System.exit(1);

        Properties props = new Properties();
        props.setProperty("akka.remote.netty.tcp.hostname", masterIP);
        props.setProperty("akka.remote.netty.tcp.port", masterPort + "");
        props.setProperty("akka.actor.provider", "remote");
        props.setProperty("akka.actor.serializers.proto", "akka.remote.serialization.ProtobufSerializer");
        props.setProperty("akka.actor.warn-about-java-serializer-usage", "off");
        props.setProperty("akka.remote.log-remote-lifecycle-events", "off");
        Config akkaConf = ConfigFactory.parseProperties(props);

        ActorSystem system = ActorSystem.create("MSBenchMaster", akkaConf);
        System.out.println("MasterCom start " + masterIP + ":" + masterPort);
        ActorRef master = system.actorOf(Props.create(MasterCom.class, masterIP, masterPort, runTime, streams, writerNum, readerNum), "master");

//        System.out.println("MasterCom tell ");
//        master.tell("MasterCom MESSAGES", master);
        system.awaitTermination();
    }

    private void startReader(String workerIP, String masterIP, int masterPort, int runTime, String stream, int from, MS ms, String systemName) {
        System.out.println("startReader:" + "\n"
                + "\t" + "workerIP" + " = " + workerIP + "\n"
                + "\t" + "masterIP" + " = " + masterIP + "\n"
                + "\t" + "masterPort" + " = " + masterPort + "\n"
                + "\t" + "runTime" + " = " + runTime + "\n"
                + "\t" + "stream" + " = " + stream + "\n"
                + "\t" + "from" + " = " + from + "\n"
                + "\t" + "ms" + " = " + ms);
        //System.exit(1);

        int workerPort = 0;
        Properties props = new Properties();
        props.setProperty("akka.actor.provider", "remote");
        props.setProperty("akka.remote.netty.tcp.hostname", workerIP);
        props.setProperty("akka.remote.netty.tcp.port", workerPort + "");
        props.setProperty("akka.actor.warn-about-java-serializer-usage", "off");
        Config akkaConf = ConfigFactory.parseProperties(props);
        ActorSystem system = ActorSystem.create("MSBenchWorker", akkaConf);

        System.out.println("WorkerCom start " + workerIP + ":" + workerPort);
        ActorRef worker = system.actorOf(Props.create(WorkerCom.class, workerIP, masterIP, masterPort, runTime, stream, from, ms, systemName), "worker");
//        worker.tell("WorkerCom MESSAGES", worker);
    }

    private void startWriter(String workerIP, String masterIP, int masterPort, int runTime, String stream, MS ms, String systemName, int messageSize, boolean isSync, ThroughputStrategy strategy) {
        System.out.println("startWriter:" + "\n"
                + "\t" + "workerIP" + " = " + workerIP + "\n"
                + "\t" + "masterIP" + " = " + masterIP + "\n"
                + "\t" + "masterPort" + " = " + masterPort + "\n"
                + "\t" + "runTime" + " = " + runTime + "\n"
                + "\t" + "stream" + " = " + stream + "\n"
                + "\t" + "ms" + " = " + ms + "\n"
                + "\t" + "messageSize" + " = " + messageSize + "\n"
                + "\t" + "isSync" + " = " + isSync + "\n"
                + "\t" + "strategy" + " = " + strategy);
        //System.exit(1);

        int workerPort = 0;
        Properties props = new Properties();
        props.setProperty("akka.actor.provider", "remote");
        props.setProperty("akka.remote.netty.tcp.hostname", workerIP);
        props.setProperty("akka.remote.netty.tcp.port", workerPort + "");
        props.setProperty("akka.actor.warn-about-java-serializer-usage", "off");
        Config akkaConf = ConfigFactory.parseProperties(props);
        ActorSystem system = ActorSystem.create("MSBenchWorker", akkaConf);

        System.out.println("WorkerCom start " + workerIP + ":" + workerPort);
        ActorRef worker = system.actorOf(Props.create(WorkerCom.class, workerIP, masterIP, masterPort, runTime, stream, ms, systemName, messageSize, isSync, strategy), "worker");
//        worker.tell("WorkerCom MESSAGES", worker);
    }

    private ArrayList<String> getStreamNames(int streamNum, String StreamPrefix) {
        ArrayList<String> streams = new ArrayList<String>();
        for (int i = 0; i < streamNum; i++) {
            streams.add(StreamPrefix + i);
        }
        return streams;
    }

    public static void main(String[] args) {
        MSBClient client = new MSBClient(args);
    }


    /** Get the command-line argument parser. */
    //TODO 把参数改成静态变量
    private ArgumentParser argParser() {
        parser = ArgumentParsers
                .newArgumentParser("MSBench")
                .defaultHelp(true)
                .description(SYSTEM_DESCRPTION);

        // For global variables
        parser.addArgument(CONFIG_PRE + RUN_TIME).action(store()).required(true).type(Integer.class).metavar(RUN_TIME.toUpperCase()).help(RUN_TIME_DOC);
        //parser.addArgument(CONFIG_PRE + HOSTS).action(store()).required(true).type(String.class).metavar(HOSTS).help(HOSTS_DOC);
        parser.addArgument(CONFIG_PRE + MASTER_ADDRESS).action(store()).required(true).type(String.class).metavar(MASTER_ADDRESS.toUpperCase()).help(MASTER_ADDRESS_DOC);
        ArrayList<String> processes = new ArrayList<String>();
        processes.add(MASTER);
        processes.add(READER);
        processes.add(WRITER);
        parser.addArgument(CONFIG_PRE + PROCESS).action(store()).required(true).type(String.class).metavar(PROCESS.toUpperCase()).help(PROCESS_DOC).choices(processes);

        // For Stream
        parser.addArgument(CONFIG_PRE + STREAM_NUM).action(store()).required(false).type(Integer.class).metavar(STREAM_NUM.toUpperCase()).help(STREAM_NUM_DOC);
        parser.addArgument(CONFIG_PRE + STREAM_NAME_PREFIX).action(store()).required(false).type(String.class).metavar(STREAM_NAME_PREFIX.toUpperCase()).help(STREAM_NAME_PREFIX_DOC);
        parser.addArgument(CONFIG_PRE + STREAM_NAME).action(store()).required(false).type(String.class).metavar(STREAM_NAME.toUpperCase()).help(STREAM_NAME_DOC);

        // For master process
        //parser.addArgument(CONFIG_PRE + WRITER_LIST).action(store()).required(false).type(String.class).metavar(WRITER_LIST).dest(WRITER_LIST).help(WRITER_LIST_DOC);
        //parser.addArgument(CONFIG_PRE + READER_LIST).action(store()).required(false).type(String.class).metavar(READER_LIST).dest(READER_LIST).help(READER_LIST_DOC);

        // For worker process
        parser.addArgument(CONFIG_PRE + WORKER_ADDRESS).action(store()).required(false).type(String.class).metavar(WORKER_ADDRESS.toUpperCase()).help(WORKER_ADDRESS_DOC);
        parser.addArgument(CONFIG_PRE + SYSTEM).action(store()).required(false).type(String.class).metavar(SYSTEM.toUpperCase()).help(SYSTEM_DOC);
        parser.addArgument(CONFIG_PRE + CONFIG_FILE).action(store()).required(false).type(String.class).metavar(CONFIG_FILE.toUpperCase()).help(CONFIG_FILE_DOC);
        //    For Writer
        parser.addArgument(CONFIG_PRE + WRITER_NUM).action(store()).required(false).type(Integer.class).metavar(WRITER_NUM.toUpperCase()).help(WRITER_NUM_DOC);
        parser.addArgument(CONFIG_PRE + SYNC).action(storeTrue()).required(false).type(Boolean.class).metavar(SYNC.toUpperCase()).help(SYNC_DOC);
        parser.addArgument(CONFIG_PRE + MESSAGE_SIZE).action(store()).required(false).type(Integer.class).metavar(MESSAGE_SIZE.toUpperCase()).help(MESSAGE_SIZE_DOC);
        //        For Writer Throughput
        parser.addArgument(CONFIG_PRE + THROUGHPUT).action(store()).required(false).type(Integer.class).metavar(THROUGHPUT.toUpperCase()).help(THROUGHPUT_DOC);
        parser.addArgument(CONFIG_PRE + FINAL_THROUGHPUT).action(store()).required(false).type(Integer.class).metavar(FINAL_THROUGHPUT.toUpperCase()).help(FINAL_THROUGHPUT_DOC);
        parser.addArgument(CONFIG_PRE + CHANGE_THROUGHPUT).action(store()).required(false).type(Integer.class).metavar(CHANGE_THROUGHPUT.toUpperCase()).help(CHANGE_THROUGHPUT_DOC);
        parser.addArgument(CONFIG_PRE + CHANGE_THROUGHPUT_SECONDS).action(store()).required(false).type(Integer.class).metavar(CHANGE_THROUGHPUT_SECONDS.toUpperCase()).help(CHANGE_THROUGHPUT_SECONDS_DOC);
        parser.addArgument(CONFIG_PRE + RANDOM_THROUGHPUT_LIST).action(store()).required(false).type(String.class).metavar(RANDOM_THROUGHPUT_LIST.toUpperCase()).help(RANDOM_THROUGHPUT_LIST_DOC);
        //    For Reader
        parser.addArgument(CONFIG_PRE + READER_NUM).action(store()).required(false).type(Integer.class).metavar(READER_NUM.toUpperCase()).help(READER_NUM_DOC);
        parser.addArgument(CONFIG_PRE + READ_FROM).action(store()).required(false).type(Integer.class).metavar(READ_FROM.toUpperCase()).help(READ_FROM_DOC);

        return parser;
    }
}