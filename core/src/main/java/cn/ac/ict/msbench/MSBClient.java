package cn.ac.ict.msbench;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import cn.ac.ict.msbench.communication.MasterCom;
import cn.ac.ict.msbench.communication.WorkerCom;
import cn.ac.ict.msbench.exporter.Exporter;
import cn.ac.ict.msbench.exporter.FileExporter;
import cn.ac.ict.msbench.worker.job.ReadJob;
import cn.ac.ict.msbench.worker.job.WriteJob;
import cn.ac.ict.msbench.worker.throughput.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;

import static cn.ac.ict.msbench.constants.Constants.*;
import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;


/**
 * Main class for executing MSBench.
 */
public class MSBClient {

    private static final Logger log = LoggerFactory.getLogger(MSBClient.class);

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
     * Arguments: -home /home/ms/msbench -tr 1000 -M 127.0.0.1:9999 -P master -w 1 -r 1 -sn 1 -name topic
     *
     * 2. Start Worker
     * 2.1 Start Writer
     *   Arguments:
     *    ThroughputStrategy: NoLimitThroughput
     *      -home /Users/jiecxy/Desktop/tmp/msbench -tr 1000 -M 127.0.0.1:9999 -P writer -W 127.0.0.1 -sys cn.ac.ict.msbench.utils.SimpleMS -cf ./kafka.config -sname topic0 -ms 10  -tp -1
     *
     *    ThroughputStrategy: ConstantThroughput
     *      -home /home/ms/msbench -tr 1000 -M 127.0.0.1:9999 -P writer -W 127.0.0.1 -sys cn.ac.ict.msbench.utils.SimpleMS -cf ./kafka.config -sname topic0 -ms 10 -tp 1000
     *
     *    ThroughputStrategy: GradualChangeThroughput
     *      -home /home/ms/msbench -tr 1000 -M 127.0.0.1:9999 -P writer -W 127.0.0.1 -sys cn.ac.ict.msbench.utils.SimpleMS -cf ./kafka.config -sname topic0 -ms 10 -tp 1000 -ftp 2000 -ctp 100 -ctps 5
     *
     *    ThroughputStrategy: GivenRandomChangeThroughputList
     *      -home /home/ms/msbench -tr 1000 -M 127.0.0.1:9999 -P writer -W 127.0.0.1 -sys cn.ac.ict.msbench.utils.SimpleMS -cf ./kafka.config -sname topic0 -ms 10 -rtpl 100,200,300,400 -ctps 5
     *
     *    Note: If you want the write mode to be sync(default is Async), then add -sync
     *
     * 2.2 Start Reader
     *   Arguments:
     *      -tr 1000 -M 127.0.0.1:9999 -P reader -W 127.0.0.1 -sys cn.ac.ict.msbench.utils.SimpleMS -cf ./kafka.config -sname topic0 -from 0
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


            // Get the Home Path
            String homePath = getStringArgOrException(res, HOME_PATH);
            File dataDir = new File(homePath, DATA_DIR_NAME);
            if (!dataDir.exists()) {
                dataDir.mkdirs();
            }

//            // TODO set the exporter
//            exporter = new FileExporter(new FileOutputStream());

            // Get the process: Master, Reader, or Writer
            String process = res.getString(PROCESS);
            if (process.equals(MASTER)) {
                File masterDir = new File(dataDir, MASTER_DIR_NAME);
                if (!masterDir.exists()) {
                    masterDir.mkdirs();
                }

                ArrayList<String> streams = getStreamNames(getIntArgOrException(res, STREAM_NUM), getStringArgOrException(res, STREAM_NAME_PREFIX));
                Integer writerNum = res.getInt(WRITER_NUM);
                Integer readerNum = res.getInt(READER_NUM);
                if (writerNum == null && readerNum == null) {
                    throw new ArgumentParserException("Argument -w or -r is required!", parser);
                }
                writerNum = writerNum == null ? 0 : writerNum;
                readerNum = readerNum == null ? 0 : readerNum;
                startMaster(new FileExporter(masterDir, true), masterIP, masterPort, runTime, streams, writerNum, readerNum);

            } else {
                File workerDir = new File(dataDir, WORKER_DIR_NAME);
                if (!workerDir.exists()) {
                    workerDir.mkdirs();
                }

                // Get the workerIP
                String workerIP = getStringArgOrException(res, WORKER_ADDRESS);

                Boolean isProducer = null;
                if (process.equals(READER)) {
                    isProducer = false;
                } else if (process.equals(WRITER)) {
                    isProducer = true;
                } else {
                    throw new ArgumentParserException("Invalid process(P) Argument!", parser);
                }

                String systemClass = getStringArgOrException(res, SYSTEM);
                String streamName = getStringArgOrException(res, STREAM_NAME);
                Integer delaySec = res.getInt(WORKER_DELAY);
                delaySec = delaySec == null ? 0 : delaySec;

                // Get the ms class
                try {
                    //TODO 将properties改为可选
                    String configFilePath = res.getString(CONFIG_FILE);
                    Properties msClientProps = null;
                    if (configFilePath != null) {
                        InputStream propStream = new FileInputStream(configFilePath);
                        msClientProps = new Properties();
                        msClientProps.load(propStream);
                    }
                    ms = MSFactory.newMS(systemClass, isProducer, streamName, msClientProps, res.getInt(READ_FROM));
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new ArgumentParserException("Load MS Class Error!", parser);
                }

                if (!isProducer) {

                    int from = getIntArgOrException(res, READ_FROM);
                    startReader(new FileExporter(workerDir, false), workerIP, masterIP, masterPort, runTime, streamName, from, ms, systemClass, delaySec);
                } else {

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
                        startWriter(new FileExporter(workerDir, false), workerIP, masterIP, masterPort, runTime, streamName, ms, systemClass, messageSize, isSync,
                                new GivenRandomChangeThroughputList(list, ctps), delaySec);
                    } else {
                        Integer ftp = res.getInt(FINAL_THROUGHPUT);
                        Integer ctp = res.getInt(CHANGE_THROUGHPUT);
                        Integer ctps = res.getInt(CHANGE_THROUGHPUT_SECONDS);
                        if (ftp != null || ctp != null || ctps != null) {
                            if (ftp != null && ctp != null && ctps != null) { // GradualChangeThroughput
                                startWriter(new FileExporter(workerDir, false), workerIP, masterIP, masterPort, runTime, streamName, ms, systemClass, messageSize, isSync,
                                        new GradualChangeThroughput(tp, ftp, ctp, ctps), delaySec);
                            } else {
                                throw new ArgumentParserException("GradualChangeThroughput Mode: Require tp ftp ctp ctps!", parser);
                            }
                        } else {
                            if (tp == -1) { // NoLimitThroughput
                                startWriter(new FileExporter(workerDir, false), workerIP, masterIP, masterPort, runTime, streamName, ms, systemClass, messageSize, isSync,
                                        new NoLimitThroughput(), delaySec);
                            } else { // ConstantThroughput
                                startWriter(new FileExporter(workerDir, false), workerIP, masterIP, masterPort, runTime, streamName, ms, systemClass, messageSize, isSync,
                                        new ConstantThroughput(tp), delaySec);
                            }
                        }
                    }
                }
            }
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
//                parser.printHelp();
                System.exit(0);
            } else {
//                parser.handleError(e);
                log.error(e.getMessage());
                log.debug(e.toString());
                System.exit(1);
            }
        }
    }

    private void startMaster(Exporter exporter, String masterIP, int masterPort, int runTime, ArrayList<String> streams, int writerNum, int readerNum) {

        String conf = "akka.actor.provider = remote" + "\n"
                + "akka.loglevel = ERROR" + "\n"
                + "akka.log-dead-letters = off" + "\n"
                + "akka.log-dead-letters-during-shutdown = off" + "\n"
                + "akka.remote.log-remote-lifecycle-events = off" + "\n"
                + "akka.remote.netty.tcp.hostname = " + masterIP + "\n"
                + "akka.remote.netty.tcp.port = " + masterPort + "\n"
                + "akka.actor.warn-about-java-serializer-usage = off" + "\n";
        Config akkaConf = ConfigFactory.parseString(conf);

        log.info("Start Master:" + "\n"
                + "\t" + "masterIP" + " = " + masterIP + "\n"
                + "\t" + "masterPort" + " = " + masterPort + "\n"
                + "\t" + "runTimeInSec" + " = " + runTime + "\n"
                + "\t" + "streams" + " = " + streams + "\n"
                + "\t" + "writerNum" + " = " + writerNum + "\n"
                + "\t" + "readerNum" + " = " + readerNum + "\n"
                + "\t" + "with Akka Conf: " + akkaConf.toString());

//        props.setProperty("akka.actor.serializers.proto", "akka.remote.serialization.ProtobufSerializer");

        ActorSystem system = ActorSystem.create("MSBenchMaster", akkaConf);
        system.actorOf(Props.create(MasterCom.class, exporter, masterIP, masterPort, runTime, streams, writerNum, readerNum), "master");
        system.awaitTermination();
    }

    private void startReader(Exporter exporter, String workerIP, String masterIP, int masterPort, int runTime, String stream, int from, MS ms, String systemName, long delayStartSec) {

        int workerPort = 0;
        String conf = "akka.actor.provider = remote" + "\n"
                + "akka.loglevel = ERROR" + "\n"
                + "akka.log-dead-letters = off" + "\n"
                + "akka.log-dead-letters-during-shutdown = off" + "\n"
                + "akka.remote.log-remote-lifecycle-events = off" + "\n"
                + "akka.remote.netty.tcp.hostname = " + workerIP + "\n"
                + "akka.remote.netty.tcp.port = " + workerPort + "\n"
                + "akka.actor.warn-about-java-serializer-usage = off" + "\n";
        Config akkaConf = ConfigFactory.parseString(conf);

        log.info("Start Reader:" + "\n"
                + "\t" + "workerIP" + " = " + workerIP + "\n"
                + "\t" + "masterIP" + " = " + masterIP + "\n"
                + "\t" + "masterPort" + " = " + masterPort + "\n"
                + "\t" + "runTimeInSec" + " = " + runTime + "\n"
                + "\t" + "stream" + " = " + stream + "\n"
                + "\t" + "from" + " = " + from + "\n"
                + "\t" + "ms" + " = " + ms + "\n"
                + "\t" + "with Akka Conf: " + akkaConf.toString());

        ActorSystem system = ActorSystem.create("MSBenchWorker", akkaConf);
        system.actorOf(Props.create(WorkerCom.class, exporter, masterIP, masterPort, ms, new ReadJob(systemName, workerIP, runTime, stream, from, delayStartSec)), "worker");
    }

    private void startWriter(Exporter exporter, String workerIP, String masterIP, int masterPort, int runTime, String stream, MS ms, String systemName, int messageSize, boolean isSync, ThroughputStrategy strategy, long delayStartSec) {

        int workerPort = 0;
        String conf = "akka.actor.provider = remote" + "\n"
                + "akka.loglevel = ERROR" + "\n"
                + "akka.log-dead-letters = off" + "\n"
                + "akka.log-dead-letters-during-shutdown = off" + "\n"
                + "akka.remote.log-remote-lifecycle-events = off" + "\n"
                + "akka.remote.netty.tcp.hostname = " + workerIP + "\n"
                + "akka.remote.netty.tcp.port = " + workerPort + "\n"
                + "akka.actor.warn-about-java-serializer-usage = off" + "\n";
        Config akkaConf = ConfigFactory.parseString(conf);

        log.info("StartWriter:" + "\n"
                + "\t" + "workerIP" + " = " + workerIP + "\n"
                + "\t" + "masterIP" + " = " + masterIP + "\n"
                + "\t" + "masterPort" + " = " + masterPort + "\n"
                + "\t" + "runTimeInSec" + " = " + runTime + "\n"
                + "\t" + "stream" + " = " + stream + "\n"
                + "\t" + "ms" + " = " + ms + "\n"
                + "\t" + "messageSize" + " = " + messageSize + "\n"
                + "\t" + "isSync" + " = " + isSync + "\n"
                + "\t" + "strategy" + " = " + strategy + "\n"
                + "\t" + "with Akka Conf: " + akkaConf.toString());

        ActorSystem system = ActorSystem.create("MSBenchWorker", akkaConf);
        system.actorOf(Props.create(WorkerCom.class, exporter, masterIP, masterPort, ms, new WriteJob(systemName, workerIP, runTime, stream, messageSize, isSync, strategy, delayStartSec)), "worker");
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
        parser.addArgument(CONFIG_PRE + HOME_PATH).action(store()).required(true).type(String.class).metavar(HOME_PATH.toUpperCase()).help(HOME_PATH_DOC);

        // For Stream
        parser.addArgument(CONFIG_PRE + STREAM_NUM).action(store()).required(false).type(Integer.class).metavar(STREAM_NUM.toUpperCase()).help(STREAM_NUM_DOC);
        parser.addArgument(CONFIG_PRE + STREAM_NAME_PREFIX).action(store()).required(false).type(String.class).metavar(STREAM_NAME_PREFIX.toUpperCase()).help(STREAM_NAME_PREFIX_DOC);
        parser.addArgument(CONFIG_PRE + STREAM_NAME).action(store()).required(false).type(String.class).metavar(STREAM_NAME.toUpperCase()).help(STREAM_NAME_DOC);

        // For master process
        //parser.addArgument(CONFIG_PRE + WRITER_LIST).action(store()).required(false).type(String.class).metavar(WRITER_LIST).dest(WRITER_LIST).help(WRITER_LIST_DOC);
        //parser.addArgument(CONFIG_PRE + READER_LIST).action(store()).required(false).type(String.class).metavar(READER_LIST).dest(READER_LIST).help(READER_LIST_DOC);

        // For worker process
        parser.addArgument(CONFIG_PRE + WORKER_ADDRESS).action(store()).required(false).type(String.class).metavar(WORKER_ADDRESS.toUpperCase()).help(WORKER_ADDRESS_DOC);
        parser.addArgument(CONFIG_PRE + WORKER_DELAY).action(store()).required(false).type(Integer.class).metavar(WORKER_DELAY.toUpperCase()).help(WORKER_DELAY_DOC);
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