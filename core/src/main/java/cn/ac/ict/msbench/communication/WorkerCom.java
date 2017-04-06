package cn.ac.ict.msbench.communication;

import akka.actor.*;
import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.exception.MSException;
import cn.ac.ict.msbench.exporter.Exporter;
import cn.ac.ict.msbench.stat.MSBWorkerStat;
import cn.ac.ict.msbench.worker.*;
import cn.ac.ict.msbench.stat.StatHeader;
import cn.ac.ict.msbench.stat.StatTail;
import cn.ac.ict.msbench.stat.StatWindow;
import cn.ac.ict.msbench.worker.job.Job;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static cn.ac.ict.msbench.communication.Command.*;

public class WorkerCom extends Communication implements CallBack {

    private static final Logger log = LoggerFactory.getLogger(WorkerCom.class);

    private final int REGISTRATION_RETRIES = 6;
    private long lastHeartbeat = 0;

    private String workerID = "Worker";
    private int connectionAttemptCount = 0;
    private ActorSelection master;
    private Cancellable registerScheduler = null;
    private Cancellable heartbeatScheduler = null;
    private Worker worker = null;
    private Thread workerThread = null;
    private MSBWorkerStat stat = new MSBWorkerStat();

    private MS ms = null;
    private Exporter exporter = null;
    private boolean isWriter;
    private Job job;

    public WorkerCom(Exporter exporter, String masterIP, int masterPort, MS ms, Job job) {
        super(masterIP, masterPort);
        String path = "akka.tcp://MSBenchMaster@" + masterIP +  ":" + masterPort + "/user/master";
        master = getContext().actorSelection(path);
        this.ms = ms;
        //TODO do not store
        this.exporter = null;
//        this.exporter = exporter;
        this.job = job;
        this.job.statInterval = STATS_INTERVAL;
        isWriter = job.isWriter;
        workerID = isWriter ? "Writer" : "Reader";
        workerID += "-" + UUID.randomUUID().toString();
        log.debug("Starting Worker Communication with job: " + job);
        log.info("Started Worker Communication with ID: " + workerID);
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("akka.actor.provider", "remote");
        props.setProperty("akka.remote.netty.tcp.hostname", args[0]);
        props.setProperty("akka.remote.netty.tcp.port", args[2]);
        props.setProperty("akka.actor.warn-about-java-serializer-usage", "off");
        Config akkaConf = ConfigFactory.parseProperties(props);
        ActorSystem system = ActorSystem.create("MSBenchWorker", akkaConf);
        System.out.println("WorkerCom start " + args[0] + ":" + args[2]);
        ActorRef worker = system.actorOf(Props.create(WorkerCom.class, args[0], Integer.parseInt(args[2]), args[0], Integer.parseInt(args[1])), "worker");
//        worker.tell("WorkerCom MESSAGES", worker);
        //system.stop(worker);
        //system.terminate();
    }

    private String getWorkerLogPrefix() {
        return "WorkerCom (ID: " + workerID + ") ";
    }

    /***
     * start the thread to attempt to register
     * @throws Exception
     */
    @Override
    public void preStart() throws Exception {
        super.preStart();
        Command registerCmd = new Command(workerID, REGISTER_WORKER, TYPE.REQUEST);
        registerCmd.data = job;
        log.info(getWorkerLogPrefix() + "Starting register scheduler...");
        registerScheduler = getContext().system().scheduler().schedule(Duration.create(500, TimeUnit.MILLISECONDS), Duration.create(2000, TimeUnit.MILLISECONDS),
                getSelf(), registerCmd, getContext().dispatcher(), getSelf());
    }

    @Override
    public void onReceive(Object message) throws Throwable {
//        System.out.println("WorkerCom receive " + message);
        if (message instanceof Command) {
            Command msg = (Command)message;
            switch (msg.api) {
                case REGISTER_WORKER:
                    switch (msg.type) {
                        // only from worker
                        case REQUEST:
                            if (connectionAttemptCount < REGISTRATION_RETRIES) {
                                log.debug(getWorkerLogPrefix() + "Register attempt count: " + connectionAttemptCount);
                                master.tell(msg, getSelf());
                                connectionAttemptCount++;
                            } else {
                                log.error(getWorkerLogPrefix() + "Register retries timeout! Stopping all...");
                                stopAll();
                            }
                            break;
                        case RESPONSE:
                            if (msg.status == Command.STATUS.SUCCESS) {
                                registerScheduler.cancel();
                                lastHeartbeat = System.currentTimeMillis();
                                log.info(getWorkerLogPrefix() + "Register success.");
                                startHeartBeatScheduler();
                            } else if (msg.status == Command.STATUS.EXISTED) {
                                registerScheduler.cancel();
                                log.warn(getWorkerLogPrefix() + "Already registered.");
                            } else if (msg.status == Command.STATUS.FAIL) {
                                registerScheduler.cancel();
                                log.error(getWorkerLogPrefix() + "Registration was refused! Stopping all...");
                                stopAll();
                            }
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case INITIALIZE_MS:
                    switch (msg.type) {
                        case REQUEST:
                            Command cmd = new Command(workerID, INITIALIZE_MS, TYPE.RESPONSE);
                            try {
                                log.info(getWorkerLogPrefix() + "Initializing MS...");
                                ms.initializeMS((ArrayList<String>) msg.data);
                                cmd.status = STATUS.SUCCESS;
                                master.tell(cmd, getSelf());
                            } catch (MSException e) {
                                cmd.status = STATUS.FAIL;
                                master.tell(cmd, getSelf());
                                log.error(getWorkerLogPrefix() + "Failed to initialize MS. Stopping all");
                                e.printStackTrace();
                                stopAll();
                            }
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case FINALIZE_MS:
                    switch (msg.type) {
                        case REQUEST:
                            Command cmd = new Command(workerID, FINALIZE_MS, TYPE.RESPONSE);
                            try {
                                log.info(getWorkerLogPrefix() + "Finalizing MS...");
                                ms.finalizeMS((ArrayList<String>) msg.data);
                                cmd.status = STATUS.SUCCESS;
                                master.tell(cmd, getSelf());
                            } catch (MSException e) {
                                cmd.status = STATUS.FAIL;
                                master.tell(cmd, getSelf());
                                log.error(getWorkerLogPrefix() + "Failed to Finalize MS. Stopping all");
                                e.printStackTrace();
                            }
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case HEARTBEAT:
                    switch (msg.type) {
                        case REQUEST:
                            long dif = System.currentTimeMillis() - lastHeartbeat;
                            if (dif < WORKER_TIMEOUT_MS) {
                                log.debug(getWorkerLogPrefix() + "Sending heartbeat...");
                                msg.data = workerID;
                                master.tell(msg, getSelf());
                            } else {
                                log.error(getWorkerLogPrefix() + "Heartbeat Timeout with " + dif + " ms!. Stopping client");
                                stopAll();
                            }
                            break;
                        case RESPONSE:
                            lastHeartbeat = System.currentTimeMillis();
                            log.debug(getWorkerLogPrefix() + "Received heartbeat.");
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case START_WORK:
                    switch (msg.type) {
                        case REQUEST:
                            log.info(getWorkerLogPrefix() + "Received START_WORK request. Starting to work...");
                            startWorker();
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case STOP_WORK:
                    switch (msg.type) {
                        case REQUEST:
                            log.info(getWorkerLogPrefix() + "Received STOP_WORK request. Stopping work...");
                            stopWorker();
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case STOP_CLIENT:
                    switch (msg.type) {
                        case REQUEST:
                            log.info(getWorkerLogPrefix() + "Received STOP_CLIENT request. Stopping client...");
                            stopAll();
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                default:
                    unhandled(message);
                    break;
            }
        } else {
            log.error(getWorkerLogPrefix() + "Received an unknown message: " + message);
        }
    }

    // every 2s send a heartbeat

    /***
     * start the thread to send heatbeat to master
     */
    private void startHeartBeatScheduler() {
        log.info(getWorkerLogPrefix() + "Starting heartbeat scheduler...");
        heartbeatScheduler = getContext().system().scheduler().schedule(Duration.create(0, TimeUnit.MILLISECONDS), Duration.create(WORKER_TIMEOUT_MS / 4, TimeUnit.MILLISECONDS),
                getSelf(), new Command(workerID, HEARTBEAT, TYPE.REQUEST), getContext().dispatcher(), getSelf());
    }

    private void startWorker() {

        if (isWriter) {
            worker = new WriteWorker(this, ms, job);
        } else {
            worker = new ReadWorker(this, ms, job);
        }
        workerThread = new Thread(worker);
        workerThread.start();
    }

    private void stopWorker() {
        if (worker != null)
            worker.stopWork();
    }

    private void stopAll() {
        stopWorker();
        try {
            if (exporter != null)
                exporter.close();
        } catch (IOException e) {
            log.error(getWorkerLogPrefix() + "Failed to close data file!");
            e.printStackTrace();
        }
        if (registerScheduler != null)
            registerScheduler.cancel();
        if (heartbeatScheduler != null)
            heartbeatScheduler.cancel();
        getContext().system().stop(getSelf());
        getContext().system().terminate();
        log.info(getWorkerLogPrefix() + "Stopped.");
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
    }

    /***
     * Callback method, to send header data
     * @param header
     */
    public void onSendStatHeader(StatHeader header) {

        master.tell(new Command(workerID, METRICS_HEAD, TYPE.RESPONSE, header), getSelf());
        insertHeader(exporter, header);
        log.debug(getWorkerLogPrefix() + "Sending METRICS_HEAD " + header);
    }

    /**
     * Callback method, to send window data
     * @param window
     */
    public void onSendStatWindow(StatWindow window) {

        window.version = insertWindow(exporter, window);
        master.tell(new Command(workerID, METRICS_WINDOW, TYPE.RESPONSE, window), getSelf());
        log.debug(getWorkerLogPrefix() + "Sending METRICS_WINDOW " + window);
    }

    /***
     * Callback method, to send tail data
     * @param tail
     */
    public void onSendStatTail(StatTail tail) {

        master.tell(new Command(workerID, METRICS_TAIL, TYPE.RESPONSE, tail), getSelf());
        insertTail(exporter, tail);
        log.debug(getWorkerLogPrefix() + "Sending METRICS_TAIL " + tail);
    }

    private void insertHeader(Exporter exporter, StatHeader header) {
        stat.head = header;
        if (exporter != null) {
            exporter.write(workerID, METRICS_HEAD, header.toString());
        }
    }

    private int insertWindow(Exporter exporter, StatWindow window) {
        stat.statWindow.add(window);
        if (exporter != null) {
            exporter.write(workerID, METRICS_WINDOW, window.toString());
        }
        return stat.statWindow.size() - 1;
    }

    private void insertTail(Exporter exporter, StatTail tail) {
        stat.tail = tail;
        if (exporter != null) {
            exporter.write(workerID, METRICS_TAIL, tail.toString());
        }
    }
}
