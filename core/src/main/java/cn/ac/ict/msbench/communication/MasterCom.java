package cn.ac.ict.msbench.communication;

import akka.actor.*;
import cn.ac.ict.msbench.exporter.Exporter;
import cn.ac.ict.msbench.stat.StatHeader;
import cn.ac.ict.msbench.stat.StatTail;
import cn.ac.ict.msbench.stat.StatWindow;
import cn.ac.ict.msbench.worker.job.Job;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static cn.ac.ict.msbench.communication.Command.*;

public class MasterCom extends Communication {

    private static final Logger log = LoggerFactory.getLogger(MasterCom.class);

    private final int WAIT_REGISTRATION_TIMEOUT_MS = 20*1000;
    private final int CHECK_WORKER_TIMEOUT_INTERVAL_MS = WORKER_TIMEOUT_MS/4;

    private long startTime = -1;

    private final String masterID = "master";
    private Map<String, WorkerComInfo> workers = new HashMap<String, WorkerComInfo>();
    private Cancellable checkTimeoutScheduler = null;
    private boolean isRunning = false;

    private Exporter exporter = null;
    private ArrayList<String> streams;
    private int writerNum;
    private int readerNum;
    private int runTime;

    public MasterCom(Exporter exporter, String masterIP, int masterPort, int runTime, ArrayList<String> streams, int writerNum, int readerNum) {
        super(masterIP, masterPort);
        this.streams = streams;
        this.exporter = exporter;
        this.runTime = runTime;
        this.writerNum = writerNum;
        this.readerNum = readerNum;
        log.info("Started Master.");
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("akka.remote.netty.tcp.hostname", args[0]);
        props.setProperty("akka.remote.netty.tcp.port", args[1]);
        props.setProperty("akka.actor.provider", "remote");
        props.setProperty("akka.actor.serializers.proto", "akka.remote.serialization.ProtobufSerializer");
        props.setProperty("akka.actor.warn-about-java-serializer-usage", "off");
        props.setProperty("akka.remote.log-remote-lifecycle-events", "off");
        Config akkaConf = ConfigFactory.parseProperties(props);
        ActorSystem system = ActorSystem.create("MSBenchMaster", akkaConf);
        System.out.println("MasterCom start " + args[0] + ":" + args[1]);
        //ActorRef master = system.actorOf(Props.create(MasterCom.class, args[0], Integer.parseInt(args[1])), "master");
        ActorRef master = system.actorOf(Props.create(MasterCom.class), "master");
        System.out.println("MasterCom tell ");
        master.tell("MasterCom MESSAGES", master);
        system.awaitTermination();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        log.info(getMasterLogPrefix() + "Starting check timeout scheduler...");
        startTime = System.currentTimeMillis();
        checkTimeoutScheduler = getContext().system().scheduler().schedule(Duration.create(500, TimeUnit.MILLISECONDS), Duration.create(CHECK_WORKER_TIMEOUT_INTERVAL_MS, TimeUnit.MILLISECONDS),
                getSelf(), new Command(masterID, CHECK_TIMEOUT, TYPE.REQUEST), getContext().dispatcher(), getSelf());
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
    }

    private String getMasterLogPrefix() {
        return "Master: " ;
    }

    public void onReceive(Object message) throws Throwable {
//        System.out.println("MasterCom receive " + message + " from sender: " + getSender());
        if (message instanceof Command) {
            Command msg = (Command)message;
            switch (msg.api) {
                case REGISTER_WORKER:
                    if (msg.type == TYPE.REQUEST) {
                        if (registerWorker(msg, getSender())) {
                            if (checkWorkersReady()) {
                                isRunning = true;
                                if (NEED_INITIALIZE_MS) {
                                    log.info(getMasterLogPrefix() + "Sending Initialize request...");
                                    sendInitializeRequest();
                                } else {
                                    log.info(getMasterLogPrefix() + "INITIALIZE_MS is disabled. Sending StartWorker request...");
                                    sendStartWorkerRequest();
                                }
                            }
                        } else {
                            log.error(getMasterLogPrefix() + "Worker with ID :" + msg.from + " register failed!");
                        }
                    } else {
                        unhandled(msg);
                    }
                    break;
                case INITIALIZE_MS:
                    if (msg.type == TYPE.RESPONSE) {
                        if (msg.status == STATUS.SUCCESS) {
                            log.info(getMasterLogPrefix() + "Initialization Success. Sending StartWorker request...");
                            sendStartWorkerRequest();
                        } else {
                            log.error(getMasterLogPrefix() + "Failed to initialize MS. Stopping all workers...");
                            stopAllLiveClients();
                        }
                    } else {
                        unhandled(msg);
                    }
                    break;
                case FINALIZE_MS:
                    if (msg.type == TYPE.RESPONSE) {
                        if (msg.status == STATUS.FAIL) {
                            log.error(getMasterLogPrefix() + "Failed to finalize MS. Stopping all workers...");
                        }
                        stopAllLiveClients();
                    } else {
                        unhandled(msg);
                    }
                    break;
                case METRICS_HEAD:
                    switch (msg.type) {
                        case RESPONSE:
                            log.debug(getMasterLogPrefix() + "Received METRICS_HEAD from worker (" + msg.from + "): " +  msg.data);
//                            System.out.print(StatWindow.printHead());
                            workers.get(msg.from).insertHeader(exporter, (StatHeader) msg.data);
                            break;
                        default:
                            unhandled(msg);
                            break;
                    }
                    break;
                case METRICS_TAIL:
                    switch (msg.type) {
                        case RESPONSE:
                            log.debug(getMasterLogPrefix() + "Received METRICS_TAIL from worker (" + msg.from + "): " +  msg.data);
                            workers.get(msg.from).insertTail(exporter, (StatTail) msg.data);
                            workers.get(msg.from).status = WorkerComInfo.STATUS.DONE;
                            if (checkIfAllDone()) {
                                if (NEED_FINALIZE_MS) {
                                    log.info(getMasterLogPrefix() + "Sending FINALIZE_MS request...");
                                    sendFinalizeRequest();
                                } else {
                                    log.info(getMasterLogPrefix() + "FINALIZE_MS is disabled. Stopping all clients...");
                                    stopAllLiveClients();
                                }
                            }
                            break;
                        default:
                            unhandled(msg);
                            break;
                    }
                    break;
                case METRICS_WINDOW:
                    switch (msg.type) {
                        case RESPONSE:
                            log.debug(getMasterLogPrefix() + "Received METRICS_WINDOW from worker (" + msg.from + "): " +  msg.data);
                            workers.get(msg.from).insertWindow(exporter, (StatWindow)msg.data);
                            break;
                        default:
                            unhandled(msg);
                            break;
                    }
                    break;
                case CHECK_TIMEOUT:
                    switch (msg.type) {
                        case REQUEST:
                            checkTimeoutWorker();
                            break;
                        default:
                            unhandled(msg);
                            break;
                    }
                    break;
                case HEARTBEAT:
                    switch (msg.type) {
                        case REQUEST:
                            log.debug(getMasterLogPrefix() + "Received HEARTBEAT from worker (" + msg.from + "): ");
                            workers.get(msg.data).lastHeartbeat = System.currentTimeMillis();
                            getSender().tell(new Command(masterID, HEARTBEAT, TYPE.RESPONSE), getSelf());
                            break;
                        default:
                            unhandled(msg);
                            break;
                    }
                    break;
                default:
                    unhandled(msg);
                    break;
            }
        } else if (message instanceof Terminated) {
            Terminated t = (Terminated) message;
            for (Map.Entry<String, WorkerComInfo> entry : workers.entrySet()) {
                if (entry.getValue().ref.equals(t.getActor())) {
                    if (entry.getValue().status == WorkerComInfo.STATUS.RUNNING) {
                        log.error(getMasterLogPrefix() + "Worker (ID: " + entry.getValue().workerID + ") down accidentally, closing all workers...");
                        stopAllLiveClients();
                    } else {
                        log.info(getMasterLogPrefix() + "Worker (ID: " + entry.getValue().workerID + ") Terminated.");
                    }
                    entry.getValue().status = WorkerComInfo.STATUS.TERMINATED;
                }
            }
            if (checkIfAllDead()) {
                log.info(getMasterLogPrefix() + "All Workers are dead. Stoping master...");
                stopMaster();
            }
        } else {
            log.error(getMasterLogPrefix() + "Received unknown message: " + message);
        }
    }

    private void sendFinalizeRequest() {
        for (Map.Entry<String, WorkerComInfo> entry : workers.entrySet()) {
            if (entry.getValue().status == WorkerComInfo.STATUS.DONE) {
                entry.getValue().ref.tell(new Command(masterID, FINALIZE_MS, TYPE.REQUEST, streams), getSelf());
                break;
            }
        }
    }

    private void sendInitializeRequest() {
        for (Map.Entry<String, WorkerComInfo> entry : workers.entrySet()) {
            if (entry.getValue().status == WorkerComInfo.STATUS.RUNNING) {
                entry.getValue().ref.tell(new Command(masterID, INITIALIZE_MS, TYPE.REQUEST, streams), getSelf());
                break;
            }
        }
    }

    private void stopMaster() {
        try {
            exporter.close();
        } catch (IOException e) {
            log.error(getMasterLogPrefix() + "Fail to close data file!");
            e.printStackTrace();
        }
        if (checkTimeoutScheduler != null)
            checkTimeoutScheduler.cancel();
        getContext().system().stop(getSelf());
        getContext().system().terminate();
        log.info(getMasterLogPrefix() + "Master Stopped.");
    }

    private boolean checkIfAllDead() {
        int count = 0;
        for (Map.Entry<String, WorkerComInfo> entry : workers.entrySet()) {
            if (entry.getValue().status == WorkerComInfo.STATUS.TERMINATED
                    || entry.getValue().status == WorkerComInfo.STATUS.TIMEOUT) {
                count++;
            }
        }
        return !workers.isEmpty() && count == workers.size();
    }

    private boolean checkIfAllDone() {
        int count = 0;
        for (Map.Entry<String, WorkerComInfo> entry : workers.entrySet()) {
            if (entry.getValue().status == WorkerComInfo.STATUS.DONE) {
                count++;
            }
        }
        return count >= readerNum + writerNum;
    }

    private void checkTimeoutWorker() {
        long now = System.currentTimeMillis();
        if (isRunning) {
            // while running the test
            for (Map.Entry<String, WorkerComInfo> entry : workers.entrySet()) {
                if (entry.getValue().status == WorkerComInfo.STATUS.TERMINATED
                        || entry.getValue().status == WorkerComInfo.STATUS.TIMEOUT) {

                } else {
                    long dif = now - entry.getValue().lastHeartbeat;
                    if (dif >= WORKER_TIMEOUT_MS) {
                        entry.getValue().status = WorkerComInfo.STATUS.TIMEOUT;
                        log.error(getMasterLogPrefix() + "Worker (ID: " + entry.getValue().workerID + ") Timeout " + dif + " ms, closing all clients");
                        stopAllLiveClients();
                    }
                }
            }
        } else {
            // while waiting the registration
            long dif = System.currentTimeMillis() - startTime;
            if (dif >=  WAIT_REGISTRATION_TIMEOUT_MS) {
                log.error(getMasterLogPrefix() + "Waiting required workers' registration Timeout " + dif + " ms, closing all clients and master");
                int[] num = getAvailableWorkerNum();
                if (num[0] + num[1] == 0) {
                    stopMaster();
                } else {
                    stopAllLiveClients();
                }
            }
        }
    }

    private void stopAllLiveClients() {
        sendToAllLiveWorkers(new Command(masterID, STOP_CLIENT, TYPE.REQUEST));
    }

    private void sendStartWorkerRequest() {
        sendToAllLiveWorkers(new Command(masterID, START_WORK, TYPE.REQUEST));
    }

    private void sendToAllLiveWorkers(Object object) {
        for (Map.Entry<String, WorkerComInfo> entry : workers.entrySet()) {
            if (entry.getValue().status != WorkerComInfo.STATUS.TERMINATED) {
                entry.getValue().ref.tell(object, getSelf());
            }
        }
    }

    private boolean registerWorker(Command request, ActorRef sender) {
        int[] num = getAvailableWorkerNum();
        if (((Job)request.data).isWriter) {
            if (num[0] >= writerNum) {
                Command cmd = new Command(masterID, REGISTER_WORKER, TYPE.RESPONSE);
                cmd.status = STATUS.FAIL;
                sender.tell(cmd, getSelf());
                log.warn(getMasterLogPrefix() + "Refused registration from writer " + request.from + ", Because there are enough writer.");
                return false;
            }
        } else {
            if (num[0] >= readerNum) {
                Command cmd = new Command(masterID, REGISTER_WORKER, TYPE.RESPONSE);
                cmd.status = STATUS.FAIL;
                sender.tell(cmd, getSelf());
                log.warn(getMasterLogPrefix() + "Refused registration from reader " + request.from + ", Because there are enough reader.");
                return false;
            }
        }

        if (workers.containsKey(request.from)) {
            Command cmd = new Command(masterID, REGISTER_WORKER, TYPE.RESPONSE);
            cmd.status = STATUS.EXISTED;
            sender.tell(cmd, getSelf());
            return false;
        } else {
            workers.put(request.from, new WorkerComInfo(request.from, sender, (Job) request.data, System.currentTimeMillis()));
            getContext().watch(sender);
            log.info(getMasterLogPrefix() + "Worker registered with ID: " + request.from + " and  Job: " + request.data);
            Command cmd = new Command(masterID, REGISTER_WORKER, TYPE.RESPONSE);
            cmd.status = STATUS.SUCCESS;
            sender().tell(cmd, getSelf());
            return true;
        }
    }

    private int[] getAvailableWorkerNum() {
        int writerCount = 0, readerCount = 0;
        for (Map.Entry<String, WorkerComInfo> entry : workers.entrySet()) {
            if (entry.getValue().status == WorkerComInfo.STATUS.RUNNING) {
                if (entry.getValue().job.isWriter) {
                    writerCount++;
                } else {
                    readerCount++;
                }
            }
        }
        return new int[] {writerCount, readerCount};
    }

    private boolean checkWorkersReady() {
        int[] num = getAvailableWorkerNum();
        if (num[0] > writerNum) {
            log.error(getMasterLogPrefix() + "Available Worker writer number is greater than required number " + writerNum + "! Stopping all...");
            stopAllLiveClients();
            return false;
        }
        if (num[1] > readerNum) {
            log.error(getMasterLogPrefix() + "Available Worker reader number is greater than required number " + readerNum + "! Stopping all...");
            stopAllLiveClients();
            return false;
        }
        return num[0] == writerNum && num[1] == readerNum;
    }
}