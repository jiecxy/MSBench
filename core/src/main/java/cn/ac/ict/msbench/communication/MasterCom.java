package cn.ac.ict.msbench.communication;

import akka.actor.*;
import cn.ac.ict.msbench.stat.StatHeader;
import cn.ac.ict.msbench.stat.StatTail;
import cn.ac.ict.msbench.stat.StatWindow;
import cn.ac.ict.msbench.worker.job.Job;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static cn.ac.ict.msbench.communication.Command.*;

public class MasterCom extends Communication {

    private final String masterID = "master";
    private Map<String, WorkerComInfo> workers = new HashMap<String, WorkerComInfo>();
    private Cancellable checkTimeoutScheduler = null;

    private ArrayList<String> streams;
    private int writerNum;
    private int readerNum;
    private int runTime;

    public MasterCom(String masterIP, int masterPort, int runTime, ArrayList<String> streams, int writerNum, int readerNum) {
        super(masterIP, masterPort);
        this.streams = streams;
        this.runTime = runTime;
        this.writerNum = writerNum;
        this.readerNum = readerNum;
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
        checkTimeoutScheduler = getContext().system().scheduler().schedule(Duration.create(500, TimeUnit.SECONDS), Duration.create(CHECK_TIMEOUT_SEC, TimeUnit.SECONDS),
                getSelf(), new Command(masterID, CHECK_TIMEOUT, TYPE.REQUEST), getContext().dispatcher(), getSelf());
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
    }

    public void onReceive(Object message) throws Throwable {
//        System.out.println("MasterCom receive " + message + " from sender: " + getSender());
        if (message instanceof Command) {
            Command msg = (Command)message;
            switch (msg.api) {
                case REGISTER_WORKER:
                    if (msg.type == TYPE.REQUEST) {
                        if (registerWorker(msg)) {
                            if (checkWorkersReady()) {
                                sendInitializeRequest();
                            }
                        } else {
                            System.out.println("register fail");
                        }
                    } else {
                        unhandled(msg);
                    }
                    break;
                case INITIALIZE_MS:
                    if (msg.type == TYPE.RESPONSE) {
                        if (checkWorkersReady()) {
                            sendStartWorkerRequest();
                        }
                    } else {
                        unhandled(msg);
                    }
                    break;
                case FINALIZE_MS:
                    if (msg.type == TYPE.RESPONSE) {
                        //TODO
                        stopAllLiveClients();
                    } else {
                        unhandled(msg);
                    }
                    break;
                case METRICS_HEAD:
                    switch (msg.type) {
                        case RESPONSE:
                            System.out.println("\nMETRICS_HEAD = \n" + msg.data + "\n");
                            System.out.print(StatWindow.printHead());
                            workers.get(msg.from).stat.head = (StatHeader) msg.data;
                            break;
                        default:
                            unhandled(msg);
                            break;
                    }
                    break;
                case METRICS_TAIL:
                    switch (msg.type) {
                        case RESPONSE:
                            System.out.println("\nMETRICS_TAIL = \n" + msg.data + "\n");
                            workers.get(msg.from).stat.tail = (StatTail) msg.data;
                            workers.get(msg.from).status = WorkerComInfo.STATUS.DONE;
                            if (checkIfAllDone()) {
                                sendFinalizeRequest();
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
                            System.out.println("" + msg.data);
                            workers.get(msg.from).stat.statWindow.add((StatWindow) msg.data);
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
                        case RESPONSE:
                            workers.get(msg.data).lastHeartbeat = System.currentTimeMillis();
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
            Terminated t = (Terminated)message;
            System.out.println("Terminated " + t.actor());
            for (Map.Entry<String, WorkerComInfo> entry : workers.entrySet()) {
                if (entry.getValue().ref.equals(t.getActor())) {
                    if (entry.getValue().status == WorkerComInfo.STATUS.RUNNING) {

                        System.out.println("Worker " + entry.getValue().ref + "down accidentally, closing all workers...");
                        stopAllLiveClients();
                    }
                    entry.getValue().status = WorkerComInfo.STATUS.TERMINATED;
                }
            }
            if (checkIfAllDead()) {
                stopMaster();
            }
        } else {
            System.out.println("MasterCom onReceive " + message);
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
        if (checkTimeoutScheduler != null)
            checkTimeoutScheduler.cancel();
        getContext().system().stop(getSelf());
        getContext().system().terminate();
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

    // TODO 打印数据还是什么？
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
        for (Map.Entry<String, WorkerComInfo> entry : workers.entrySet()) {
            if (entry.getValue().status == WorkerComInfo.STATUS.TERMINATED
                    || entry.getValue().status == WorkerComInfo.STATUS.TIMEOUT) {

            } else {
                if (now - entry.getValue().lastHeartbeat > CHECK_TIMEOUT_SEC*1000) {
                    entry.getValue().status = WorkerComInfo.STATUS.TIMEOUT;
                    //TODO 有worker超时，目前先关闭所有
                    System.out.println("Some Worker Timeout, closing all clients");
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

    private boolean registerWorker(Command request) {
        //TODO 判断启动多了的情况
        if (workers.containsKey(request.from)) {
            Command cmd = new Command(masterID, REGISTER_WORKER, TYPE.RESPONSE);
            cmd.status = STATUS.EXISTED;
            getSender().tell(cmd, getSelf());
            return false;
        } else {
            workers.put(request.from, new WorkerComInfo(getSender(), (Job) request.data, System.currentTimeMillis()));

            getContext().watch(getSender());

            System.out.println("Worker registered with ID: " + request.from + " and  " + request.data);

            Command cmd = new Command(masterID, REGISTER_WORKER, TYPE.RESPONSE);
            cmd.status = STATUS.SUCCESS;
            getSender().tell(cmd, getSelf());
            return true;
        }
    }

    private boolean checkWorkersReady() {
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
        //TODO 判断超过数量的情况
        return writerCount == writerNum && readerCount == readerNum;
    }
}