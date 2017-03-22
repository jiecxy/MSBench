package cn.ac.ict.communication;

import akka.actor.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static cn.ac.ict.communication.Command.*;

public class MasterCom extends Communication {

    private Map<String, WorkerComINfo> workers = new HashMap<String, WorkerComINfo>();
    private String hostURL;
    private int port;
    private final int REQUIRED_WORKER_NUM = 2;
    private Cancellable checkTimeoutScheduler = null;

    public MasterCom(String hostURL, int port) {
        this.hostURL = hostURL;
        this.port = port;
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
        ActorRef master = system.actorOf(Props.create(MasterCom.class, args[0], Integer.parseInt(args[1])), "master");
        System.out.println("MasterCom tell ");
        master.tell("MasterCom MESSAGES", master);
        system.awaitTermination();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        checkTimeoutScheduler = getContext().system().scheduler().schedule(Duration.create(500, TimeUnit.SECONDS), Duration.create(CHECK_TIMEOUT_SEC, TimeUnit.SECONDS),
                getSelf(), new Command(CHECK_TIMEOUT, TYPE.REQUEST), getContext().dispatcher(), getSelf());
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        checkTimeoutScheduler.cancel();
    }

    public void onReceive(Object message) throws Throwable {
        System.out.println("MasterCom receive " + message + " from sender: " + getSender());
        if (message instanceof Command) {
            Command msg = (Command)message;
            switch (msg.api) {
                case REGISTER_WORKER:
                    if (msg.type == TYPE.REQUEST) {
                        if (registerWorker(msg)) {
                            if (checkWorkersReady()) {
                                sendStartWorkerRequest();
                            }
                        } else {
                            System.out.println("register fail");
                        }
                    } else {
                        unhandled(message);
                    }
                    break;
                case METRICS_HEAD:
                    switch (msg.type) {
                        case RESPONSE:
                            System.out.println("METRICS_HEAD = " + msg.data);
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case METRICS_TAIL:
                    switch (msg.type) {
                        case RESPONSE:
                            System.out.println("METRICS_TAIL = " + msg.data);
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case METRICS_WINDOW:
                    switch (msg.type) {
                        case RESPONSE:
                            System.out.println("METRICS_WINDOW = " + msg.data);
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case CHECK_TIMEOUT:
                    switch (msg.type) {
                        case REQUEST:
                            checkTimeoutWorker();
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case HEARTBEAT:
                    switch (msg.type) {
                        case RESPONSE:
                            workers.get(msg.data).lastHeartbeat = System.currentTimeMillis();
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
        } else if (message instanceof Terminated) {
            Terminated t = (Terminated)message;
            System.out.println("Terminated " + t.actor());
            for (Map.Entry<String, WorkerComINfo> entry : workers.entrySet()) {
                if (entry.getValue().ref.equals(t.getActor())) {
                    entry.getValue().status = WorkerComINfo.STATUS.TERMINATED;
                }
            }
//            if (checkIfAllDead()) {
//                stopMaster();
//            }
        } else {
            System.out.println("MasterCom onReceive " + message);
        }
    }

    private void stopMaster() {
        getContext().system().stop(getSelf());
        getContext().system().terminate();
    }

    private boolean checkIfAllDead() {
        for (Map.Entry<String, WorkerComINfo> entry : workers.entrySet()) {
            if (entry.getValue().status != WorkerComINfo.STATUS.TERMINATED) {
                return false;
            }
        }
        return true;
    }

    private void checkTimeoutWorker() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, WorkerComINfo> entry : workers.entrySet()) {
            if (now - entry.getValue().lastHeartbeat > CHECK_TIMEOUT_SEC) {
                entry.getValue().ref.isTerminated();
                //TODO 有worker超时，目前先关闭所有
                System.out.println("Some Worker Timeout");
                stopAllClients();
            }
        }
    }

    private void stopAllClients() {
        sendToAllLiveWorkers(new Command(STOP_CLIENT, TYPE.REQUEST));
    }

    private void sendStartWorkerRequest() {
        sendToAllLiveWorkers(new Command(START_WORK, TYPE.REQUEST));
    }

    private void sendToAllLiveWorkers(Object object) {
        for (Map.Entry<String, WorkerComINfo> entry : workers.entrySet()) {
            if (entry.getValue().status == WorkerComINfo.STATUS.RUNNING) {
                entry.getValue().ref.tell(object, getSelf());
            }
        }
    }

    private boolean registerWorker(Command request) {
        if (workers.containsKey(request.data)) {
            Command cmd = new Command(REGISTER_WORKER, TYPE.RESPONSE);
            cmd.status = STATUS.EXISTED;
            getSender().tell(cmd, getSelf());
            return false;
        } else {
            workers.put((String)request.data, new WorkerComINfo(getSender(), System.currentTimeMillis()));
            getContext().watch(getSender());

            System.out.println("register " + request.data);

            Command cmd = new Command(REGISTER_WORKER, TYPE.RESPONSE);
            cmd.status = STATUS.SUCCESS;
            getSender().tell(cmd, getSelf());
            return true;
        }
    }

    private boolean checkWorkersReady() {
        int count = 0;
        for (Map.Entry<String, WorkerComINfo> entry : workers.entrySet()) {
            if (entry.getValue().status == WorkerComINfo.STATUS.RUNNING) {
                count++;
            }
        }
        return count == REQUIRED_WORKER_NUM;
    }
}