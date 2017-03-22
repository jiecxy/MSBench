package cn.ac.ict.communication;

import akka.actor.*;
import cn.ac.ict.MS;
import cn.ac.ict.worker.Worker;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;
import cn.ac.ict.worker.throughput.ThroughputStrategy;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static cn.ac.ict.communication.Command.*;

public class WorkerCom extends Communication implements CallBack {


    private String workerID = UUID.randomUUID().toString();
    private ActorSelection master;
    private Cancellable registerScheduler;
    private Cancellable heartbeatScheduler;
    private Worker worker = null;
    private Thread workerThread = null;

    private Boolean isWriter;
    private MS ms = null;
    private String stream;
    private String workerIP;

    // Writer
    private int messageSize;
    private boolean isSync;
    private ThroughputStrategy strategy;

    // Reader
    private int from;

    public WorkerCom(String workerIP, String masterIP, int masterPort, int runTime, String stream, int from, MS ms) {
        super(masterIP, masterPort, runTime);
        String path = "akka.tcp://MSBenchMaster@" + masterIP +  ":" + masterPort + "/user/master";
        master = getContext().actorSelection(path);
        this.ms = ms;
        this.stream = stream;
        isWriter = false;
        this.from = from;
        this.workerIP = workerIP;
    }

    public WorkerCom(String workerIP, String masterIP, int masterPort, int runTime, String stream, MS ms, int messageSize, boolean isSync, ThroughputStrategy strategy) {
        super(masterIP, masterPort, runTime);
        String path = "akka.tcp://MSBenchMaster@" + masterIP +  ":" + masterPort + "/user/master";
        master = getContext().actorSelection(path);
        isWriter = true;
        this.ms = ms;
        this.stream = stream;
        this.messageSize = messageSize;
        this.isSync = isSync;
        this.strategy = strategy;
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
        worker.tell("WorkerCom MESSAGES", worker);
        //system.stop(worker);
        //system.terminate();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        Command registerCmd = new Command(REGISTER_WORKER, TYPE.REQUEST);
        registerCmd.data = workerID;
        //TODO 改成尝试次数
        registerScheduler = getContext().system().scheduler().schedule(Duration.create(500, TimeUnit.MILLISECONDS), Duration.create(2, TimeUnit.SECONDS),
                getSelf(), registerCmd, getContext().dispatcher(), getSelf());
    }

    @Override
    public void onReceive(Object message) throws Throwable {
        System.out.println("WorkerCom receive " + message);
        if (message instanceof Command) {
            Command msg = (Command)message;
            switch (msg.api) {
                case REGISTER_WORKER:
                    switch (msg.type) {
                        case REQUEST:
                            master.tell(msg, getSelf());
                            break;
                        case RESPONSE:
                            if (msg.status == Command.STATUS.SUCCESS) {
                                registerScheduler.cancel();
                                startHeartBeatScheduler();
                            } else if (msg.status == Command.STATUS.EXISTED) {
                                registerScheduler.cancel();
                                System.out.println("WorkerCom already registered");
                            } else if (msg.status == Command.STATUS.FAIL) {
                                System.out.println("WorkerCom register fail");
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
                            msg.data = workerID;
                            master.tell(msg, getSelf());
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case START_WORK:
                    switch (msg.type) {
                        case REQUEST:
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
                            stopAll();
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                // Only from Worker
                case METRICS_WINDOW:
                    switch (msg.type) {
                        case REQUEST:
                            Command cmd = new Command(METRICS_WINDOW, TYPE.RESPONSE);
                            cmd.data = msg.data;
                            master.tell(cmd, getSelf());
                            System.out.println("WorkerCom METRICS_WINDOW " + msg.data);
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                // Only from Worker
                case METRICS_HEAD:
                    switch (msg.type) {
                        case REQUEST:
                            Command cmd = new Command(METRICS_HEAD, TYPE.RESPONSE);
                            cmd.data = msg.data;
                            master.tell(cmd, getSelf());
                            System.out.println("WorkerCom METRICS_HEAD " + msg.data);
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                // Only from Worker
                case METRICS_TAIL:
                    switch (msg.type) {
                        case REQUEST:
                            Command cmd = new Command(METRICS_TAIL, TYPE.RESPONSE);
                            cmd.data = msg.data;
                            master.tell(cmd, getSelf());
                            System.out.println("WorkerCom METRICS_TAIL " + msg.data);
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
            System.out.println("WorkerCom onReceive " + message);
        }
    }

    private void startHeartBeatScheduler() {
        heartbeatScheduler = getContext().system().scheduler().schedule(Duration.create(0, TimeUnit.MILLISECONDS), Duration.create(CHECK_TIMEOUT_SEC / 4, TimeUnit.SECONDS),
                getSelf(), new Command(HEARTBEAT, TYPE.REQUEST), getContext().dispatcher(), getSelf());
    }

    private void startWorker() {
        worker = new Worker(this);
        workerThread = new Thread(worker);
        workerThread.start();
    }

    private void stopWorker() {
        if (worker != null)
            worker.stopWork();
    }

    private void stopAll() {
        stopWorker();
        getContext().system().stop(getSelf());
        getContext().system().terminate();
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        //getContext().stop(getSelf());
        System.out.println("WorkerCom postStop ");
    }

    public void onSendStatHeader(StatHeader header) {
        System.out.println("WorkerCom onSendStatHeader " + header);

        Command cmd = new Command(METRICS_HEAD, TYPE.REQUEST);
        cmd.data = header;
        getSelf().tell(cmd, getSelf());
    }

    public void onSendStatWindow(StatWindow window) {
        System.out.println("WorkerCom onSendWindowMetrics " + window);

        Command cmd = new Command(METRICS_WINDOW, TYPE.REQUEST);
        cmd.data = window;
        getSelf().tell(cmd, getSelf());
    }

    public void onSendStatTail(StatTail tail) {
        System.out.println("WorkerCom onSendStatTail " + tail);

        Command cmd = new Command(METRICS_TAIL, TYPE.REQUEST);
        cmd.data = tail;
        getSelf().tell(cmd, getSelf());
    }
}
