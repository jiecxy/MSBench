package cn.ac.ict.msbench.communication;

import akka.actor.*;
import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.exception.MSException;
import cn.ac.ict.msbench.worker.*;
import cn.ac.ict.msbench.stat.StatHeader;
import cn.ac.ict.msbench.stat.StatTail;
import cn.ac.ict.msbench.stat.StatWindow;
import cn.ac.ict.msbench.worker.job.Job;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static cn.ac.ict.msbench.communication.Command.*;

public class WorkerCom extends Communication implements CallBack {

    private String workerID = UUID.randomUUID().toString();
    private ActorSelection master;
    private Cancellable registerScheduler;
    private Cancellable heartbeatScheduler;
    private Worker worker = null;
    private Thread workerThread = null;

    private MS ms = null;
    private boolean isWriter;
    private Job job;

    public WorkerCom(String masterIP, int masterPort, MS ms, Job job) {
        super(masterIP, masterPort);
        String path = "akka.tcp://MSBenchMaster@" + masterIP +  ":" + masterPort + "/user/master";
        master = getContext().actorSelection(path);
        this.ms = ms;
        this.job = job;
        this.job.statInterval = STATS_INTERVAL;
        isWriter = job.isWriter;
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

    @Override
    public void preStart() throws Exception {
        super.preStart();
        Command registerCmd = new Command(workerID, REGISTER_WORKER, TYPE.REQUEST);
        registerCmd.data = job;
        //TODO 改成尝试次数
        registerScheduler = getContext().system().scheduler().schedule(Duration.create(500, TimeUnit.MILLISECONDS), Duration.create(2, TimeUnit.SECONDS),
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
                case INITIALIZE_MS:
                    switch (msg.type) {
                        case REQUEST:
                            //TODO 检查是否成功
                            ms.initializeMS((ArrayList<String>) msg.data);
                            master.tell(new Command(workerID, INITIALIZE_MS, TYPE.RESPONSE), getSelf());
                            break;
                        default:
                            unhandled(message);
                            break;
                    }
                    break;
                case FINALIZE_MS:
                    switch (msg.type) {
                        case REQUEST:
                            //TODO 检查是否成功
                            try {
                                ms.finalizeMS((ArrayList<String>) msg.data);
                            } catch (MSException e) {
                                e.printStackTrace();
                            }
                            master.tell(new Command(workerID, FINALIZE_MS, TYPE.RESPONSE), getSelf());
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
//                            Command cmd = new Command(workerID, METRICS_WINDOW, TYPE.RESPONSE);
//                            cmd.data = msg.data;
                            master.tell(new Command(workerID, METRICS_WINDOW, TYPE.RESPONSE, msg.data), getSelf());
                            System.out.println("METRICS_WINDOW " + msg.data);
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
//                            Command cmd = new Command(workerID, METRICS_HEAD, TYPE.RESPONSE);
//                            cmd.data = msg.data;
                            master.tell(new Command(workerID, METRICS_HEAD, TYPE.RESPONSE, msg.data), getSelf());
                            System.out.println("METRICS_HEAD " + msg.data);
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
//                            Command cmd = new Command(workerID, METRICS_TAIL, TYPE.RESPONSE);
//                            cmd.data = msg.data;
                            master.tell(new Command(workerID, METRICS_TAIL, TYPE.RESPONSE, msg.data), getSelf());
                            System.out.println("METRICS_TAIL " + msg.data);
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
        if (registerScheduler != null)
            registerScheduler.cancel();
        if (heartbeatScheduler != null)
            heartbeatScheduler.cancel();
        getContext().system().stop(getSelf());
        getContext().system().terminate();
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        //getContext().stop(getSelf());
//        System.out.println("WorkerCom postStop ");
    }

    public void onSendStatHeader(StatHeader header) {
//        System.out.println("WorkerCom onSendStatHeader " + header);

//        Command cmd = new Command(workerID, METRICS_HEAD, TYPE.REQUEST);
//        cmd.data = header;
//        getSelf().tell(cmd, getSelf());

        master.tell(new Command(workerID, METRICS_HEAD, TYPE.RESPONSE, header), getSelf());
        System.out.println("METRICS_HEAD " + header);
    }

    public void onSendStatWindow(StatWindow window) {
//        System.out.println("WorkerCom onSendWindowMetrics " + window);

//        Command cmd = new Command(workerID, METRICS_WINDOW, TYPE.REQUEST);
//        cmd.data = window;
//        getSelf().tell(cmd, getSelf());

        master.tell(new Command(workerID, METRICS_WINDOW, TYPE.RESPONSE, window), getSelf());
        System.out.println("METRICS_WINDOW " + window);
    }

    public void onSendStatTail(StatTail tail) {
//        System.out.println("WorkerCom onSendStatTail " + tail);

//        Command cmd = new Command(workerID, METRICS_TAIL, TYPE.REQUEST);
//        cmd.data = tail;
//        getSelf().tell(cmd, getSelf());

        master.tell(new Command(workerID, METRICS_TAIL, TYPE.RESPONSE, tail), getSelf());
        System.out.println("METRICS_TAIL " + tail);
    }
}
