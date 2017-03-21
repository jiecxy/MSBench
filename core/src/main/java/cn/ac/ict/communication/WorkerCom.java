package cn.ac.ict.communication;

import akka.actor.*;
import cn.ac.ict.Worker;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import scala.concurrent.duration.Duration;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static cn.ac.ict.communication.Command.*;

/**
 * Created by apple on 2017/3/20.
 */
public class WorkerCom extends Communication {

    private String workerID = UUID.randomUUID().toString();
    private String hostURL;
    private int port;
    private String masterHostURL;
    private int masterPort;
    private String path;
    private ActorSelection master;
    private Cancellable registerScheduler;
    private Cancellable heartbeatScheduler;
    private Worker worker = null;

    public WorkerCom(String hostURL, int port, String masterHostURL, int masterPort) {
        this.hostURL = hostURL;
        this.port = port;
        this.masterHostURL = masterHostURL;
        this.masterPort = masterPort;
        path = "akka.tcp://MSBenchMaster@" + masterHostURL +  ":" + masterPort + "/user/master";
        master = getContext().actorSelection(path);
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
        try {
            Thread.sleep((long)5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //system.stop(worker);
        //system.terminate();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        Command registerCmd = new Command(REGISTER_WORKER, TYPE.REQUEST);
        registerCmd.data = workerID;
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
                default:
                    unhandled(message);
                    break;
            }
        } else {
            System.out.println("WorkerCom onReceive " + message);
        }
    }

    private void close() {
        getContext().system().stop(getSelf());
        getContext().system().terminate();
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        //getContext().stop(getSelf());
        System.out.println("WorkerCom postStop ");
    }


    public void sendWindowMetrics(){
        //worker = new Worker(this);
    }

}
