package cn.ac.ict.communication;

import akka.actor.*;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static cn.ac.ict.communication.Command.*;

/**
 * Created by jiecxy on 2017/3/17.
 */
public class MasterCom extends Communication {

    private Map<String, ActorRef> workers = new HashMap<String, ActorRef>();
    private String hostURL;
    private int port;
    private int WORKER_NUM = 1;

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
                default:
                    unhandled(message);
                    break;
            }
        } else if (message instanceof Terminated) {
            Terminated t = (Terminated)message;
            // TODO 判断是否中途结束
            System.out.println("Terminated " + t.actor());
        } else {
            System.out.println("MasterCom onReceive " + message);
        }
    }

    private void sendStartWorkerRequest() {
        for (Map.Entry<String, ActorRef> entry : workers.entrySet()) {
            entry.getValue().tell(new Command(START_WORK, TYPE.REQUEST), getSelf());
        }
    }

    private boolean registerWorker(Command request) {
        if (workers.containsKey((String)request.data)) {
            Command cmd = new Command(REGISTER_WORKER, TYPE.RESPONSE);
            cmd.status = STATUS.EXISTED;
            getSender().tell(cmd, getSelf());
            return false;
        } else {
            workers.put((String)request.data, getSender());
            getContext().watch(getSender());
            System.out.println("register " + request.data);
            Command cmd = new Command(REGISTER_WORKER, TYPE.RESPONSE);
            cmd.status = STATUS.SUCCESS;
            getSender().tell(cmd, getSelf());
            return true;
        }
    }

    private boolean checkWorkersReady() {
        return workers.size() == WORKER_NUM;
    }
}
