package cn.ac.ict.communication;

import org.omg.CORBA.Object;

import java.io.Serializable;

import java.io.Serializable;

/**
 * Created by apple on 2017/3/20.
 */
public class Command implements Serializable {

    // REQUEST/RESPONSE API
    public static final int REGISTER_WORKER = 1;
    public static final int HEARTBEAT = 2;
    public static final int START_WORK = 3;
    public static final int STOP_WORK = 4;
    public static final int METRICS_HEAD = 5;
    public static final int METRICS_WINDOW = 6;
    public static final int METRICS_TAIL = 7;

    public enum TYPE implements Serializable {
        REQUEST, RESPONSE, UNKNOWN;
    }

    public enum STATUS implements Serializable {
        SUCCESS, FAIL, UNKNOWN, EXISTED;
    }

    public int api = -1;
    public TYPE type = TYPE.UNKNOWN;
    public STATUS status = STATUS.FAIL;
    public java.lang.Object data = null;

    public Command(int api, TYPE type) {
        this.api = api;
        this.type = type;
    }

    @Override
    public String toString() {
        return "{ ackAPI = " + api + "; type = " + type + " }";
    }
}
