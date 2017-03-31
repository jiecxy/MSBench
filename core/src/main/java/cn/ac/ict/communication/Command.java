package cn.ac.ict.communication;

import java.io.Serializable;

public class Command implements Serializable {

    // REQUEST/RESPONSE API
    public static final int REGISTER_WORKER = 1;
    public static final int HEARTBEAT = 2;
    public static final int START_WORK = 3;
    public static final int STOP_WORK = 4;
    public static final int METRICS_HEAD = 5;
    public static final int METRICS_WINDOW = 6;
    public static final int METRICS_TAIL = 7;
    public static final int STOP_CLIENT = 8;
    public static final int CHECK_TIMEOUT = 9;
    public static final int INITIALIZE_MS = 10;
    public static final int FINALIZE_MS = 11;

    public enum TYPE implements Serializable {
        REQUEST, RESPONSE, UNKNOWN;
    }

    public enum STATUS implements Serializable {
        SUCCESS, FAIL, REQUESTING, EXISTED;
    }

    public String from;  // workerId or master
    public int api = -1;
    public TYPE type = TYPE.UNKNOWN;
    public STATUS status = STATUS.FAIL;
    public Object data = null;
    public int version = -1;

    public Command(String from, int api, TYPE type) {
        this(from, api, type, null);
    }

    public Command(String from, int api, TYPE type, Object data) {
        this.data = data;
        this.from = from;
        this.api = api;
        this.type = type;
        if (this.type == TYPE.REQUEST) {
            status = STATUS.REQUESTING;
        } else if (this.type == TYPE.RESPONSE) {
            status = STATUS.SUCCESS;
        }
    }

    @Override
    public String toString() {
        return "{ API = " + getAPIName(api)
                + "; type = " + type
                + "; status = " + status
                + "; version = " + version
                + "; data = " + data + " }";
    }

    private String getAPIName(int api) {
        switch (api) {
            case REGISTER_WORKER: return "REGISTER_WORKER";
            case HEARTBEAT: return "HEARTBEAT";
            case START_WORK: return "START_WORK";
            case STOP_WORK: return "STOP_WORK";
            case METRICS_HEAD: return "METRICS_HEAD";
            case METRICS_WINDOW: return "METRICS_WINDOW";
            case METRICS_TAIL: return "METRICS_TAIL";
            case STOP_CLIENT: return "STOP_CLIENT";
            case CHECK_TIMEOUT: return "CHECK_TIMEOUT";
            case INITIALIZE_MS: return "INITIALIZE_MS";
            case FINALIZE_MS: return "FINALIZE_MS";
            default: return "UNKNOWN";
        }
    }
}
