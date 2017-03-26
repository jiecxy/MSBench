package cn.ac.ict.ms;

import cn.ac.ict.MS;
import cn.ac.ict.Status;
import cn.ac.ict.worker.WorkerCallBack;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class KafkaClient extends MS {
    @Override
    public Status send(boolean isSync, byte[] msg, String stream, WorkerCallBack sentCallBack) {
        return null;
    }

    @Override
    public Status read(String stream, WorkerCallBack readCallBack) {
        return null;
    }

    public Status close() {
        return null;
    }
}
