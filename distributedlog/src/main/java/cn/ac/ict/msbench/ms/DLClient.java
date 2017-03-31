package cn.ac.ict.msbench.ms;

import cn.ac.ict.msbench.MS;
import cn.ac.ict.msbench.exception.MSException;
import cn.ac.ict.msbench.worker.callback.ReadCallBack;
import cn.ac.ict.msbench.worker.callback.WriteCallBack;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class DLClient extends MS {

    public DLClient(String streamName, boolean isProducer, Properties p, int from) {
        super(streamName, isProducer, p, from);
    }

    @Override
    public void initializeMS(ArrayList<String> streams) throws MSException {

    }

    @Override
    public void finalizeMS(ArrayList<String> streams) throws MSException {

    }

    @Override
    public void send(boolean isSync, byte[] msg, WriteCallBack sentCallBack, long requestTime) {

    }

    @Override
    public void read(ReadCallBack readCallBack, long requestTime) {

    }

    @Override
    public void close() {

    }
}
