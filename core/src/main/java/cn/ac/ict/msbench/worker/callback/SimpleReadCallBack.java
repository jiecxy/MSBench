package cn.ac.ict.msbench.worker.callback;

import cn.ac.ict.msbench.worker.callback.ReadCallBack;

/**
 * Created by krumo on 3/31/17.
 */
public class SimpleReadCallBack implements ReadCallBack {
    @Override
    public void handleReceivedMessage(byte[] msg, long requestTime) {
        System.out.println("Received msg "+new String(msg));
        return;
    }
}
