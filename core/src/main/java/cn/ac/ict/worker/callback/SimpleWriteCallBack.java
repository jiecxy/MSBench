package cn.ac.ict.worker.callback;

import cn.ac.ict.msbench.worker.callback.WriteCallBack;

/**
 * Created by krumo on 3/31/17.
 */
public class SimpleWriteCallBack implements WriteCallBack {
    @Override
    public void handleSentMessage(byte[] msg, long requestTime) {
        System.out.println("sent msg "+new String(msg));
        return;
    }
}
