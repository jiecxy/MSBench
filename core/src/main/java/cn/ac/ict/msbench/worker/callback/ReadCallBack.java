package cn.ac.ict.msbench.worker.callback;

/**
 * Created by krumo on 3/26/17.
 */
public interface ReadCallBack {
    public void handleReceivedMessage(byte[] msg, long requestTime,long publishTime);
}
