package cn.ac.ict.msbench.worker.callback;

/**
 * Created by krumo on 3/26/17.
 */
public interface WriteCallBack {
    public void handleSentMessage(byte[] msg, long requestTimeInNano);
}
