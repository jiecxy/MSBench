package cn.ac.ict.worker;

/**
 * Created by krumo on 3/24/17.
 */
public interface WorkerCallBack {
    public void handleSentMessage(byte[] msg);
    public void handleReceivedMessage(byte[] msg);
}
