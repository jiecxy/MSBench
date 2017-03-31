package cn.ac.ict.worker.callback;

/**
 * Created by krumo on 3/31/17.
 */
public class SimpleReadCallBack implements ReadCallBack{
    @Override
    public void handleReceivedMessage(byte[] msg, long requestTime) {
        System.out.println("Received msg "+new String(msg));
        return;
    }
}
