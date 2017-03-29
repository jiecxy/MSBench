package cn.ac.ict;

import cn.ac.ict.exception.MSException;
import cn.ac.ict.worker.callback.ReadCallBack;
import cn.ac.ict.worker.callback.WriteCallBack;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by jiecxy on 2017/3/1.
 */
public abstract class MS {

    protected boolean isProducer = false;
    protected String streamName = "";

    /**
     * Properties for configuring this MSCLient.
     */
    private Properties properties = null;

    public void setProducer(boolean producer) {
        isProducer = producer;
    }

    /**
     * Get the set of properties for this MSCLient.
     */
    public Properties getProperties() {
        return properties;
    }

    public MS(String streamName, boolean isProducer, Properties p) {
        this.streamName = streamName;
        this.isProducer = isProducer;
        this.properties = p;
    }

    /**
     * Initialize any state for Message System. Like create topic in kafka, you can't create topic by every worker.
     *
     * !!!!!!!!!!!!!!!!!!!!
     * Note:
     *      This method will be called by master indirectly
     *      It means that master will send a request, that asks worker to initialize the message system, to one worker, only one.
     *      For this method, it will be called only one time, executed by worker by from master's command.
     */
    public abstract  void initializeMS(ArrayList<String> streams) throws MSException;

    /**
     * Send a record to the Message System.
     *
     * @param msg The message to be sent
     * @return
     */
    public abstract void send(boolean isSync, byte[] msg, WriteCallBack sentCallBack);

    /**
     * read messages from the Message System.
     *
     * @param
     * @return
     */
    public abstract void read(ReadCallBack readCallBack);

    //TODO 读操作怎么处理
    public void doRead(ReadCallBack readCallBack) {
        while (true) {

        }
    }

    /**
     * close the Message System.
     *
     * @param
     * @return
     */
    public abstract void close();
}
