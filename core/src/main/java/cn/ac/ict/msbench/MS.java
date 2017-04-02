package cn.ac.ict.msbench;

import cn.ac.ict.msbench.exception.MSException;
import cn.ac.ict.msbench.worker.callback.ReadCallBack;
import cn.ac.ict.msbench.worker.callback.WriteCallBack;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by jiecxy on 2017/3/1.
 */
public abstract class MS {

    protected boolean isProducer = false;
    protected String streamName = "";
    protected int from = -2; // -1: read from latest; 0: read from oldest

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

    public MS(String streamName, boolean isProducer, Properties p, int from) {
        this.streamName = streamName;
        this.isProducer = isProducer;
        this.properties = p;
        this.from = from;

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
    // TODO 将其变成master调用一次命令
    public abstract  void initializeMS(ArrayList<String> streams) throws MSException;

    public abstract void finalizeMS(ArrayList<String> streams) throws MSException;

    /**
     * Send a record to the Message System.
     *
     * @param msg The message to be sent
     * @return
     */
    public abstract void send(boolean isSync, byte[] msg, WriteCallBack sentCallBack, long requestTime);

    /**
     * read messages from the Message System.
     *
     * @param
     * @return
     */
    public abstract void read(ReadCallBack readCallBack, long requestTime);

    /**
     * close the Message System.
     *
     * @param
     * @return
     */
    public abstract void close();
}
