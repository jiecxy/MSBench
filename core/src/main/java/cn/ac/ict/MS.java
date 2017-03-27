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

    /**
     * Properties for configuring this MSCLient.
     */
    private Properties properties = new Properties();

    /**
     * Set the properties for this MSCLient.
     */
    public void setProperties(Properties p) {
        properties = p;

    }

    public void setProducer(boolean producer) {
        isProducer = producer;
    }

    /**
     * Get the set of properties for this MSCLient.
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     * Initialize any state for this MSCLient.
     * Called once per MSCLient instance; there is one MSCLient instance per client thread.
     */
    public void init(ArrayList<String> streams) throws MSException {
    }

    /**
     * Send a record to the Message System.
     *
     * @param msg The message to be sent
     * @return
     */
    public abstract void send(boolean isSync,byte[] msg,String stream,WriteCallBack sentCallBack);

    /**
     * read messages from the Message System.
     *
     * @param
     * @return
     */
    public abstract void read(String stream, ReadCallBack readCallBack);

    /**
     * close the Message System.
     *
     * @param
     * @return
     */
    public abstract void close();
}
