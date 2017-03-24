package cn.ac.ict;

import cn.ac.ict.exception.MSException;

import java.util.Properties;

/**
 * Created by jiecxy on 2017/3/1.
 */
public abstract class MS {

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
    public void init() throws MSException {
    }

    /**
     * Send a record to the Message System.
     *
     * @param msg The message to be sent
     * @return
     */
    public abstract Status send(byte[] msg,String stream);

    /**
     * read messages from the Message System.
     *
     * @param
     * @return
     */
    public abstract Status read(String stream);

    /**
     * close the Message System.
     *
     * @param
     * @return
     */
    public abstract Status close();
}
