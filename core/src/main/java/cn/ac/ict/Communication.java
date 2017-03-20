package cn.ac.ict;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by jiecxy on 2017/3/15.
 */
public class Communication {

    protected MS ms = null;
    protected ArrayList<String> streams = null;
    protected int runTime = 0;
    protected Properties props = null;

    public Communication(MS ms, ArrayList<String> streams, int runTime, Properties props) {
        this.ms = ms;
        this.streams = streams;
        this.runTime = runTime;
        this.props = props;
    }
}
