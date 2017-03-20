package cn.ac.ict;

import java.util.ArrayList;
import java.util.Properties;

/**
 * Created by apple on 2017/3/20.
 */
public class MasterCom extends Communication {
    public MasterCom(MS ms, ArrayList<String> streams, int runTime, Properties props) {
        super(ms, streams, runTime, props);
    }
}
