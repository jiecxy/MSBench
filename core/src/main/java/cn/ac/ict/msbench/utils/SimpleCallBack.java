package cn.ac.ict.msbench.utils;

import cn.ac.ict.msbench.communication.CallBack;
import cn.ac.ict.msbench.stat.StatHeader;
import cn.ac.ict.msbench.stat.StatTail;
import cn.ac.ict.msbench.stat.StatWindow;

/**
 * Created by krumo on 3/24/17.
 */
public class SimpleCallBack implements CallBack {

    @Override
    public void onSendStatHeader(StatHeader header) {
        System.out.println("onSendStatHeader "+header);
        return;
    }

    @Override
    public void onSendStatWindow(StatWindow window) {
        System.out.println("onSendStatWindow "+window);
        return;
    }

    @Override
    public void onSendStatTail(StatTail tail) {
        System.out.println("onSendStatTail "+tail);
        return;
    }
}
