package cn.ac.ict.utils;

import cn.ac.ict.communication.CallBack;
import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;

/**
 * Created by krumo on 3/24/17.
 */
public class SimpleCallBack implements CallBack {

    @Override
    public void onSendStatHeader(StatHeader header) {
        System.out.println("onSendStatHeader");
        return;
    }

    @Override
    public void onSendStatWindow(StatWindow window) {
        System.out.println("onSendStatWindow");
        return;
    }

    @Override
    public void onSendStatTail(StatTail tail) {
        System.out.println("onSendStatTail");
        return;
    }
}
