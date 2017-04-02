package cn.ac.ict.msbench.communication;

import cn.ac.ict.msbench.stat.StatHeader;
import cn.ac.ict.msbench.stat.StatTail;
import cn.ac.ict.msbench.stat.StatWindow;

public interface CallBack {

    public void onSendStatHeader(StatHeader header);
    public void onSendStatWindow(StatWindow window);
    public void onSendStatTail(StatTail tail);
}
