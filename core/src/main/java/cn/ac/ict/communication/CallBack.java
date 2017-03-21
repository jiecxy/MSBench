package cn.ac.ict.communication;

import cn.ac.ict.stat.StatHeader;
import cn.ac.ict.stat.StatTail;
import cn.ac.ict.stat.StatWindow;

public interface CallBack {

    public void onSendStatHeader(StatHeader header);
    public void onSendStatWindow(StatWindow window);
    public void onSendStatTail(StatTail tail);
}
