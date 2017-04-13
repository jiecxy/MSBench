package cn.ac.ict.msbench.exporter;

import java.io.Closeable;
import java.io.IOException;

/**
 * Used to export the collected measurements into a useful format, for example
 * human readable text or machine readable JSON.
 */
public interface Exporter extends Closeable {

    // metrics
    public static final int HEAD = 1;
    public static final int WINDOW = 2;
    public static final int WINDOW_END_TO_END = 3;
    public static final int TAIL = 4;
    public static final int TAIL_END_TO_END = 4;

    void write(String fileID, int metrics, String msg);
}
