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
    public static final int WINDOW_WRITER = 2;
    public static final int WINDOW_READER = 3;
    public static final int TAIL_WRITER = 4;
    public static final int TAIL_READER = 4;

    void write(String fileID, int metrics, String msg);
}
