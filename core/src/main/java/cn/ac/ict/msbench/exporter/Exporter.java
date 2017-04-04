package cn.ac.ict.msbench.exporter;

import java.io.Closeable;
import java.io.IOException;

/**
 * Used to export the collected measurements into a useful format, for example
 * human readable text or machine readable JSON.
 */
public interface Exporter extends Closeable {

    // metrics: METRICS_HEAD  METRICS_WINDOW  METRICS_TAIL
    void write(String fileID, int metrics, String msg);
}
