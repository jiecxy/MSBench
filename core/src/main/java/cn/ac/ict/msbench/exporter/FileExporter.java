package cn.ac.ict.msbench.exporter;

import cn.ac.ict.msbench.communication.MasterCom;
import cn.ac.ict.msbench.communication.WorkerComInfo;
import cn.ac.ict.msbench.stat.StatWindow;
import cn.ac.ict.msbench.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static cn.ac.ict.msbench.communication.Command.METRICS_HEAD;
import static cn.ac.ict.msbench.communication.Command.METRICS_WINDOW;

/**
 * Write human readable text. Tries to emulate the previous print report method.
 */
public class FileExporter implements Exporter {

    private static final Logger log = LoggerFactory.getLogger(FileExporter.class);

    private File parentDir = null;
    private HashMap<String, FileInfo> files = new HashMap<>();

    private class FileInfo {
        public BufferedWriter bw;
        public Boolean writenWindowHeader;

        public FileInfo(BufferedWriter bw) {
            this.bw = bw;
            this.writenWindowHeader = false;
        }
    }
    // parent: /home/ms/msbench/data/master/2017-1-1_1-1-1_111/
    //         /home/ms/msbench/data/worker/
    public FileExporter(File root, boolean isMaster) {
        if (isMaster) {
            parentDir = new File(root, Utils.getTimeForDirName(System.currentTimeMillis()));
        } else {
            parentDir = root;
        }
        parentDir.mkdirs();
        log.debug("Created data directory: " + parentDir.getAbsolutePath());
    }

    public void write(String fileID, int metrics, String msg) {
        try {
            if (!files.containsKey(fileID)) {
                files.put(fileID, new FileInfo(createFile(fileID)));
            }
            if (METRICS_WINDOW == metrics && !files.get(fileID).writenWindowHeader) {
                files.get(fileID).bw.newLine();
                files.get(fileID).bw.write(StatWindow.printHead());
                files.get(fileID).writenWindowHeader = true;
            }
            files.get(fileID).bw.write(msg);
            files.get(fileID).bw.newLine();
        } catch (IOException e) {
            log.error("Fail to write data file with data: " +  msg);
            e.printStackTrace();
        }
    }

    private BufferedWriter createFile(String fileID) throws IOException {
        File file  = new File(parentDir, fileID + "." + Utils.getTimeForDirName(System.currentTimeMillis()));
        if (file.createNewFile()) {
            return new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
        }
        return null;
    }

    public void close() throws IOException {
        for (Map.Entry<String, FileInfo> entry : files.entrySet()) {
            entry.getValue().bw.close();
        }
    }
}
