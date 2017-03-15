package cn.ac.ict.constants;

/**
 * Created by jiecxy on 2017/3/14.
 */
public class Constants {

    public static final String CONFIG_PRE = "-";

    public static final String SN = "sn";
    public static final String SN_DOC = "The number of streams.";

    public static final String NAME = "name";
    public static final String NAME_DOC = "The prefix name of stream.";

    public static final String W = "w";
    public static final String W_DOC = "The number of writer.";

    public static final String R = "r";
    public static final String R_DOC = "The number of reader.";

    public static final String FROM = "from";
    public static final String FROM_DOC = "The read strategy of reader. n: from offset n; 0: from oldest; -1: from latest; -2: each has different offset.";

    public static final String MS = "ms";
    public static final String MS_DOC = "Message Size (Byte).";
}
