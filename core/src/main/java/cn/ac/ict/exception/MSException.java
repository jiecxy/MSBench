package cn.ac.ict.exception;

/**
 * Created by jiecxy on 2017/3/1.
 */
public class MSException extends Exception {

    public MSException(String message) {
        super(message);
    }

    public MSException() {
        super();
    }

    public MSException(String message, Throwable cause) {
        super(message, cause);
    }

    public MSException(Throwable cause) {
        super(cause);
    }
}
