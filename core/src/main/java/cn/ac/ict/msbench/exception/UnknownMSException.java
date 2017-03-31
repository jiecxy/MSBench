package cn.ac.ict.msbench.exception;

/**
 * Could not create the specified MS.
 */
public class UnknownMSException extends Exception {
  /**
   *
   */
  private static final long serialVersionUID = 459099842269616836L;

  public UnknownMSException(String message) {
    super(message);
  }

  public UnknownMSException() {
    super();
  }

  public UnknownMSException(String message, Throwable cause) {
    super(message, cause);
  }

  public UnknownMSException(Throwable cause) {
    super(cause);
  }

}
