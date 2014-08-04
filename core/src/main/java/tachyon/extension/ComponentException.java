package tachyon.extension;


/**
 * General Exception class for Tachyon extensions.
 */
public class ComponentException extends Exception {

  private static final long serialVersionUID = 1L;

  public ComponentException(String msg) {
    super(msg);
  }

  public ComponentException(Exception e) {
    super(e);
  }
}