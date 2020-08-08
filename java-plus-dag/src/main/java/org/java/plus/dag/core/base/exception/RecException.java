package org.java.plus.dag.core.base.exception;

/**
 * @author seven.wxy
 * @date 2018/10/10
 */
public class RecException extends RuntimeException {
    private static final long serialVersionUID = 2273455898312444488L;
    protected int code;
    protected String message;
    protected Throwable exception;

    public RecException(int code) {
        this.code = code;
    }

    public RecException(int code, Throwable exception) {
        this(code);
        this.exception = exception;
    }

    public RecException(int code, String message) {
        this(code);
        this.message = message;
    }

    public RecException(int code, String message, Throwable exception) {
        this(code, message);
        this.exception = exception;
    }

    public RecException(StatusType statusType, Throwable exp) {
        this(statusType.getStatus(), statusType.getMsg());
        this.exception = exp;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @Override
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("RecException [code=");
        builder.append(code);
        builder.append(", message=");
        builder.append(message);
        builder.append(", exception=");
        builder.append(exception);
        builder.append("]");
        return builder.toString();
    }

}
