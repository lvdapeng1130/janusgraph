package org.janusgraph.hadoop.kerberos;

/**
 * @author: ldp
 * @time: 2020/12/30 9:43
 * @jira:
 */
public class KerberosException extends RuntimeException {

    private static final long serialVersionUID = 6919587430753536295L;

    public KerberosException() {
        super();
    }

    public KerberosException(String message) {
        super(message);
    }

    public KerberosException(String message, Throwable cause) {
        super(message, cause);
    }

    public KerberosException(Throwable cause) {
        super(cause);
    }

    protected KerberosException(String message, Throwable cause,
                               boolean enableSuppression,
                               boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
