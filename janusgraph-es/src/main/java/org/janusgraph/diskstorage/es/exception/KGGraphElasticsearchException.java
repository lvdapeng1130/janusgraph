package org.janusgraph.diskstorage.es.exception;

import java.io.IOException;

/**
 * @author: ldp
 * @time: 2020/12/31 17:35
 * @jira:
 */
public class KGGraphElasticsearchException extends IOException {

    public KGGraphElasticsearchException() {
        super();
    }

    public KGGraphElasticsearchException(String message) {
        super(message);
    }

    public KGGraphElasticsearchException(String message, Throwable cause) {
        super(message, cause);
    }

    public KGGraphElasticsearchException(Throwable cause) {
        super(cause);
    }
}
