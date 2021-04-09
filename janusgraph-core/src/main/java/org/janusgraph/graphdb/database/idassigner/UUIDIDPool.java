package org.janusgraph.graphdb.database.idassigner;

import java.util.UUID;

/**
 * @author: ldp
 * @time: 2021/4/1 14:21
 * @jira:
 */
public class UUIDIDPool implements IDPool{

    public UUIDIDPool(){
    }
    @Override
    public String nextID() {
        String uuidstr = UUID.randomUUID().toString();
        String uuid = uuidstr.replaceAll("-", "");
        return uuid;
    }

    @Override
    public void close() {

    }
}
