package org.janusgraph.graphdb.database.idassigner;

import org.janusgraph.diskstorage.IDAuthority;

import java.time.Duration;

public class NanoTimeIDPool implements IDPool{

    private final IDAuthority idAuthority;
    private final long idUpperBound; //exclusive
    private final int partition;
    private final int idNamespace;

    private final Duration renewTimeout;
    private final double renewBufferPercentage;

    public NanoTimeIDPool(IDAuthority idAuthority, int partition, int idNamespace, long idUpperBound, Duration renewTimeout, double renewBufferPercentage) {
        this.idAuthority = idAuthority;
        this.partition = partition;
        this.idNamespace = idNamespace;
        this.idUpperBound = idUpperBound;
        this.renewTimeout = renewTimeout;
        this.renewBufferPercentage = renewBufferPercentage;
    }

    @Override
    public String nextID() {
        return System.nanoTime()+"";
    }

    @Override
    public void close() {

    }
}
