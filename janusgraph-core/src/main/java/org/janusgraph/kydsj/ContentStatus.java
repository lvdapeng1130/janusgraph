package org.janusgraph.kydsj;

import lombok.Data;

import java.io.Serializable;

@Data
public class ContentStatus implements Serializable {
    private String path;
    private String hdfsPath;
    private String name;
    private long length;
    private boolean isdir;
    private short block_replication;
    private long blocksize;
    private long modification_time;
    private long access_time;
    private String owner;
    private String group;
}
