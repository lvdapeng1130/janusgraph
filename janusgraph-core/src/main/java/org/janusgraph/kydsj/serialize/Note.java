package org.janusgraph.kydsj.serialize;


import lombok.Data;
import org.janusgraph.graphdb.internal.InternalVertex;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * 注释类型
 */
@Data
public class Note implements Serializable
{
    private static final long serialVersionUID = 2237743318377642502L;
    private String id;
    private Set<String> dsr=new HashSet<>();
    private String noteTitle;
    private String noteData;
    private String linkType;
    private transient InternalVertex vertex;
}
