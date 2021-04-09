package org.janusgraph.kydsj.serialize;


import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.graphdb.internal.AbstractElement;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * 注释类型
 */
public class Note extends AbstractElement implements JanusGraphElement, Serializable
{
    private static final long serialVersionUID = 2237743318377642502L;
    private String id;
    private Set<String> dsr=new HashSet<>();
    private String noteTitle;
    private String noteData;
    private String linkType;
    private transient InternalVertex vertex;

    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    public Set<String> getDsr() {
        return dsr;
    }

    public void setDsr(Set<String> dsr) {
        this.dsr = dsr;
    }

    public String getNoteTitle() {
        return noteTitle;
    }

    public void setNoteTitle(String noteTitle) {
        this.noteTitle = noteTitle;
    }

    public String getNoteData() {
        return noteData;
    }

    public void setNoteData(String noteData) {
        this.noteData = noteData;
    }

    public String getLinkType() {
        return linkType;
    }

    public void setLinkType(String linkType) {
        this.linkType = linkType;
    }

    public InternalVertex getVertex() {
        return vertex;
    }

    public void setVertex(InternalVertex vertex) {
        this.vertex = vertex;
    }

    public Note(String id) {
        super(id);
        this.id=id;
    }

    @Override
    public InternalElement it() {
        return vertex;
    }

    @Override
    public StandardJanusGraphTx tx() {
        return vertex.tx();
    }

    @Override
    public byte getLifeCycle() {
        return 0;
    }

    @Override
    public void remove() {
        tx().removeNote(this);
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(String... propertyKeys) {
        return null;
    }

    @Override
    public String label() {
        return noteTitle;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        return null;
    }

    @Override
    public <V> V valueOrNull(PropertyKey key) {
        return null;
    }

    @Override
    public String toString() {
        return  new ToStringBuilder( this, ToStringStyle.MULTI_LINE_STYLE)
            .append( "id", id)
            .append( "dsr", dsr)
            .append( "noteTitle", noteTitle)
            .append( "noteData", noteData)
            .append( "linkType", linkType)
            .toString();
    }
}
