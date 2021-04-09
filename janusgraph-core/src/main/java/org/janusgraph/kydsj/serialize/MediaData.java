/**
 * 
 */
package org.janusgraph.kydsj.serialize;

import com.google.common.collect.Sets;
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
import java.util.Iterator;
import java.util.Set;

/**
 * 顶点的多媒体类型
 */

public class MediaData extends AbstractElement implements JanusGraphElement, Serializable
{
    private static final long serialVersionUID = 1226171500784936834L;
    private Set<String> dsr = Sets.newHashSet();
	
	private String mediaType;
	
	private String linkType;
	
	private String mimeType;
	
	private String filename;
	
	private String aclId;
	
	private String mediaTitle;
	
	private byte[] mediaData;
	
	private String key;

	private String status;

    private transient InternalVertex vertex;

    public MediaData(String id) {
        super(id);
        this.key=id;
    }

    public Set<String> getDsr() {
        return dsr;
    }

    public void setDsr(Set<String> dsr) {
        this.dsr = dsr;
    }

    public String getMediaType() {
        return mediaType;
    }

    public void setMediaType(String mediaType) {
        this.mediaType = mediaType;
    }

    public String getLinkType() {
        return linkType;
    }

    public void setLinkType(String linkType) {
        this.linkType = linkType;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getAclId() {
        return aclId;
    }

    public void setAclId(String aclId) {
        this.aclId = aclId;
    }

    public String getMediaTitle() {
        return mediaTitle;
    }

    public void setMediaTitle(String mediaTitle) {
        this.mediaTitle = mediaTitle;
    }

    public byte[] getMediaData() {
        return mediaData;
    }

    public void setMediaData(byte[] mediaData) {
        this.mediaData = mediaData;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public InternalVertex getVertex() {
        return vertex;
    }

    public void setVertex(InternalVertex vertex) {
        this.vertex = vertex;
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
        tx().removeAttachment(this);
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(String... propertyKeys) {
        return null;
    }

    @Override
    public String label() {
        return mediaTitle;
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
            .append( "key", key)
            .append( "dsr", dsr)
            .append( "mediaType", mediaType)
            .append( "linkType", linkType)
            .append( "mimeType", mimeType)
            .append( "filename", filename)
            .append( "aclId", aclId)
            .append( "mediaTitle", mediaTitle)
            .append( "mediaData", mediaData)
            .append( "status", status)
            .toString();
    }

}
