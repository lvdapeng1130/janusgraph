/**
 * 
 */
package org.janusgraph.kydsj.serialize;

import com.google.common.collect.Sets;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
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
	
	private String mediaTitle;
	
	private byte[] mediaData;
	
	private String key;

	private String desc;

    private transient InternalVertex vertex;

    public MediaData(String id) {
        super(id);
        this.key=id;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
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

    public MediaDataRaw mediaDataRaw(){
        MediaDataRaw mediaDataRaw=new MediaDataRaw(this.key);
        mediaDataRaw.setVertex(this.vertex);
        mediaDataRaw.setFilename(this.getFilename());
        mediaDataRaw.setKey(this.getKey());
        mediaDataRaw.setLinkType(this.getLinkType());
        mediaDataRaw.setMediaTitle(this.getMediaTitle());
        mediaDataRaw.setMimeType(this.getMimeType());
        mediaDataRaw.setDesc(this.getDesc());
        return mediaDataRaw;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        MediaData mediaData1 = (MediaData) o;

        return new EqualsBuilder()
            .appendSuper(super.equals(o))
            .append(dsr, mediaData1.dsr)
            .append(mediaType, mediaData1.mediaType)
            .append(linkType, mediaData1.linkType)
            .append(mimeType, mediaData1.mimeType)
            .append(filename, mediaData1.filename)
            .append(mediaTitle, mediaData1.mediaTitle)
            .append(mediaData, mediaData1.mediaData)
            .append(key, mediaData1.key)
            .append(desc, mediaData1.desc)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .appendSuper(super.hashCode())
            .append(dsr)
            .append(mediaType)
            .append(linkType)
            .append(mimeType)
            .append(filename)
            .append(mediaTitle)
            .append(mediaData)
            .append(key)
            .append(desc)
            .toHashCode();
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
            .append( "mediaTitle", mediaTitle)
            .append( "mediaData", mediaData)
            .toString();
    }

}
