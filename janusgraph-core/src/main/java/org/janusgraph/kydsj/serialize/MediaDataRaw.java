/**
 * 
 */
package org.janusgraph.kydsj.serialize;

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
import java.util.Date;
import java.util.Iterator;

/**
 * 顶点的多媒体类型
 */

public class MediaDataRaw extends AbstractElement implements JanusGraphElement, Serializable
{
    public static final String  PREFIX_COL="RAW__";
    private static final long serialVersionUID = 9186714731047801226L;
    private String mediaType;
	private String linkType;
	private String mimeType;
	private String filename;
	private String mediaTitle;
	private String key;
	private String desc;
	private Date updateDate;
    private Integer sort;

    private transient InternalVertex vertex;

    public MediaDataRaw() {
        super(null);
    }

    public MediaDataRaw(String id) {
        super(id);
        this.key=id;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
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

    public String getKey() {
        return key;
    }

    public String cellName(){
        return PREFIX_COL+this.getKey();
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Date getUpdateDate() {
        return updateDate;
    }

    public void setUpdateDate(Date updateDate) {
        this.updateDate = updateDate;
    }

    public Integer getSort() {
        return sort;
    }

    public void setSort(Integer sort) {
        this.sort = sort;
    }

    public InternalVertex getVertex() {
        return vertex;
    }

    public void setVertex(InternalVertex vertex) {
        this.vertex = vertex;
    }

    public MediaData mediaData(){
        MediaData mediaData=new MediaData(this.key);
        mediaData.setVertex(this.vertex);
        mediaData.setFilename(this.getFilename());
        mediaData.setKey(this.getKey());
        mediaData.setLinkType(this.getLinkType());
        mediaData.setMediaTitle(this.getMediaTitle());
        mediaData.setMimeType(this.getMimeType());
        mediaData.setMediaType(this.getMediaType());
        mediaData.setDesc(this.getDesc());
        mediaData.setUpdateDate(this.getUpdateDate());
        mediaData.setSort(this.getSort());
        return mediaData;
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
        tx().removeAttachment(this.mediaData());
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
        return "MediaDataRaw{" +
            "mediaType='" + mediaType + '\'' +
            ", linkType='" + linkType + '\'' +
            ", mimeType='" + mimeType + '\'' +
            ", filename='" + filename + '\'' +
            ", mediaTitle='" + mediaTitle + '\'' +
            ", key='" + key + '\'' +
            ", desc='" + desc + '\'' +
            ", updateDate=" + updateDate +
            ", sort=" + sort +
            ", vertex=" + vertex +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        MediaDataRaw that = (MediaDataRaw) o;

        return new EqualsBuilder()
            .appendSuper(super.equals(o))
            .append(mediaType, that.mediaType)
            .append(linkType, that.linkType)
            .append(mimeType, that.mimeType)
            .append(filename, that.filename)
            .append(mediaTitle, that.mediaTitle)
            .append(key, that.key)
            .append(desc, that.desc)
            .append(updateDate, that.updateDate)
            .append(sort, that.sort)
            .append(vertex, that.vertex)
            .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
            .appendSuper(super.hashCode())
            .append(mediaType)
            .append(linkType)
            .append(mimeType)
            .append(filename)
            .append(mediaTitle)
            .append(key)
            .append(desc)
            .append(updateDate)
            .append(sort)
            .append(vertex)
            .toHashCode();
    }
}
