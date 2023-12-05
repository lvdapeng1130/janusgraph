package org.janusgraph.graphdb.relations;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.kydsj.ContentStatus;

import java.nio.charset.StandardCharsets;

import static org.janusgraph.graphdb.util.Constants.HDFS_DOCTEXT_SUFFIX;

public class ContentCacheVertexProperty extends CacheVertexProperty{
    private String largeContentData;
    private ContentStatus status;
    public ContentCacheVertexProperty(String id, PropertyKey key, InternalVertex start, Object value, Entry data) {
        super(id, key, start, value, data);
        String hdfsFileName = this.getVertex(0).id().toString() + HDFS_DOCTEXT_SUFFIX;
        if(hdfsFileName.equals(value)) {
            ContentStatus contentStatus = this.getVertex(0).getContentStatus(hdfsFileName);
            this.status=contentStatus;
        }
    }
    @Override
    public Object value() {
        Object value = super.value();
        if(largeContentData!=null){
            return largeContentData;
        }else{
            String hdfsFileName = this.getVertex(0).id().toString() + HDFS_DOCTEXT_SUFFIX;
            if(hdfsFileName.equals(value)) {
                byte[] largeCellContent = this.getVertex(0).getLargeCellContent(hdfsFileName);
                if(largeCellContent!=null){
                    largeContentData=new String(largeCellContent, StandardCharsets.UTF_8);
                    return largeContentData;
                }
            }
        }
        return value;
    }

    public Object oldValue(){
        Object value = super.value();
        return value;
    }

    public ContentStatus getContentStatus(){
        return status;
    }
}
