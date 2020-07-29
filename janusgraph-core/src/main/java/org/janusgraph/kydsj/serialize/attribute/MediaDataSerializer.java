package org.janusgraph.kydsj.serialize.attribute;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.SerializerInjected;
import org.janusgraph.kydsj.serialize.MediaData;

import java.util.Set;

/**
 * @author: ldp
 * @time: 2020/7/28 16:01
 * @jira:
 */
public class MediaDataSerializer implements AttributeSerializer<MediaData>, SerializerInjected {

    private Serializer serializer;

    @Override
    public MediaData read(ScanBuffer buffer) {
        String key = serializer.readObjectNotNull(buffer, String.class);
        String aclid = (String)serializer.readClassAndObject(buffer);
        Set<String> dsr = (Set<String>)serializer.readClassAndObject(buffer);
        String fileName = (String)serializer.readClassAndObject(buffer);
        String linkType = (String)serializer.readClassAndObject(buffer);
        byte[] mediaData = (byte[])serializer.readClassAndObject(buffer);
        String mediaType = (String)serializer.readClassAndObject(buffer);
        String status = (String)serializer.readClassAndObject(buffer);
        String mediaTitle = (String)serializer.readClassAndObject(buffer);
        String mimeType = (String)serializer.readClassAndObject(buffer);
        MediaData media=new MediaData();
        media.setKey(key);
        media.setMediaTitle(mediaTitle);
        media.setMediaData(mediaData);
        media.setFilename(fileName);
        media.setAclId(aclid);
        media.setDsr(dsr);
        media.setLinkType(linkType);
        media.setMediaType(mediaType);
        media.setStatus(status);
        media.setMimeType(mimeType);
        return media;
    }

    @Override
    public void write(WriteBuffer buffer, MediaData attribute) {
        DataOutput out = (DataOutput)buffer;
        out.writeObjectNotNull(attribute.getKey());
        out.writeClassAndObject(attribute.getAclId());
        out.writeClassAndObject(attribute.getDsr());
        out.writeClassAndObject(attribute.getFilename());
        out.writeClassAndObject(attribute.getLinkType());
        out.writeClassAndObject(attribute.getMediaData());
        out.writeClassAndObject(attribute.getMediaType());
        out.writeClassAndObject(attribute.getStatus());
        out.writeClassAndObject(attribute.getMediaTitle());
        out.writeClassAndObject(attribute.getMimeType());
    }

    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer = Preconditions.checkNotNull(serializer);
    }
}
