package org.janusgraph.kydsj.serialize.attribute;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.SerializerInjected;
import org.janusgraph.kydsj.serialize.MediaDataRaw;

/**
 * @author: ldp
 * @time: 2020/7/28 16:01
 * @jira:
 */
public class MediaDataRawSerializer implements AttributeSerializer<MediaDataRaw>, SerializerInjected {

    private Serializer serializer;

    @Override
    public MediaDataRaw read(ScanBuffer buffer) {
        String key = serializer.readObjectNotNull(buffer, String.class);
        String fileName = (String)serializer.readClassAndObject(buffer);
        String linkType = (String)serializer.readClassAndObject(buffer);
        String mediaType = (String)serializer.readClassAndObject(buffer);
        String mediaTitle = (String)serializer.readClassAndObject(buffer);
        String mimeType = (String)serializer.readClassAndObject(buffer);
        MediaDataRaw media=new MediaDataRaw(key);
        media.setKey(key);
        media.setMediaTitle(mediaTitle);
        media.setFilename(fileName);
        media.setLinkType(linkType);
        media.setMediaType(mediaType);
        media.setMimeType(mimeType);
        return media;
    }

    @Override
    public void write(WriteBuffer buffer, MediaDataRaw attribute) {
        DataOutput out = (DataOutput)buffer;
        out.writeObjectNotNull(attribute.getKey());
        out.writeClassAndObject(attribute.getFilename());
        out.writeClassAndObject(attribute.getLinkType());
        out.writeClassAndObject(attribute.getMediaType());
        out.writeClassAndObject(attribute.getMediaTitle());
        out.writeClassAndObject(attribute.getMimeType());
    }

    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer = Preconditions.checkNotNull(serializer);
    }
}
