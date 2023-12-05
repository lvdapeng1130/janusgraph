package org.janusgraph.kydsj.serialize.attribute;

import org.janusgraph.graphdb.database.idassigner.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.SerializerInjected;
import org.janusgraph.kydsj.serialize.MediaData;

import java.util.Date;
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
        Set<String> dsr = (Set<String>)serializer.readClassAndObject(buffer);
        String fileName = (String)serializer.readClassAndObject(buffer);
        String linkType = (String)serializer.readClassAndObject(buffer);
        byte[] mediaData = (byte[])serializer.readClassAndObject(buffer);
        String mediaType = (String)serializer.readClassAndObject(buffer);
        String desc = (String)serializer.readClassAndObject(buffer);
        String mediaTitle = (String)serializer.readClassAndObject(buffer);
        String mimeType = (String)serializer.readClassAndObject(buffer);
        Date updateDate = (Date)serializer.readClassAndObject(buffer);
        Integer sort = (Integer) serializer.readClassAndObject(buffer);
        MediaData media=new MediaData(key);
        media.setKey(key);
        media.setMediaTitle(mediaTitle);
        media.setMediaData(mediaData);
        media.setFilename(fileName);
        media.setDsr(dsr);
        media.setLinkType(linkType);
        media.setMediaType(mediaType);
        media.setDesc(desc);
        media.setMimeType(mimeType);
        media.setUpdateDate(updateDate);
        media.setSort(sort);
        if(buffer.hasRemaining()) {
            String text = (String) serializer.readClassAndObject(buffer);
            media.setText(text);
        }
        return media;
    }

    @Override
    public void write(WriteBuffer buffer, MediaData attribute) {
        DataOutput out = (DataOutput)buffer;
        out.writeObjectNotNull(attribute.getKey());
        out.writeClassAndObject(attribute.getDsr());
        out.writeClassAndObject(attribute.getFilename());
        out.writeClassAndObject(attribute.getLinkType());
        out.writeClassAndObject(attribute.getMediaData());
        out.writeClassAndObject(attribute.getMediaType());
        out.writeClassAndObject(attribute.getDesc());
        out.writeClassAndObject(attribute.getMediaTitle());
        out.writeClassAndObject(attribute.getMimeType());
        out.writeClassAndObject(attribute.getUpdateDate());
        out.writeClassAndObject(attribute.getSort());
        out.writeClassAndObject(attribute.getText());
    }

    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer = Preconditions.checkNotNull(serializer);
    }
}
