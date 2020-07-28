package org.janusgraph.kydsj.serialize.attribute;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.SerializerInjected;
import org.janusgraph.kydsj.serialize.Note;

/**
 * @author: ldp
 * @time: 2020/7/28 16:01
 * @jira:
 */
public class NoteSerializer implements AttributeSerializer<Note>, SerializerInjected {
    private Serializer serializer;

    @Override
    public Note read(ScanBuffer buffer) {
        Note note = serializer.readObjectNotNull(buffer, Note.class);
        return note;
    }

    @Override
    public void write(WriteBuffer buffer, Note attribute) {
        DataOutput out = (DataOutput)buffer;
        out.writeObjectNotNull(attribute);
    }

    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer = Preconditions.checkNotNull(serializer);
    }
}
