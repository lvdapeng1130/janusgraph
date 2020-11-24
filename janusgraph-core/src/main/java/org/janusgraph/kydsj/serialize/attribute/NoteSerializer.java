package org.janusgraph.kydsj.serialize.attribute;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.database.serialize.SerializerInjected;
import org.janusgraph.kydsj.serialize.Note;

import java.util.Set;

/**
 * @author: ldp
 * @time: 2020/7/28 16:01
 * @jira:
 */
public class NoteSerializer implements AttributeSerializer<Note>, SerializerInjected {
    private Serializer serializer;

    @Override
    public Note read(ScanBuffer buffer) {
        String id=serializer.readObjectNotNull(buffer,String.class);
        String noteTitle=(String)serializer.readClassAndObject(buffer);
        String linkType=(String)serializer.readClassAndObject(buffer);
        String noteData=(String)serializer.readClassAndObject(buffer);
        Set<String> dsr=(Set<String>)serializer.readClassAndObject(buffer);
        Note note=new Note();
        note.setId(id);
        note.setNoteTitle(noteTitle);
        note.setLinkType(linkType);
        note.setNoteData(noteData);
        note.setDsr(dsr);
        return note;
    }

    @Override
    public void write(WriteBuffer buffer, Note attribute) {
        DataOutput out = (DataOutput)buffer;
        out.writeObjectNotNull(attribute.getId());
        out.writeClassAndObject(attribute.getNoteTitle());
        out.writeClassAndObject(attribute.getLinkType());
        out.writeClassAndObject(attribute.getNoteData());
        out.writeClassAndObject(attribute.getDsr());
    }

    @Override
    public void setSerializer(Serializer serializer) {
        this.serializer = Preconditions.checkNotNull(serializer);
    }
}
