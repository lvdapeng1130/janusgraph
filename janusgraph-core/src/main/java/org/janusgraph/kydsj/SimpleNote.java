package org.janusgraph.kydsj;

import com.google.common.collect.HashMultimap;
import org.janusgraph.kydsj.serialize.Note;

/**
 * @author: ldp
 * @time: 2020/7/28 20:22
 * @jira:
 */
public class SimpleNote {
    private final HashMultimap<String, Note> notes= HashMultimap.create();

    public boolean add(String id,Note note) {
        notes.put(id,note);
        return true;
    }

    public boolean remove(String id,Note note) {
        notes.remove(id,note);
        return true;
    }

    public HashMultimap<String, Note> getNotes() {
        return notes;
    }

    public boolean isEmpty() {
        return notes.isEmpty();
    }

}
