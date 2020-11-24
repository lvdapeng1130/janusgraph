package org.janusgraph.kydsj;

import com.google.common.collect.HashMultimap;
import org.janusgraph.kydsj.serialize.MediaData;

/**
 * @author: ldp
 * @time: 2020/7/28 20:22
 * @jira:
 */
public class SimpleAddedAttachment {
    private final HashMultimap<Long, MediaData> attachments = HashMultimap.create();

    public boolean add(Long id,MediaData mediaData) {
        attachments.put(id,mediaData);
        return true;
    }

    public boolean remove(Long id,MediaData mediaData) {
        attachments.remove(id,mediaData);
        return true;
    }

    public HashMultimap<Long, MediaData> getAttachments() {
        return attachments;
    }

    public boolean isEmpty() {
        return attachments.isEmpty();
    }

}
