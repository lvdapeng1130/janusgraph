/**
 * 
 */
package org.janusgraph.kydsj.serialize;

import com.google.common.collect.Sets;
import lombok.Data;
import org.janusgraph.graphdb.internal.InternalVertex;

import java.io.Serializable;
import java.util.Set;

/**
 * 顶点的多媒体类型
 */

@Data
public class MediaData implements Serializable
{
    private static final long serialVersionUID = 1226171500784936834L;
    private Set<String> dsr = Sets.newHashSet();
	
	private String mediaType;
	
	private String linkType;
	
	private String mimeType;
	
	private String filename;
	
	private String aclId;
	
	private String mediaTitle;
	
	private byte[] mediaData;
	
	private String key;

	private String status;

    private transient InternalVertex vertex;

}
