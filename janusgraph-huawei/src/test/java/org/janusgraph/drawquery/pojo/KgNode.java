/**
 * 
 */
package org.janusgraph.drawquery.pojo;

import lombok.Data;

/**
 * <p>
 * <b>KgNode</b> 是
 * </p>
 *
 * @since 2020年11月4日
 * @author czhcc
 * @version $Id$
 *
 */
@Data
public class KgNode
{
	private String key;
	
	private String text;
	
	private String type;

	private String uri;
}
