/**
 * 
 */
package org.janusgraph.drawquery.pojo;

import lombok.Data;

/**
 * <p>
 * <b>KgLink</b> 是
 * </p>
 *
 * @since 2020年11月4日
 * @author czhcc
 * @version $Id$
 *
 */
@Data
public class KgLink
{
	private String key;
	
	private String from;
	
	private String to;
	
	private String text;
	
	private String type;
	
	private String role;

	private String uri;
}
