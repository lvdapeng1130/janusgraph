/**
 * 
 */
package org.janusgraph.drawquery.pojo;

import com.google.common.collect.Sets;
import lombok.Data;

import java.util.Set;

/**
 * <p>
 * <b>KgDiagram</b> 是
 * </p>
 *
 * @since 2020年11月4日
 * @author czhcc
 * @version $Id$
 *
 */
@Data
public class KgDiagram
{
	private Set<KgNode> nodeDataArray = Sets.newHashSet();
	
	private Set<KgLink> linkDataArray = Sets.newHashSet();
}
