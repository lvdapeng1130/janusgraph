/**
 * 
 */
package org.janusgraph.drawquery.pojo;

import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;

/**
 * <p>
 * <b>KgPathGraph</b> 是
 * </p>
 *
 * @since 2020年11月13日
 * @author czhcc
 * @version $Id$
 *
 */
@Data
public class KgPathGraph
{
	private KgDiagram kgDiagram = new KgDiagram();
	
	private List<KgPath> kgPaths = Lists.newArrayList();
}
