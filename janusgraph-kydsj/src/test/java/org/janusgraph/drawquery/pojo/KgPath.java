/**
 * 
 */
package org.janusgraph.drawquery.pojo;

import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;

/**
 * <p>
 * <b>KgPath</b> 是
 * </p>
 *
 * @since 2020年11月13日
 * @author czhcc
 * @version $Id$
 *
 */
@Data
public class KgPath
{
	private List<String> labels = Lists.newArrayList();
	
	private List<String> links = Lists.newArrayList();
}
