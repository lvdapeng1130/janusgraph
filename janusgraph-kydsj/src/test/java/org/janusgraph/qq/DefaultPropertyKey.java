/**
 * 
 */
package org.janusgraph.qq;

import com.google.common.collect.Sets;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Geoshape;

import java.util.Date;
import java.util.Set;

/**
 * <p>
 * <b>DefaultPropertyKey</b> 是
 * </p>
 *
 * @since 2020年8月4日
 * @author czhcc
 * @version $Id$
 *
 */
public enum DefaultPropertyKey 
{
	TID("tid", "对象ID"),
	CREATE_DATE("createdate", "创建日期", Date.class),
	UPDATE_DATE("updatedate", "更新日期", Date.class),
	LINK_TID("link_tid", "关系ID",String.class, Cardinality.SINGLE),
	LINK_TYPE("link_type", "关系类型",String.class, Cardinality.SINGLE),
	LINK_ROLE("link_role", "关系规则",String.class, Cardinality.SINGLE),
	LINK_TEXT("link_text", "关系标签",String.class, Cardinality.SINGLE),
	LEFT_TID("left_tid", "关系左边对象ID",String.class, Cardinality.SINGLE),
	LEFT_TYPE("left_type", "关系左边对象类型",String.class, Cardinality.SINGLE),
	RIGHT_TID("right_tid", "关系右边对象ID",String.class, Cardinality.SINGLE),
	RIGHT_TYPE("right_type", "关系右边对象类型",String.class, Cardinality.SINGLE),
	TITLE("title", "对象标签"),
	DOC_TEXT("doctext", "文档正文"),
	START_DATE("startDate", "开始日期", Date.class),
	END_DATE("endDate", "结束日期", Date.class),
	GEO("geo", "地理坐标", Geoshape.class, Cardinality.SINGLE),
	DSR("dsr", "数据来源", String.class, Cardinality.SET),
	ROLE("role", "数值规则"),
	STATUS("status", "对象状态", Integer.class),
    @Deprecated
	ATTACHMENT("attachment", "附件文本"),
	NOTESET("noteset", "注释"),
    MEDIASET("mediaset", "附件文本",String.class,Cardinality.SET),
	MERGE_TO("merge_to", "记录对象合并到的对象ID",Long.class, Cardinality.SINGLE)
	;
	
	private String key;
	
	private String name;
	
	private Class<?> type;
	
	private Cardinality cardinality;
	
	private DefaultPropertyKey(String key, String name)
	{
		this(key, name, String.class, Cardinality.SINGLE);
	}
	
	private DefaultPropertyKey(String key, String name, Class<?> type)
	{
		this(key, name, type, Cardinality.SINGLE);
	}
	
	private DefaultPropertyKey(String key, String name, Class<?> type, Cardinality cardinality)
	{
		this.key = key;
		this.name = name;
		this.type = type;
		this.cardinality = cardinality;
	}

	/**
	 * @return the key
	 */
	public String getKey()
	{
		return key;
	}

	/**
	 * @return the type
	 */
	public Class<?> getType()
	{
		return type;
	}

	/**
	 * @return the cardinality
	 */
	public Cardinality getCardinality()
	{
		return cardinality;
	}

	/**
	 * @return the name
	 */
	public String getName()
	{
		return name;
	}

	public final static Set<String> DEFAULT_KEYS = Sets.newHashSet(
			TID.getKey(),
			LINK_TID.getKey(),
			CREATE_DATE.getKey(),
			UPDATE_DATE.getKey(),
			LINK_ROLE.getKey(),
			LINK_TEXT.getKey(),
			TITLE.getKey(),
			DOC_TEXT.getKey(),
			START_DATE.getKey(),
			END_DATE.getKey(),
			GEO.getKey(),
			DSR.getKey(),
			ROLE.getKey(),
			STATUS.getKey(),
			ATTACHMENT.getKey(),
			MEDIASET.getKey(),
			NOTESET.getKey(),
			MERGE_TO.getKey()
			);
	
}
