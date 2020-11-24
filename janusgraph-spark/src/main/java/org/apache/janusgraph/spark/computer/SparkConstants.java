package org.apache.janusgraph.spark.computer;

/**
 * @author: ldp
 * @time: 2020/10/10 13:30
 * @jira:
 */
public final class SparkConstants {

    private SparkConstants(){

    }
    //设置要参与处理的对象类型
    public static final String COMBINE_CONDITION_OBJECT_TYPE = "janusgraph.combine.condition.object.type";
    //按照指定属性的值相同进行合并
    public static final String COMBINE_CONDITION_PROPERTY_TYPE="janusgraph.combine.condition.property.type";
    //在生成mapreduce的key时是否考虑对象类型
    public static final String COMBINE_CONDITION_THINKOVER_OBJECT_TYPE="janusgraph.combine.condition.thinkOver.object.type";
    //设置对象时合并属性时排除的属性类型，定义被合并对象的属性不被合并到合并对象上。
    public static final String COMBINE_ELIMINATE_PROPERTY_TYPE="janusgraph.combine.eliminate.property.type";
    //设置对象时合并关系时排除的关系类型，定义被合并对象的关系类型不被合并到合并对象上。
    public static final String COMBINE_ELIMINATE_LINK_TYPE="janusgraph.combine.eliminate.link.type";

}
