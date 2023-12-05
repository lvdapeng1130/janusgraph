package org.janusgraph.kggraph.bo;

import lombok.Data;

import java.util.Map;

@Data
public class GraphVertex {
    /**********graphvis.js节点所需参数(http://www.graphvis.cn/article/6)**********/
    private String id;
    private String type;
    private String label;
    private Integer x;
    private Integer y;
    private String image;
    private Map<String,String> properties;
    /*******************自定义参数*****************/
    /************类型的中文名称*****************/
    private String typeName;
}
