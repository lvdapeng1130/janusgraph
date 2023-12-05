package org.janusgraph.util.system;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.graphdb.types.ParameterType;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public enum DefaultFields {
    TITLE("title",new StandardKeyInformation(String.class,
        Cardinality.SINGLE, Mapping.TEXTSTRING.asParameter(),Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "title")
        )),
    LINK_TEXT("link_text",new StandardKeyInformation(String.class,
        Cardinality.SINGLE, Mapping.TEXTSTRING.asParameter(),Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "link_text")
    )),
    UPDATEDATE("updatedate",new StandardKeyInformation(Date.class,
        Cardinality.SINGLE,
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "updatedate")
    )),
    DOCTEXT("doctext",new StandardKeyInformation(String.class,
        Cardinality.SINGLE, Mapping.TEXTSTRING.asParameter(),Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "doctext")
    )),
    @Deprecated
    ATTACHMENT("attachment",new StandardKeyInformation(String.class,
        Cardinality.SINGLE, Mapping.TEXTSTRING.asParameter(),Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "attachment")
    )),
    MEDIASET("mediaset",new StandardKeyInformation(String.class,
        Cardinality.SET, Mapping.TEXTSTRING.asParameter(),Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "mediaset")
    )),
    NOTESET("noteset",new StandardKeyInformation(String.class,
        Cardinality.SINGLE, Mapping.TEXTSTRING.asParameter(),Parameter.of(ParameterType.TEXT_ANALYZER.getName(), "ik_max_word"),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "noteset")
    )),
    STARTDATE("startDate",new StandardKeyInformation(Date.class,
        Cardinality.SINGLE,
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "startDate")
    )),
    ENDDATE("endDate",new StandardKeyInformation(Date.class,
        Cardinality.SINGLE,
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "endDate")
    )),
    DSR("dsr",new StandardKeyInformation(String.class,
        Cardinality.SET, Mapping.TEXTSTRING.asParameter(),Parameter.of(ParameterType.MAPPED_NAME.getName(), "dsr")
    )),
    GEO("geo",new StandardKeyInformation(Geoshape.class,
        Cardinality.SINGLE,
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "geo")
    )),
    LINK_TYPE("link_type",new StandardKeyInformation(String.class,
        Cardinality.SINGLE, Mapping.STRING.asParameter(),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "link_type")
    )),
    LEFT_TID("left_tid",new StandardKeyInformation(String.class,
        Cardinality.SINGLE,  Mapping.STRING.asParameter(),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "left_tid")
    )),
    LEFT_TYPE("left_type",new StandardKeyInformation(String.class,
        Cardinality.SINGLE,  Mapping.STRING.asParameter(),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "left_type")
    )),
    RIGHT_TID("right_tid",new StandardKeyInformation(String.class,
        Cardinality.SINGLE,  Mapping.STRING.asParameter(),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "right_tid")
    )),
    RIGHT_TYPE("right_type",new StandardKeyInformation(String.class,
        Cardinality.SINGLE, Mapping.STRING.asParameter(),
        Parameter.of(ParameterType.MAPPED_NAME.getName(), "right_type")
    ));
    private static Set<String> defaultFields=new HashSet<>();
    static {
        DefaultFields[] values = DefaultFields.values();
        for(DefaultFields fields:values){
            defaultFields.add(fields.getName());
        }
    }
    private String name;
    private StandardKeyInformation keyInformation;

    public String getName() {
        return name;
    }

    DefaultFields(String name,StandardKeyInformation keyInformation) {
        this.name = name;
        this.keyInformation=keyInformation;
    }

    public StandardKeyInformation getKeyInformation() {
        return keyInformation;
    }

    public static boolean isDefaultField(String fieldName){
        return defaultFields.contains(fieldName);
    }
}
