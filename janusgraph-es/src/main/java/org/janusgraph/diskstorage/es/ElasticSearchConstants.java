// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.es;

public class ElasticSearchConstants {

    public static final String ES_DOC_KEY = "doc";
    public static final String ES_UPSERT_KEY = "upsert";
    public static final String ES_SCRIPT_KEY = "script";
    public static final String ES_SOURCE_KEY = "source";
    public static final String ES_ID_KEY = "id";
    public static final String ES_PARAMS_KEY = "params";
    public static final String ES_PARAMS_FIELDS_KEY = "fields";
    public static final String ES_LANG_KEY = "lang";
    public static final String ES_TYPE_KEY = "type";
    public static final String ES_ANALYZER = "analyzer";
    public static final String ES_GEO_COORDS_KEY = "coordinates";
    public static final String CUSTOM_ALL_FIELD = "all";
    //lvdapeng 定义elasticsearch属性的内置字段
    public static final String KG_PROPERTY_VALUE = "value";
    public static final String KG_PROPERTY_VALUE_PARAM = "v";
    public static final String KG_CARDINALITY= "cardinality";
    public static final String KG_CARDINALITY_PARAM = "c";
    public static final String KG_PROPERTY_STARTDATE = "startDate";
    public static final String KG_PROPERTY_STARTDATE_PARAM = "s";
    public static final String KG_PROPERTY_ENDDATE = "endDate";
    public static final String KG_PROPERTY_ENDDATE_PARAM = "e";
    public static final String KG_PROPERTY_GEO = "geo";
    public static final String KG_PROPERTY_GEO_PARAM = "g";
    public static final String KG_PROPERTY_DSR = "dsr";
    public static final String KG_PROPERTY_DSR_PARAM = "d";
    public static final String KG_PROPERTY_ROLE = "role";
    public static final String KG_PROPERTY_ROLE_PARAM = "r";
    public static final String KG_PROPERTY_DS = "ds";
    public static final String KG_PROPERTY_DS_NAME = "name";
    public static final String KG_PROPERTY_OVERLAID = "overlaid";
    public static final String KG_PROPERTY_DS_NAME_PARAM = "n";
    public static final String KG_PROPERTY_DS_RECORD = "record";

    /**********************附件正文**********************/
    public static final String KG_MEDIASET_KEY = "key";
    public static final String KG_MEDIASET_TITLE = "title";
    public static final String KG_MEDIASET_CONTENT = "content";
    /**********************注释************************/
    public static final String KG_NOTE_KEY = "key";
    public static final String KG_NOTE_TITLE = "title";
    public static final String KG_NOTE_CONTENT = "content";
    public static final String KG_NOTE_USER = "user";
    public static final String KG_NOTE_UPDATEDATE = "updateDate";

}
