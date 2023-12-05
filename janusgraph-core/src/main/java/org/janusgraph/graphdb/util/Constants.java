package org.janusgraph.graphdb.util;

/**
 * @author lvdapeng
 * @create 2020-02-25 16:31
 */
public interface Constants {
    String ATTR_V_PREFIX="v_";
    String ATTR_E_PREFIX="e_";
    String SIMPLEDATEFORMATSTRING = "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||strict_date_time";
    String ATTR_NAMESPACE="namespace";
    String ATTR_INVESTIGATION="investigation";
    String ATTR_INDEXNAME="es.index";
    String INDEXNAME="indexName";
    String ATTR_INDEXTYPE="es.type";
    String ATTR_GRAPHNAEM="graphName";
    String ATTR_DOCUMNETID="es.id";

    String ATTR_CREATE_BY="create_by";

    String ATTR_CREATE_TIME="create_time";


    String FILE_PATH = "filePath";
    String FILE_NAME = "fileName";
    String CONTENT = "com.trs.property.content";
    //记录数据抽入到了的图库名称
    String INNER_TRSGRAPH_FIELD="com.trs.property.trsgraph";

    String OBJECT_TYPE = "object_type";

    String DOC_FILE_NAME = "com.trs.property.document.filename";
    String DOC_FILE_PATH = "com.trs.property.document.filepath";
    String FILE_SIZE = "com.trs.property.document.filesize";
    String EXTENSION = "com.trs.property.document.extension";
    String TYPE = "com.trs.property.document.type";
    String CREATOR = "com.trs.property.document.creator";
    String CREATE_TIME = "com.trs.property.document.createtime";
    String INTRINSIC_TITLE = "com.trs.property.IntrinsicTitle";
    String UNSPECIFIED = "com.trs.property.Unspecified";

    String DOMAIN_NAME = "com.trs.property.domain.name";
    String EMAIL_SUBJECT = "com.trs.property.EmailSubject";
    String EMAIL = "com.trs.property.Email";

    String TIMEINTERVAL = "com.trs.property.TimeInterval";

    String KEYWORD = "com.trs.property.Keyword";

    String LONGITUDE="com.trs.property.longitude";

    String LATITUDE="com.trs.property.latitude";

//	public final static String CONTENT = "com.trs.property.content";

    String EMAIL_FROM = "com.trs.property.SENDER";
    String EMAIL_TO = "com.trs.property.RCEIVER";
    String EMAIL_CC = "com.trs.property.CC";
    String EMAIL_BCC = "com.trs.property.BCC";
    String EMAIL_SEND_TIME = "com.trs.property.email.sendtime";
    //新增email属性
    String EMAIL_CONTENTTYPE = "com.trs.property.eml.contentType";//信息体读取类型
    String EMAIL_ACCEPTLANG = "com.trs.property.eml.acceptLang";//可支持的语言
    String EMAIL_CONTENTLANG = "com.trs.property.eml.contentLang";//信息体字符集
    String EMAIL_MIMEVERSION = "com.trs.property.eml.mimeVersion";//MIME版本号
    String EMAIL_ENCRYPTED = "com.trs.property.eml.encrypted";//是否加密
    String EMAIL_RECEIVED = "com.trs.property.eml.received";//邮件往来
    String EMAIL_RETPATH = "com.trs.property.eml.retPath";//发件人地址
    String EMAIL_INREPLYTO = "com.trs.property.eml.inReplyTo";//答复
    String EMAIL_MSGID = "com.trs.property.eml.msgId";//唯一标识
    String EMAIL_REFERENCES = "com.trs.property.eml.references";//相关邮件
    String EMAIL_REPLAYTO = "com.trs.property.eml.replayTo";//可选字段
    String EMAIL_XORIIP = "com.trs.property.eml.xoriip";//发送邮件的ip地址
    String EMAIL_HROWKEY = "com.trs.property.eml.hrowkey";//针对oz项目记录邮件数据hbase原始表的rowkey值。

    String LINK_EMAIL_REPLYTO = "com.trs.link.email.replyTo";//邮件回复

    String EMAIL_TYPE = "com.trs.object.EMAIL";

    String ORIGINAL_TID = "com.trs.property.original.tid";//tid根据md5加密的原始值
    String LINK_SIMPLE="com.trs.link.Simple";

    String ROLE_TO = "com.trs.role.to";
    String ROLE_FROM = "com.trs.role.from";
    String ROLE_NONE = "com.trs.role.none";

    String VERSION_RELATION_TABLENAME = "fmb_version_relation";

    String OBJECT_INNER_TIME_POSTFIX = "_inner_time";//放到flowfile里的对象内置属性的后缀名

    String INDX_SUFFIX = "_index";

    String ALIAS_POSTFIX="object";

    String STRING_DATA_TYPE = "string";
    String TEXT_DATA_TYPE = "text";
    String NUMBER_DATA_TYPE = "double";
    String DATE_DATA_TYPE = "date";

    String OBJECT_ABSTRACT = "com.trs.object.abstract";
    String PROPERTY_TID = "com.trs.property.tid";

    /*************************内置的一些uri********************/
    String LINK_RAW = "com.trs.link.raw";
    String MUGSHOT_URI = "com.trs.link.Mugshot";
    String SRC_URI = "com.trs.link.src";
    String SIMPLE_URI = "com.trs.link.Simple";
    String SINGLE_VALUE="single";
    String KG_ARCHIVE_ENTITY="ENTITY";
    String KG_ARCHIVE_LINK="LINK";
    String KG_ARCHIVE_DATASOURCE="DATASOURCE";
    String KG_ARCHIVE_SCHEMA="SCHEMA";
    String KG_SCHEMA_OBJECTTYPE="ObjectType";
    String KG_SCHEMA_PROPERTYTYPE="PropertyType";
    String KG_SCHEMA_LINKTYPE="LinkType";
    String KG_SCHEMA_ICONTYPE="IconType";
    String KG_SCHEMA_OBJECTPROPERTYMAPPING="ObjectPropertyMapping";
    String KG_SCHEMA_OBJECTOBJECTMAPPING="ObjectObjectMapping";


    /**************************本体URI前缀*******************************/
    //对象uri前缀
    String OBJECT_PREFIX = "com.trs.object.";
    //关系uri前缀
    String LINK_PREFIX = "com.trs.link.";
    //属性uri前缀
    String PROPERTY_PREFIX = "com.trs.property.";
    //关系表的表名名称前缀
    String FMB_SHORT_LINK_PREFIX = "fmb_links_";
    String SHORT_LINK_MIDDLE_PREFIX = "link_";

    /********************************hbase列族**************************/
    String F_OBJECT_V = "ov";
    String F_OBJECT_VC = "vc";
    String F_OBJECT = "object";
    String F_PROPERTIES = "properties";
    String F_MEDIASET = "mediasets";
    String F_NOTESETS = "notesets";
    String F_MERGE_FAMILY = "hb";
    String F_LINK = "link";
    String F_FILE = "file";
    String F_EMAIL_ATTA = "attachment"; //email 附件 结构 内容
    String F_EXTEND_PROPERTIES = "appproperty"; //扩展属性

    /********************************hbase一些内置列名称********************/
    /********对象表内置列**************/
    String C_OBJ_LABEL = "label";//对象的Title
    String C_PXML_NAME = "pxml_name";//PXML文件名
    String C_OBJ_ID = "tid";//PXML中对象的ID值
    String C_BEGIN_DATE = "begindate";//开始时间
    String C_END_DATE = "enddate";//结束时间
    String C_CREATE_DATE = "createdate";//数据的创建时间
    String C_UPDATE_DATE = "updatedate";//数据的修改时间
    String E_INNER_TIME = "@timestamp";//数据的修改时间
    String C_EMAIL_ATTA = "attachment"; //email 附件 结构 内容
    String C_NOTESETS = "notesets";
    String C_TYPE = "type";
    String C_CONTENT = "content";
    String C_BASETYPE = "basetype";
    String C_TEXT = "text";
    String C_DOC_BYTES = "doc_bytes";
    String C_HVC = "hvc";//数据版本号
    String C_MEDIASETS = "mediasets";
    String C_HB_FROM = "hb_from";
    String C_HB_TO = "hb_to";
    String C_NAME = "name";
    String C_FILE = "file";

    /********关系表内置列**************/
    String C_LEFT_BEGIN_DATE = "left_begin_date";
    String C_LEFT_END_DATE = "left_end_date";
    String C_RIGHT_BEGIN_DATE = "right_begin_date";
    String C_RIGHT_END_DATE = "right_end_date";
    String C_LINK_BEGIN_DATE = "link_begin_date";
    String C_LINK_END_DATE = "link_end_date";
    String C_LEFT_TID = "left_tid";
    String C_LEFT_TYPE = "left_type";
    String C_LINK_ROLE = "link_role";
    String C_LINK_TID = "link_tid";
    String C_LINK_TYPE = "link_type";
    String C_RIGHT_TID = "right_tid";
    String C_RIGHT_TYPE = "right_type";
    String C_LINK_TEXT="link_text";
    String C_DSR_ID = "dsr_id";

    String C_LEFT_TID_TO = "left_tid_to";
    String C_LEFT_TYPE_TO = "left_type_to";
    String C_RIGHT_TID_TO = "right_tid_to";
    String C_RIGHT_TYPE_TO = "right_type_to";
    /***********************字段常量**********************/

    String EMPTY_DATASOURCE_RECORD="trs_uuid";
    String EXTEND_PROPERTY_PREFIX="ep_";

    String ELASTICSEARCH_INNER_FIELD_LONLAT = "object_geo";
    String FIELD_DSR = "dsr";
    String FIELD_STA = "sta";
    String FIELD_VERSION = "hvc";
    String FIELD_LONGITUDE = "longitude";
    String FIELD_LATITUDE = "latitude";
    String FIELD_TID = "tid";
    String FIELD_DS = "ds";
    //记录关系抽到graph后的图库名称
    String FIELD_KG_GRAPHS = "kg_graphs";

    String LINK_TABLENAME = "tableName";
    //当一个属性的值得状态时将不会抽到elasticsearch
    String DELETE_STATE="D";
    //pim所在的postgresql所在的表名
    String T_PIM_CONFIG = "t_pim_config";
    //es默认分词
    String DEFAULT_ES_ANALYZER="ik_max_word";
    //邮件附件的表
    String H_ATTACHMENT="h_attachment";

    String GREMLIN_HADOOP_GRAPH_VERTEXLABELS = "gremlin.hadoop.vertexLabels";
    String GREMLIN_HADOOP_GRAPH_EDGELABELS = "gremlin.hadoop.edgeLabels";

    //hdfs上正文的文件名后缀
    String HDFS_DOCTEXT_SUFFIX=".zw";
    //hdfs上附件的文件名后缀
    String HDFS_MEDIA_SUFFIX=".fj";

    String HDFS_MEDIA_MEDIATYPE="hdfs/graph";


}
