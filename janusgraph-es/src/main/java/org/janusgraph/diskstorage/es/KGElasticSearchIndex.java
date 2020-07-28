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

import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.core.attribute.Geo;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.Text;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.es.compat.AbstractESCompat;
import org.janusgraph.diskstorage.es.compat.ESCompatUtils;
import org.janusgraph.diskstorage.es.mapping.IndexMapping;
import org.janusgraph.diskstorage.es.script.ESScriptResponse;
import org.janusgraph.diskstorage.indexing.*;
import org.janusgraph.diskstorage.util.DefaultTransaction;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.graphdb.database.serialize.AttributeUtils;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.janusgraph.graphdb.query.condition.*;
import org.janusgraph.graphdb.types.ParameterType;
import org.locationtech.spatial4j.shape.Rectangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.janusgraph.diskstorage.es.ElasticSearchConstants.*;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_NAME;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@PreInitializeConfigOptions
public class KGElasticSearchIndex implements IndexProvider {

    private static final Logger log = LoggerFactory.getLogger(KGElasticSearchIndex.class);

    private static final String STRING_MAPPING_SUFFIX = "__STRING";

    public static final int HOST_PORT_DEFAULT = 9200;

    /**
     * Default tree_levels used when creating geo_shape mappings.
     */
    public static final int DEFAULT_GEO_MAX_LEVELS = 20;

    /**
     * Default distance_error_pct used when creating geo_shape mappings.
     */
    public static final double DEFAULT_GEO_DIST_ERROR_PCT = 0.025;

    private static final String PARAMETERIZED_DELETION_SCRIPT = parameterizedScriptPrepare("",
            "for (field in params.fields) {",
            "    if (field.cardinality == 'SINGLE') {",
            "        ctx._source.remove(field.name);",
            "    } else if (ctx._source.containsKey(field.name)) {",
            "        def fieldValueName = field.name+'.value';",
            "        def fieldIndex = ctx._source[fieldValueName].indexOf(field.value);",
            "        if (fieldIndex >= 0 && fieldIndex < ctx._source[field.name].size()) {",
            "            ctx._source[field.name].remove(fieldIndex);",
            "        }",
            "    }",
            "}");
    private static final String PARAMETERIZED_ADDITION_SCRIPT1 = parameterizedScriptPrepare("",
        "for (field in params.fields) {",
        "    def fieldValueName = field.name+'.value';",
        "    def startDateFieldName = field.name+'.startDate';",
        "    def endDateFieldName = field.name+'.endDate';",
        "    def roleFieldName = field.name+'.role';",
        "    def geoFieldName = field.name+'.geo';",
        "    def dsrFieldName = field.name+'.dsr';",
        "    if (ctx._source[fieldValueName] == null) {",
        "        ctx._source[fieldValueName] = [];",
        "    }",
        "    if (field.cardinality != 'SET' || ctx._source[fieldValueName].indexOf(field.value) == -1) {",
        "        ctx._source[fieldValueName].add(field.value);",
        "    }",
        "    ctx._source[startDateFieldName] = field.startDate;",
        "    ctx._source[endDateFieldName] = field.endDate;",
        "    ctx._source[roleFieldName] = field.role;",
        "    if (field.geo != null && field.geo.length == 2) {",
        "        ctx._source[geoFieldName] = field.geo;",
        "    }",
        "    if (ctx._source[dsrFieldName] == null) {",
        "        ctx._source[dsrFieldName] = [];",
        "    }",
        "    if (field.dsr != null && field.dsr.length > 0) {",
        "      for (dsrvalue in field.dsr) {",
        "         if (ctx._source[dsrFieldName].indexOf(dsrvalue) == -1) {",
        "             ctx._source[dsrFieldName].add(dsrvalue);",
        "         }",
        "      }",
        "    }",
        "}");
    private static final String PARAMETERIZED_ADDITION_SCRIPT = parameterizedScriptPrepare("",
        "for (field in params.fields){",
        "            def fieldValueArray = ctx._source.get(field.name);",
        "            boolean isExists=false;",
        "            if (fieldValueArray != null) {",
        "                Map existMap=null;",
        "                for (Map simple : fieldValueArray) {",
        "                    if (simple != null && simple.containsKey(\"value\")) {",
        "                        Object v=simple.get(\"value\");",
        "                        if(v!=null&&simple.get(\"value\").equals(field.value)){",
        "                            isExists=true;",
        "                            existMap=simple;",
        "                        }",
        "                    }",
        "                }",
        "                if(isExists&&existMap!=null){",
        "                    if (field.startDate != null) {",
        "                        existMap.put(\"startDate\", field.startDate);",
        "                    }",
        "                    if (field.endDate != null) {",
        "                        existMap.put(\"endDate\", field.endDate);",
        "                    }",
        "                    if (field.role != null) {",
        "                        existMap.put(\"role\", field.role);",
        "                    }",
        "                    if(field.geo!=null){",
        "                        existMap.put(\"geo\",field.geo);",
        "                    }",
        "                    if(field.dsr!=null&&field.dsr.size()>0) {",
        "                        Object dsrObjects = existMap.get(\"dsr\");",
        "                        if (dsrObjects != null) {",
        "                            if (dsrObjects instanceof List) {",
        "                                Set existsDsrs=new HashSet((List)dsrObjects);",
        "                                for(String dsr:field.dsr){",
        "                                    if(!existsDsrs.contains(dsr)){",
        "                                        ((List)dsrObjects).add(dsr);",
        "                                    }",
        "                                }",
        "                            } else {",
        "                                existMap.put(\"dsr\",field.dsr);",
        "                            }",
        "                        }else{",
        "                            existMap.put(\"dsr\",field.dsr);",
        "                        }",
        "                    }",
        "                }else{",
        "                    Map newValueMap=new HashMap();",
        "                    if(field.value!=null){",
        "                        newValueMap.put(\"value\",field.value);",
        "                    }",
        "                    if (field.startDate != null) {",
        "                        newValueMap.put(\"startDate\", field.startDate);",
        "                    }",
        "                    if (field.endDate != null) {",
        "                        newValueMap.put(\"endDate\", field.endDate);",
        "                    }" ,
        "                    if (field.role != null) {" ,
        "                        newValueMap.put(\"role\", field.role);" ,
        "                    }" ,
        "                    if(field.geo!=null){" ,
        "                        newValueMap.put(\"geo\",field.geo);" ,
        "                    }" ,
        "                    if(field.dsr!=null&&field.dsr.size()>0) {" ,
        "                        newValueMap.put(\"dsr\",field.dsr);" ,
        "                    }" ,
        "                    fieldValueArray.add(newValueMap);" ,
        "                }" ,
        "            }else{" ,
        "                ArrayList fieldValueList=new ArrayList();" ,
        "                Map newValueMap=new HashMap();" ,
        "                if(field.value!=null){" ,
        "                    newValueMap.put(\"value\",field.value);" ,
        "                }" ,
        "                if (field.startDate != null) {" ,
        "                    newValueMap.put(\"startDate\", field.startDate);" ,
        "                }" ,
        "                if (field.endDate != null) {" ,
        "                    newValueMap.put(\"endDate\", field.endDate);" ,
        "                }" ,
        "                if (field.role != null) {" ,
        "                    newValueMap.put(\"role\", field.role);" ,
        "                }" ,
        "                if(field.geo!=null){" ,
        "                    newValueMap.put(\"geo\",field.geo);" ,
        "                }" ,
        "                if(field.dsr!=null&&field.dsr.size()>0) {" ,
        "                    newValueMap.put(\"dsr\",field.dsr);" ,
        "                }" ,
        "                fieldValueList.add(newValueMap);" ,
        "                ctx._source.put(field.name,fieldValueList);" ,
        "            }",
        "        }"
    );

    static final String INDEX_NAME_SEPARATOR = "_";
    private static final String SCRIPT_ID_SEPARATOR = "-";

    private static final String MAX_OPEN_SCROLL_CONTEXT_PARAMETER = "search.max_open_scroll_context";
    private static final Map<String, Object> MAX_RESULT_WINDOW = ImmutableMap.of("index.max_result_window", Integer.MAX_VALUE);

    private static final Parameter[] NULL_PARAMETERS = null;

    private static final String TRACK_TOTAL_HITS_PARAMETER = "track_total_hits";
    private static final Parameter[] TRACK_TOTAL_HITS_DISABLED_PARAMETERS = new Parameter[]{new Parameter<>(TRACK_TOTAL_HITS_PARAMETER, false)};
    private static final Map<String, Object> TRACK_TOTAL_HITS_DISABLED_REQUEST_BODY = ImmutableMap.of(TRACK_TOTAL_HITS_PARAMETER, false);

    private final Function<String, String> generateIndexStoreNameFunction = this::generateIndexStoreName;
    private final Map<String, String> indexStoreNamesCache = new ConcurrentHashMap<>();
    private final boolean indexStoreNameCacheEnabled;

    private final AbstractESCompat compat;
    private final ElasticSearchClient client;
    private final String indexName;
    private final int batchSize;
    private final boolean useExternalMappings;
    private final boolean allowMappingUpdate;
    private final Map<String, Object> indexSetting;
    private final long createSleep;
    private final boolean useAllField;
    private final Map<String, Object> ingestPipelines;
    private final boolean useMappingForES7;
    private final String parameterizedAdditionScriptId;
    private final String parameterizedDeletionScriptId;

    public KGElasticSearchIndex(Configuration config) throws BackendException {

        indexName = config.get(INDEX_NAME);
        parameterizedAdditionScriptId = generateScriptId("add");
        parameterizedDeletionScriptId = generateScriptId("del");
        useAllField = config.get(ElasticSearchIndex.USE_ALL_FIELD);
        useExternalMappings = config.get(ElasticSearchIndex.USE_EXTERNAL_MAPPINGS);
        allowMappingUpdate = config.get(ElasticSearchIndex.ALLOW_MAPPING_UPDATE);
        createSleep = config.get(ElasticSearchIndex.CREATE_SLEEP);
        ingestPipelines = config.getSubset(ElasticSearchIndex.ES_INGEST_PIPELINES);
        useMappingForES7 = config.get(ElasticSearchIndex.USE_MAPPING_FOR_ES7);
        indexStoreNameCacheEnabled = config.get(ElasticSearchIndex.ENABLE_INDEX_STORE_NAMES_CACHE);
        batchSize = config.get(INDEX_MAX_RESULT_SET_SIZE);
        log.debug("Configured ES query nb result by query to {}", batchSize);

        client = interfaceConfiguration(config).getClient();

        checkClusterHealth(config.get(ElasticSearchIndex.HEALTH_REQUEST_TIMEOUT));

        compat = ESCompatUtils.acquireCompatForVersion(client.getMajorVersion());

        indexSetting = ElasticSearchSetup.getSettingsFromJanusGraphConf(config);

        setupMaxOpenScrollContextsIfNeeded(config);

        setupStoredScripts();
    }

    private void checkClusterHealth(String healthCheck) throws BackendException {
        try {
            client.clusterHealthRequest(healthCheck);
        } catch (final IOException e) {
            throw new PermanentBackendException(e.getMessage(), e);
        }
    }

    private void setupStoredScripts() throws PermanentBackendException {
        setupStoredScriptIfNeeded(parameterizedAdditionScriptId, PARAMETERIZED_ADDITION_SCRIPT);
        setupStoredScriptIfNeeded(parameterizedDeletionScriptId, PARAMETERIZED_DELETION_SCRIPT);
    }

    private void setupStoredScriptIfNeeded(String storedScriptId, String source) throws PermanentBackendException {

        ImmutableMap<String, Object> preparedScript = compat.prepareScript(source).build();

        String lang = (String) ((ImmutableMap<String, Object>) preparedScript.get(ES_SCRIPT_KEY)).get(ES_LANG_KEY);

        try {
            ESScriptResponse esScriptResponse = client.getStoredScript(storedScriptId);

            if(Boolean.FALSE.equals(esScriptResponse.getFound()) || !Objects.equals(lang, esScriptResponse.getScript().getLang()) ||
                !Objects.equals(source, esScriptResponse.getScript().getSource())){
                client.createStoredScript(storedScriptId, preparedScript);
            }

        } catch (final IOException e) {
            throw new PermanentBackendException(e.getMessage(), e);
        }
    }

    private void setupMaxOpenScrollContextsIfNeeded(Configuration config) throws PermanentBackendException {

        if(client.getMajorVersion().getValue() > 6){

            boolean setupMaxOpenScrollContexts;

            if(config.has(ElasticSearchIndex.SETUP_MAX_OPEN_SCROLL_CONTEXTS)){
                setupMaxOpenScrollContexts = config.get(ElasticSearchIndex.SETUP_MAX_OPEN_SCROLL_CONTEXTS);
            } else {
                setupMaxOpenScrollContexts = ElasticSearchIndex.SETUP_MAX_OPEN_SCROLL_CONTEXTS.getDefaultValue();
            }

            if(setupMaxOpenScrollContexts){

                Map<String, Object> settings = ImmutableMap.of("persistent",
                    ImmutableMap.of(MAX_OPEN_SCROLL_CONTEXT_PARAMETER, Integer.MAX_VALUE));

                try {
                    client.updateClusterSettings(settings);
                } catch (final IOException e) {
                    throw new PermanentBackendException(e.getMessage(), e);
                }
            }
        }
    }

    /**
     * If ES already contains this instance's target index, then do nothing.
     * Otherwise, create the index, then wait {@link ElasticSearchIndex#CREATE_SLEEP }.
     * <p>
     * The {@code client} field must point to a live, connected client.
     * The {@code indexName} field must be non-null and point to the name
     * of the index to check for existence or create.
     *
     * @param index index name
     * @throws IOException if the index status could not be checked or index could not be created
     */
    private void checkForOrCreateIndex(String index) throws IOException {
        Objects.requireNonNull(client);
        Objects.requireNonNull(index);

        // Create index if it does not useExternalMappings and if it does not already exist
        if (!useExternalMappings && !client.indexExists(index)) {
            client.createIndex(index, indexSetting);
            client.updateIndexSettings(index, MAX_RESULT_WINDOW);
            try {
                log.debug("Sleeping {} ms after {} index creation returned from actionGet()", createSleep, index);
                Thread.sleep(createSleep);
            } catch (final InterruptedException e) {
                throw new JanusGraphException("Interrupted while waiting for index to settle in", e);
            }
        }
        Preconditions.checkState(client.indexExists(index), "Could not create index: %s",index);
        client.addAlias(indexName, index);
    }


    /**
     * Configure ElasticSearchIndex's ES client. See{@link ElasticSearchSetup} for more
     * information.
     *
     * @param config a config passed to ElasticSearchIndex's constructor
     * @return a client object open and ready for use
     */
    private ElasticSearchSetup.Connection interfaceConfiguration(Configuration config) {
        final ElasticSearchSetup clientMode = ConfigOption.getEnumValue(config.get(ElasticSearchIndex.INTERFACE), ElasticSearchSetup.class);

        try {
            return clientMode.connect(config);
        } catch (final IOException e) {
            throw new JanusGraphException(e);
        }
    }

    private BackendException convert(Exception esException) {
        if (esException instanceof InterruptedException) {
            return new TemporaryBackendException("Interrupted while waiting for response", esException);
        } else {
            return new PermanentBackendException("Unknown exception while executing index operation", esException);
        }
    }

    /*private static String getDualMappingName(String key) {
        return key + STRING_MAPPING_SUFFIX;
    }*/

    private String generateScriptId(String uniqueScriptSuffix){
        return indexName + SCRIPT_ID_SEPARATOR + uniqueScriptSuffix;
    }

    private String generateIndexStoreName(String store){
        return indexName + INDEX_NAME_SEPARATOR + store.toLowerCase();
    }

    private String getIndexStoreName(String store) {

        if(indexStoreNameCacheEnabled){
            return indexStoreNamesCache.computeIfAbsent(store, generateIndexStoreNameFunction);
        }

        return generateIndexStoreName(store);
    }

    @Override
    public void register(String store, String key, KeyInformation information,
                         BaseTransaction tx) throws BackendException {
        final Class<?> dataType = information.getDataType();
        final Mapping map = Mapping.getMapping(information);
        Preconditions.checkArgument(map==Mapping.DEFAULT || AttributeUtils.isString(dataType) ||
                (map==Mapping.PREFIX_TREE && AttributeUtils.isGeo(dataType)),
                "Specified illegal mapping [%s] for data type [%s]",map,dataType);
        final String indexStoreName = getIndexStoreName(store);
        if (useExternalMappings) {
            try {
                //We check if the externalMapping have the property 'key'
                final IndexMapping mappings = client.getMapping(indexStoreName, store);
                if (mappings == null || (!mappings.isDynamic() && !mappings.getProperties().containsKey(key))) {
                    //Error if it is not dynamic and have not the property 'key'
                    throw new PermanentBackendException("The external mapping for index '"+ indexStoreName + "' and type '" + store + "' do not have property '" + key + "'");
                } else if (allowMappingUpdate && mappings.isDynamic()) {
                    //If it is dynamic, we push the unknown property 'key'
                    this.pushMapping(store, key, information);
                }
            } catch (final IOException e) {
                throw new PermanentBackendException(e);
            }
        } else {
            try {
                checkForOrCreateIndex(indexStoreName);
            } catch (final IOException e) {
                throw new PermanentBackendException(e);
            }
            this.pushMapping(store, key, information);
        }
    }

    /**
     * Push mapping to ElasticSearch
     * @param store the type in the index
     * @param key the name of the property in the index
     * @param information information of the key
     */
    private void pushMapping(String store, String key,
                               KeyInformation information) throws AssertionError, BackendException {
        final Class<?> dataType = information.getDataType();
        Mapping map = Mapping.getMapping(information);
        final Map<String,Object> properties = new HashMap<>();
        if (AttributeUtils.isString(dataType)) {
            if (map==Mapping.DEFAULT) map=Mapping.TEXT;
            log.debug("Registering string type for {} with mapping {}", key, map);
            final String stringAnalyzer
                = ParameterType.STRING_ANALYZER.findParameter(information.getParameters(), null);
            final String textAnalyzer = ParameterType.TEXT_ANALYZER.findParameter(information.getParameters(), null);
            // use keyword type for string mappings unless custom string analyzer is provided
            final Map<String,Object> stringMapping
                = stringAnalyzer == null ? compat.createKeywordMapping() : compat.createTextMapping(stringAnalyzer);
            switch (map) {
                case STRING:
                    //properties.put(key,this.constructKGField(information,stringMapping,dataType));
                    properties.put(key, this.constructKGField(information,compat.createTextMapping(textAnalyzer),dataType));
                    break;
                case TEXT:
                    properties.put(key, this.constructKGField(information,compat.createTextMapping(textAnalyzer),dataType));
                    break;
                case TEXTSTRING:
                    properties.put(key, this.constructKGField(information,compat.createTextMapping(textAnalyzer),dataType));
                    //properties.put(getDualMappingName(key), this.constructKGField(information,stringMapping,dataType));
                    break;
                default: throw new AssertionError("Unexpected mapping: "+map);
            }
        } else if (dataType == Float.class) {
            log.debug("Registering float type for {}", key);
            properties.put(key, this.constructKGField(information,ImmutableMap.of(ES_TYPE_KEY, "float"),dataType));
        } else if (dataType == Double.class) {
            log.debug("Registering double type for {}", key);
            properties.put(key, this.constructKGField(information,ImmutableMap.of(ES_TYPE_KEY, "double"),dataType));
        } else if (dataType == Byte.class) {
            log.debug("Registering byte type for {}", key);
            properties.put(key, this.constructKGField(information,ImmutableMap.of(ES_TYPE_KEY, "byte"),dataType));
        } else if (dataType == Short.class) {
            log.debug("Registering short type for {}", key);
            properties.put(key, this.constructKGField(information,ImmutableMap.of(ES_TYPE_KEY, "short"),dataType));
        } else if (dataType == Integer.class) {
            log.debug("Registering integer type for {}", key);
            properties.put(key, this.constructKGField(information,ImmutableMap.of(ES_TYPE_KEY, "integer"),dataType));
        } else if (dataType == Long.class) {
            log.debug("Registering long type for {}", key);
            properties.put(key, this.constructKGField(information,ImmutableMap.of(ES_TYPE_KEY, "long"),dataType));
        } else if (dataType == Boolean.class) {
            log.debug("Registering boolean type for {}", key);
            properties.put(key, this.constructKGField(information,ImmutableMap.of(ES_TYPE_KEY, "boolean"),dataType));
        } else if (dataType == Geoshape.class) {
            switch (map) {
                case PREFIX_TREE:
                    final int maxLevels = ParameterType.INDEX_GEO_MAX_LEVELS.findParameter(information.getParameters(),
                        DEFAULT_GEO_MAX_LEVELS);
                    final double distErrorPct
                        = ParameterType.INDEX_GEO_DIST_ERROR_PCT.findParameter(information.getParameters(),
                        DEFAULT_GEO_DIST_ERROR_PCT);
                    log.debug("Registering geo_shape type for {} with tree_levels={} and distance_error_pct={}", key,
                        maxLevels, distErrorPct);
                    properties.put(key, this.constructKGField(information,ImmutableMap.of(ES_TYPE_KEY, "geo_shape",
                        "tree", "quadtree",
                        "tree_levels", maxLevels,
                        "distance_error_pct", distErrorPct),dataType));
                    break;
                default:
                    log.debug("Registering geo_point type for {}", key);
                    properties.put(key, this.constructKGField(information,ImmutableMap.of(ES_TYPE_KEY, "geo_point"),dataType));
            }
        } else if (dataType == Date.class || dataType == Instant.class) {
            log.debug("Registering date type for {}", key);
            properties.put(key, this.constructKGField(information,ImmutableMap.of(ES_TYPE_KEY, "date"),dataType));
        } else if (dataType == UUID.class) {
            log.debug("Registering uuid type for {}", key);
            properties.put(key, this.constructKGField(information,compat.createKeywordMapping(),dataType));
        }
        if (useAllField) {
            // add custom all field mapping if it doesn't exist
            properties.put(ElasticSearchConstants.CUSTOM_ALL_FIELD, compat.createTextMapping(null));
        }
        final Map<String,Object> mapping = ImmutableMap.of("properties", properties);

        try {
            client.createMapping(getIndexStoreName(store), store, mapping);
        } catch (final Exception e) {
            throw convert(e);
        }
    }

    private Map<String,Object> constructKGField(KeyInformation information,Map<String,Object> propertyValueMapping,final Class<?> dataType){
        final Map<String, Object> mapping = new HashMap<>(propertyValueMapping);
        //自定义参数
        final List<Parameter> customParameters = ParameterType.getCustomParameters(information.getParameters());
        if (!customParameters.isEmpty()) {
            customParameters.forEach(p -> mapping.put(p.key(), p.value()));
        }
        if (useAllField) {
            if (dataType != Geoshape.class) {
                mapping.put("copy_to", ElasticSearchConstants.CUSTOM_ALL_FIELD);
            }
        }
        //"ignore_malformed": true
        if(!AttributeUtils.isString(information.getDataType())&&dataType !=Boolean.class){
            mapping.put("ignore_malformed",true);
        }
        Mapping map = Mapping.getMapping(information);
        if (dataType != Geoshape.class && map!=Mapping.PREFIX_TREE) {
            mapping.put("fields", ImmutableMap.of("keyword", ImmutableMap.of("type",
                "keyword", "ignore_above", 100)));
        }
        final Map<String,Object> innerFieldMapping = new HashMap<>();
        innerFieldMapping.put(KG_PROPERTY_DSR,compat.createKeywordMapping());
        innerFieldMapping.put(KG_PROPERTY_STARTDATE,ImmutableMap.of(ES_TYPE_KEY, "date"));
        innerFieldMapping.put(KG_PROPERTY_ENDDATE,ImmutableMap.of(ES_TYPE_KEY, "date"));
        innerFieldMapping.put(KG_PROPERTY_GEO,ImmutableMap.of(ES_TYPE_KEY, "geo_point"));
        innerFieldMapping.put(KG_PROPERTY_ROLE,compat.createKeywordMapping());
        innerFieldMapping.put(KG_PROPERTY_VALUE,mapping);
        return ImmutableMap.of("properties",innerFieldMapping);
    }

    private static Mapping getStringMapping(KeyInformation information) {
        assert AttributeUtils.isString(information.getDataType());
        Mapping map = Mapping.getMapping(information);
        if (map==Mapping.DEFAULT) map = Mapping.TEXT;
        return map;
    }

    private static boolean hasDualStringMapping(KeyInformation information) {
        return AttributeUtils.isString(information.getDataType()) && getStringMapping(information)==Mapping.TEXTSTRING;
    }

    public Map<String, Object> getNewDocument(final List<IndexEntry> additions,
                                              KeyInformation.StoreRetriever information) throws BackendException {
        // JSON writes duplicate fields one after another, which forces us
        // at this stage to make de-duplication on the IndexEntry list. We don't want to pay the
        // price map storage on the Mutation level because none of other backends need that.

        final Multimap<String, IndexEntry> unique = LinkedListMultimap.create();
        for (final IndexEntry e : additions) {
            unique.put(e.field, e);
        }

        final Map<String, Object> doc = new HashMap<>();
        for (final Map.Entry<String, Collection<IndexEntry>> add : unique.asMap().entrySet()) {
            final KeyInformation keyInformation = information.get(add.getKey());
            Collection<IndexEntry> indexEntries=add.getValue();
            switch (keyInformation.getCardinality()) {
                case SINGLE:
                    IndexEntry lastEntry=Iterators.getLast(indexEntries.iterator());
                    Object fValue = convertToEsType(lastEntry.value,
                            Mapping.getMapping(keyInformation));
                    Map<String,Object> fieldValues=new HashMap<>();
                    fieldValues.put(KG_PROPERTY_VALUE,fValue);
                    if(lastEntry.getStartDate()!=null) {
                        fieldValues.put(KG_PROPERTY_STARTDATE, lastEntry.getStartDate());
                    }
                    if(lastEntry.getEndDate()!=null) {
                        fieldValues.put(KG_PROPERTY_ENDDATE, lastEntry.getEndDate());
                    }
                    if(lastEntry.getGeo()!=null&&lastEntry.getGeo().length==2) {
                        fieldValues.put(KG_PROPERTY_GEO, lastEntry.getGeo());
                    }
                    if(lastEntry.getDsr()!=null&&lastEntry.getDsr().size()>0) {
                        fieldValues.put(KG_PROPERTY_DSR, lastEntry.getDsr().toArray(new String[lastEntry.getDsr().size()]));
                    }
                    if(StringUtils.isNotBlank(lastEntry.getRole())) {
                        fieldValues.put(KG_PROPERTY_ROLE, lastEntry.getRole().trim());
                    }
                    doc.put(add.getKey(), new Object[]{fieldValues});
                    break;
                case SET:
                case LIST:
                    Object[] fieldContent = indexEntries.stream().map(entry -> {
                        Object fieldValue = convertToEsType(entry.value, Mapping.getMapping(keyInformation));
                        Map<String, Object> fieValues = new HashMap<>();
                        fieValues.put(KG_PROPERTY_VALUE, fieldValue);
                        if (entry.getStartDate() != null) {
                            fieValues.put(KG_PROPERTY_STARTDATE, entry.getStartDate());
                        }
                        if (entry.getEndDate() != null) {
                            fieValues.put(KG_PROPERTY_ENDDATE, entry.getEndDate());
                        }
                        if (entry.getGeo() != null && entry.getGeo().length == 2) {
                            fieValues.put(KG_PROPERTY_GEO, entry.getGeo());
                        }
                        if (entry.getDsr() != null && entry.getDsr().size() > 0) {
                            fieValues.put(KG_PROPERTY_DSR, entry.getDsr().toArray(new String[entry.getDsr().size()]));
                        }
                        if (StringUtils.isNotBlank(entry.getRole())) {
                            fieValues.put(KG_PROPERTY_ROLE, entry.getRole().trim());
                        }
                        return fieValues;
                    }).filter(map -> {
                        Object fieldValue = map.get(KG_PROPERTY_VALUE);
                        if(fieldValue instanceof byte[]){
                            return false;
                        }else {
                            return true;
                        }
                    }).toArray();
                    doc.put(add.getKey(), fieldContent);
                    break;
                default:
                    break;
            }
            /*if (hasDualStringMapping(information.get(add.getKey())) && keyInformation.getDataType() == String.class) {
                doc.put(getDualMappingName(add.getKey()), fieldValues);
            }*/
        }
        return doc;
    }



    private static Object convertToEsType(Object value, Mapping mapping) {
        if (value instanceof Number) {
            if (AttributeUtils.isWholeNumber((Number) value)) {
                return ((Number) value).longValue();
            } else { //double or float
                return ((Number) value).doubleValue();
            }
        } else if (AttributeUtils.isString(value)) {
            return value;
        } else if (value instanceof Geoshape) {
            return convertGeoshape((Geoshape) value, mapping);
        } else if (value instanceof Date) {
            return value;
        } else if (value instanceof  Instant) {
            return Date.from((Instant) value);
        } else if (value instanceof Boolean) {
            return value;
        } else if (value instanceof UUID) {
            return value.toString();
        } else throw new IllegalArgumentException("Unsupported type: " + value.getClass() + " (value: " + value + ")");
    }

    @SuppressWarnings("unchecked")
    private static Object convertGeoshape(Geoshape geoshape, Mapping mapping) {
        if (geoshape.getType() == Geoshape.Type.POINT && Mapping.PREFIX_TREE != mapping) {
            final Geoshape.Point p = geoshape.getPoint();
            return new double[]{p.getLongitude(), p.getLatitude()};
        } else if (geoshape.getType() == Geoshape.Type.BOX) {
            final Rectangle box = geoshape.getShape().getBoundingBox();
            final Map<String,Object> map = new HashMap<>();
            map.put("type", "envelope");
            map.put("coordinates", new double[][] {{box.getMinX(),box.getMaxY()},{box.getMaxX(),box.getMinY()}});
            return map;
        } else if (geoshape.getType() == Geoshape.Type.CIRCLE) {
            try {
                final Map<String,Object> map = geoshape.toMap();
                map.put("radius", map.get("radius") + ((Map<String, String>) map.remove("properties")).get("radius_units"));
                return map;
            } catch (final IOException e) {
                throw new IllegalArgumentException("Invalid geoshape: " + geoshape, e);
            }
        } else {
            try {
                return geoshape.toMap();
            } catch (final IOException e) {
                throw new IllegalArgumentException("Invalid geoshape: " + geoshape, e);
            }
        }
    }

    /**
     * @param mutations <String, Map<String, IndexMutation>>类型其中第一个key是所有名称，第二个key是documentID
     * @throws BackendException
     */
    @Override
    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever information,
                       BaseTransaction tx) throws BackendException {
        final List<ElasticSearchMutation> requests = new ArrayList<>();
        try {
            for (final Map.Entry<String, Map<String, IndexMutation>> stores : mutations.entrySet()) {
                final List<ElasticSearchMutation> requestByStore = new ArrayList<>();
                final String storeName = stores.getKey();
                final String indexStoreName = getIndexStoreName(storeName);
                for (final Map.Entry<String, IndexMutation> entry : stores.getValue().entrySet()) {
                    final String documentId = entry.getKey();
                    final IndexMutation mutation = entry.getValue();
                    assert mutation.isConsolidated();
                    Preconditions.checkArgument(!(mutation.isNew() && mutation.isDeleted()));
                    Preconditions.checkArgument(!mutation.isNew() || !mutation.hasDeletions());
                    Preconditions.checkArgument(!mutation.isDeleted() || !mutation.hasAdditions());
                    //Deletions first
                    if (mutation.hasDeletions()) {
                        if (mutation.isDeleted()) {
                            log.trace("Deleting entire document {}", documentId);
                            requestByStore.add(ElasticSearchMutation.createDeleteRequest(indexStoreName, storeName,
                                    documentId));
                        } else {
                            List<Map<String, Object>> params = getParameters(information.get(storeName),
                                mutation.getDeletions(), true);
                            Map doc = compat.prepareStoredScript(parameterizedDeletionScriptId, params).build();
                            log.trace("Deletion script {} with params {}", PARAMETERIZED_DELETION_SCRIPT, params);
                            requestByStore.add(ElasticSearchMutation.createUpdateRequest(indexStoreName, storeName,
                                documentId, doc));
                        }
                    }
                    if (mutation.hasAdditions()) {
                        if (mutation.isNew()) { //Index
                            log.trace("Adding entire document {}", documentId);
                            final Map<String, Object> source = getNewDocument(mutation.getAdditions(),
                                    information.get(storeName));
                            requestByStore.add(ElasticSearchMutation.createIndexRequest(indexStoreName, storeName,
                                    documentId, source));
                        } else {
                            final Map upsert;
                            if (!mutation.hasDeletions()) {
                                upsert = getNewDocument(mutation.getAdditions(), information.get(storeName));
                            } else {
                                upsert = null;
                            }

                            List<Map<String, Object>> params = getParameters(information.get(storeName),
                                    mutation.getAdditions(), false, Cardinality.SINGLE);
                            if (!params.isEmpty()) {
                                ImmutableMap.Builder builder = compat.prepareStoredScript(parameterizedAdditionScriptId, params);
                                requestByStore.add(ElasticSearchMutation.createUpdateRequest(indexStoreName, storeName,
                                        documentId, builder, upsert));
                                log.trace("Adding script {} with params {}", PARAMETERIZED_ADDITION_SCRIPT, params);
                            }

                            final Map<String, Object> doc = getAdditionDoc(information, storeName, mutation);
                            if (!doc.isEmpty()) {
                                final ImmutableMap.Builder builder = ImmutableMap.builder().put(ES_DOC_KEY, doc);
                                requestByStore.add(ElasticSearchMutation.createUpdateRequest(indexStoreName, storeName,
                                        documentId, builder, upsert));
                                log.trace("Adding update {}", doc);
                            }
                        }
                    }
                }
                if (!requestByStore.isEmpty() && ingestPipelines.containsKey(storeName)) {
                    client.bulkRequest(requestByStore, String.valueOf(ingestPipelines.get(storeName)));
                } else if (!requestByStore.isEmpty()) {
                    requests.addAll(requestByStore);
                }
            }
            if (!requests.isEmpty()) {
                client.bulkRequest(requests, null);
            }
        } catch (final Exception e) {
            log.error("Failed to execute bulk Elasticsearch mutation", e);
            throw convert(e);
        }
    }

    private List<Map<String, Object>> getParameters(KeyInformation.StoreRetriever storeRetriever,
                                                    List<IndexEntry> entries,
                                                    boolean deletion,
                                                    Cardinality... cardinalitiesToSkip) {
        Set<Cardinality> cardinalityToSkipSet = Sets.newHashSet(cardinalitiesToSkip);
        List<Map<String, Object>> result = new ArrayList<>();
        for (IndexEntry entry : entries) {
            KeyInformation info = storeRetriever.get(entry.field);
            if (cardinalityToSkipSet.contains(info.getCardinality())) {
                continue;
            }
            Object jsValue = deletion && info.getCardinality() == Cardinality.SINGLE ?
                "" : convertToEsType(entry.value, Mapping.getMapping(info));
            Map<String, Object> params=new HashMap<>();
            params.put("name", entry.field);
            params.put("value", jsValue);
            params.put("cardinality", info.getCardinality().name());
            params.put(KG_PROPERTY_STARTDATE,entry.getStartDate());
            params.put(KG_PROPERTY_ENDDATE,entry.getEndDate());
            params.put(KG_PROPERTY_GEO, entry.getGeo());
            if(entry.getDsr()!=null&&entry.getDsr().size()>0) {
                params.put(KG_PROPERTY_DSR, entry.getDsr().toArray(new String[entry.getDsr().size()]));
            }else{
                params.put(KG_PROPERTY_DSR,null);
            }
            params.put(KG_PROPERTY_ROLE, entry.getRole());
            result.add(params);
            /*if (hasDualStringMapping(info)) {
                result.add(ImmutableMap.of("name", getDualMappingName(entry.field),
                        "value", jsValue,
                        "cardinality", info.getCardinality().name()));
            }*/
        }
        return result;
    }

    private Map<String,Object> getAdditionDoc(KeyInformation.IndexRetriever information,
                                              String store, IndexMutation mutation) throws PermanentBackendException {
        final Map<String,Object> doc = new HashMap<>();
        for (final IndexEntry e : mutation.getAdditions()) {
            final KeyInformation keyInformation = information.get(store).get(e.field);
            if (keyInformation.getCardinality() == Cardinality.SINGLE) {
                IndexEntry lastEntry=e;
                Object fValue = convertToEsType(lastEntry.value,
                    Mapping.getMapping(keyInformation));
                Map<String,Object> fieldValues=new HashMap<>();
                fieldValues.put(KG_PROPERTY_VALUE,fValue);
                if(lastEntry.getStartDate()!=null) {
                    fieldValues.put(KG_PROPERTY_STARTDATE, lastEntry.getStartDate());
                }
                if(lastEntry.getEndDate()!=null) {
                    fieldValues.put(KG_PROPERTY_ENDDATE, lastEntry.getEndDate());
                }
                if(lastEntry.getGeo()!=null&&lastEntry.getGeo().length==2) {
                    fieldValues.put(KG_PROPERTY_GEO, lastEntry.getGeo());
                }
                if(lastEntry.getDsr()!=null&&lastEntry.getDsr().size()>0) {
                    fieldValues.put(KG_PROPERTY_DSR, lastEntry.getDsr().toArray(new String[lastEntry.getDsr().size()]));
                }
                if(StringUtils.isNotBlank(lastEntry.getRole())) {
                    fieldValues.put(KG_PROPERTY_ROLE, lastEntry.getRole().trim());
                }
                doc.put(e.field, fieldValues);
            }
        }

        return doc;
    }

    @Override
    public void restore(Map<String,Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information,
                        BaseTransaction tx) throws BackendException {
        final List<ElasticSearchMutation> requests = new ArrayList<>();
        try {
            for (final Map.Entry<String, Map<String, List<IndexEntry>>> stores : documents.entrySet()) {
                final List<ElasticSearchMutation> requestByStore = new ArrayList<>();
                final String store = stores.getKey();
                final String indexStoreName = getIndexStoreName(store);
                for (final Map.Entry<String, List<IndexEntry>> entry : stores.getValue().entrySet()) {
                    final String docID = entry.getKey();
                    final List<IndexEntry> content = entry.getValue();
                    if (content == null || content.size() == 0) {
                        // delete
                        if (log.isTraceEnabled())
                            log.trace("Deleting entire document {}", docID);

                        requestByStore.add(ElasticSearchMutation.createDeleteRequest(indexStoreName, store, docID));
                    } else {
                        // Add
                        if (log.isTraceEnabled())
                            log.trace("Adding entire document {}", docID);
                        final Map<String, Object> source = getNewDocument(content, information.get(store));
                        requestByStore.add(ElasticSearchMutation.createIndexRequest(indexStoreName, store, docID,
                                source));
                    }
                }
                if (!requestByStore.isEmpty() && ingestPipelines.containsKey(store)) {
                    client.bulkRequest(requestByStore, String.valueOf(ingestPipelines.get(store)));
                } else if (!requestByStore.isEmpty()) {
                    requests.addAll(requestByStore);
                }
            }
            if (!requests.isEmpty())
                client.bulkRequest(requests, null);
        } catch (final Exception e) {
            throw convert(e);
        }
    }

    private Map<String, Object> getRelationFromCmp(final Cmp cmp, String key, final Object value) {
        switch (cmp) {
            case EQUAL:
                return compat.term(key, value);
            case NOT_EQUAL:
                return compat.boolMustNot(compat.term(key, value));
            case LESS_THAN:
                return compat.lt(key, value);
            case LESS_THAN_EQUAL:
                return compat.lte(key, value);
            case GREATER_THAN:
                return compat.gt(key, value);
            case GREATER_THAN_EQUAL:
                return compat.gte(key, value);
            default:
                throw new IllegalArgumentException("Unexpected relation: " + cmp);
        }
    }

    private String getQueryFieldName(String key){
        return key+"."+KG_PROPERTY_VALUE;
    }

    public Map<String,Object> getFilter(Condition<?> condition, KeyInformation.StoreRetriever information) {
        if (condition instanceof PredicateCondition) {
            final PredicateCondition<String, ?> atom = (PredicateCondition) condition;
            Object value = atom.getValue();
            final String key = atom.getKey();
            //queryField是在key的后面追加.value
            String queryField = this.getQueryFieldName(atom.getKey());
            final JanusGraphPredicate predicate = atom.getPredicate();
            if (value instanceof Number) {
                Preconditions.checkArgument(predicate instanceof Cmp,
                        "Relation not supported on numeric types: " + predicate);
                return getRelationFromCmp((Cmp) predicate, queryField, value);
            } else if (value instanceof String) {

                final Mapping mapping = getStringMapping(information.get(key));
                final String fieldName;
                if (mapping==Mapping.TEXT && !(Text.HAS_CONTAINS.contains(predicate) || predicate instanceof Cmp))
                    throw new IllegalArgumentException("Text mapped string values only support CONTAINS and Compare queries and not: " + predicate);
                if (mapping==Mapping.STRING && Text.HAS_CONTAINS.contains(predicate))
                    throw new IllegalArgumentException("String mapped string values do not support CONTAINS queries: " + predicate);
                if (mapping==Mapping.TEXTSTRING && !(Text.HAS_CONTAINS.contains(predicate) || (predicate instanceof Cmp && predicate != Cmp.EQUAL))) {
                    //fieldName =  this.getQueryFieldName(getDualMappingName(key));
                    fieldName =  queryField+".keyword";
                } else {
                    fieldName = queryField;
                }

                if (predicate == Text.CONTAINS || predicate == Cmp.EQUAL) {
                    return compat.match(fieldName, value);
                } else if (predicate == Text.CONTAINS_PREFIX) {
                    if (!ParameterType.TEXT_ANALYZER.hasParameter(information.get(key).getParameters()))
                        value = ((String) value).toLowerCase();
                    return compat.prefix(fieldName, value);
                } else if (predicate == Text.CONTAINS_REGEX) {
                    if (!ParameterType.TEXT_ANALYZER.hasParameter(information.get(key).getParameters()))
                        value = ((String) value).toLowerCase();
                    return compat.regexp(fieldName, value);
                } else if (predicate == Text.PREFIX) {
                    return compat.prefix(fieldName, value);
                } else if (predicate == Text.REGEX) {
                    return compat.regexp(fieldName, value);
                } else if (predicate == Cmp.NOT_EQUAL) {
                    return compat.boolMustNot(compat.match(fieldName, value));
                } else if (predicate == Text.FUZZY || predicate == Text.CONTAINS_FUZZY) {
                    return compat.fuzzyMatch(fieldName, value);
                } else if (predicate == Cmp.LESS_THAN) {
                    return compat.lt(fieldName, value);
                } else if (predicate == Cmp.LESS_THAN_EQUAL) {
                    return compat.lte(fieldName, value);
                } else if (predicate == Cmp.GREATER_THAN) {
                    return compat.gt(fieldName, value);
                } else if (predicate == Cmp.GREATER_THAN_EQUAL) {
                    return compat.gte(fieldName, value);
                } else
                    throw new IllegalArgumentException("Predicate is not supported for string value: " + predicate);
            } else if (value instanceof Geoshape && Mapping.getMapping(information.get(key)) == Mapping.DEFAULT) {
                // geopoint
                final Geoshape shape = (Geoshape) value;
                Preconditions.checkArgument(predicate instanceof Geo && predicate != Geo.CONTAINS,
                        "Relation not supported on geopoint types: " + predicate);

                final Map<String,Object> query;
                switch (shape.getType()) {
                    case CIRCLE:
                        final Geoshape.Point center = shape.getPoint();
                        query = compat.geoDistance(queryField, center.getLatitude(), center.getLongitude(), shape.getRadius());
                        break;
                    case BOX:
                        final Geoshape.Point southwest = shape.getPoint(0);
                        final Geoshape.Point northeast = shape.getPoint(1);
                        query = compat.geoBoundingBox(queryField, southwest.getLatitude(), southwest.getLongitude(),
                            northeast.getLatitude(), northeast.getLongitude());
                        break;
                    case POLYGON:
                        final List<List<Double>> points = IntStream.range(0, shape.size())
                            .mapToObj(i -> ImmutableList.of(shape.getPoint(i).getLongitude(),
                                shape.getPoint(i).getLatitude()))
                            .collect(Collectors.toList());
                        query = compat.geoPolygon(queryField, points);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported or invalid search shape type for geopoint: "
                            + shape.getType());
                }

                return predicate == Geo.DISJOINT ?  compat.boolMustNot(query) : query;
            } else if (value instanceof Geoshape) {
                Preconditions.checkArgument(predicate instanceof Geo,
                        "Relation not supported on geoshape types: " + predicate);
                final Geoshape shape = (Geoshape) value;
                final Map<String,Object> geo;
                switch (shape.getType()) {
                    case CIRCLE:
                        final Geoshape.Point center = shape.getPoint();
                        geo = ImmutableMap.of(ES_TYPE_KEY, "circle",
                            ES_GEO_COORDS_KEY, ImmutableList.of(center.getLongitude(), center.getLatitude()),
                            "radius", shape.getRadius() + "km");
                        break;
                    case BOX:
                        final Geoshape.Point southwest = shape.getPoint(0);
                        final Geoshape.Point northeast = shape.getPoint(1);
                        geo = ImmutableMap.of(ES_TYPE_KEY, "envelope",
                            ES_GEO_COORDS_KEY,
                            ImmutableList.of(
                                ImmutableList.of(southwest.getLongitude(), northeast.getLatitude()),
                                ImmutableList.of(northeast.getLongitude(), southwest.getLatitude())));
                        break;
                    case LINE:
                        final List lineCoords = IntStream.range(0, shape.size())
                            .mapToObj(i -> ImmutableList.of(shape.getPoint(i).getLongitude(),
                                    shape.getPoint(i).getLatitude()))
                            .collect(Collectors.toList());
                        geo = ImmutableMap.of(ES_TYPE_KEY, "linestring", ES_GEO_COORDS_KEY, lineCoords);
                        break;
                    case POLYGON:
                        final List polyCoords = IntStream.range(0, shape.size())
                            .mapToObj(i -> ImmutableList.of(shape.getPoint(i).getLongitude(),
                                    shape.getPoint(i).getLatitude()))
                            .collect(Collectors.toList());
                        geo = ImmutableMap.of(ES_TYPE_KEY, "polygon", ES_GEO_COORDS_KEY,
                                ImmutableList.of(polyCoords));
                        break;
                    case POINT:
                        geo = ImmutableMap.of(ES_TYPE_KEY, "point",
                            ES_GEO_COORDS_KEY, ImmutableList.of(shape.getPoint().getLongitude(),
                                    shape.getPoint().getLatitude()));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported or invalid search shape type: "
                                + shape.getType());
                }

                return compat.geoShape(queryField, geo, (Geo) predicate);
            } else if (value instanceof Date || value instanceof Instant) {
                Preconditions.checkArgument(predicate instanceof Cmp,
                        "Relation not supported on date types: " + predicate);

                if (value instanceof Instant) {
                    value = Date.from((Instant) value);
                }
                return getRelationFromCmp((Cmp) predicate, queryField, value);
            } else if (value instanceof Boolean) {
                final Cmp numRel = (Cmp) predicate;
                switch (numRel) {
                    case EQUAL:
                        return compat.term(queryField, value);
                    case NOT_EQUAL:
                        return compat.boolMustNot(compat.term(queryField, value));
                    default:
                        throw new IllegalArgumentException("Boolean types only support EQUAL or NOT_EQUAL");
                }
            } else if (value instanceof UUID) {
                if (predicate == Cmp.EQUAL) {
                    return compat.term(queryField, value);
                } else if (predicate == Cmp.NOT_EQUAL) {
                    return compat.boolMustNot(compat.term(queryField, value));
                } else {
                    throw new IllegalArgumentException("Only equal or not equal is supported for UUIDs: "
                            + predicate);
                }
            } else throw new IllegalArgumentException("Unsupported type: " + value);
        } else if (condition instanceof Not) {
            return compat.boolMustNot(getFilter(((Not) condition).getChild(),information));
        } else if (condition instanceof And) {
            final List queries = StreamSupport.stream(condition.getChildren().spliterator(), false)
                .map(c -> getFilter(c,information)).collect(Collectors.toList());
            return compat.boolMust(queries);
        } else if (condition instanceof Or) {
            final List queries = StreamSupport.stream(condition.getChildren().spliterator(), false)
                .map(c -> getFilter(c,information)).collect(Collectors.toList());
            return compat.boolShould(queries);
        } else throw new IllegalArgumentException("Invalid condition: " + condition);
    }

    @Override
    public Stream<String> query(IndexQuery query, KeyInformation.IndexRetriever informations,
                                BaseTransaction tx) throws BackendException {
        final ElasticSearchRequest sr = new ElasticSearchRequest();
        final Map<String,Object> esQuery = getFilter(query.getCondition(), informations.get(query.getStore()));
        sr.setQuery(compat.prepareQuery(esQuery));
        if (!query.getOrder().isEmpty()) {
            addOrderToQuery(informations, sr, query.getOrder(), query.getStore());
        }
        sr.setFrom(0);
        if (query.hasLimit()) {
            sr.setSize(Math.min(query.getLimit(), batchSize));
        } else {
            sr.setSize(batchSize);
        }

        sr.setDisableSourceRetrieval(true);

        ElasticSearchResponse response;
        try {
            final String indexStoreName = getIndexStoreName(query.getStore());
            final boolean useScroll = sr.getSize() >= batchSize;
            response = client.search(indexStoreName,
                compat.createRequestBody(sr, useScroll? NULL_PARAMETERS : TRACK_TOTAL_HITS_DISABLED_PARAMETERS),
                useScroll);
            log.debug("First Executed query [{}] in {} ms", query.getCondition(), response.getTook());
            final Iterator<RawQuery.Result<String>> resultIterator = getResultsIterator(useScroll, response, sr.getSize());
            final Stream<RawQuery.Result<String>> toReturn
                    = StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED), false);
            return (query.hasLimit() ? toReturn.limit(query.getLimit()) : toReturn).map(RawQuery.Result::getResult);
        } catch (final IOException | UncheckedIOException e) {
            throw new PermanentBackendException(e);
        }
    }

    private Iterator<RawQuery.Result<String>> getResultsIterator(boolean useScroll, ElasticSearchResponse response, int windowSize){
        return (useScroll)? new ElasticSearchScroll(client, response, windowSize) : response.getResults().iterator();
    }

    private String convertToEsDataType(Class<?> dataType, Mapping mapping) {
        if(String.class.isAssignableFrom(dataType)) {
            return "string";
        }
        else if (Integer.class.isAssignableFrom(dataType)) {
            return "integer";
        }
        else if (Long.class.isAssignableFrom(dataType)) {
            return "long";
        }
        else if (Float.class.isAssignableFrom(dataType)) {
            return "float";
        }
        else if (Double.class.isAssignableFrom(dataType)) {
            return "double";
        }
        else if (Boolean.class.isAssignableFrom(dataType)) {
            return "boolean";
        }
        else if (Date.class.isAssignableFrom(dataType)) {
            return "date";
        }
        else if (Instant.class.isAssignableFrom(dataType)) {
            return "date";
        }
        else if (Geoshape.class.isAssignableFrom(dataType)) {
            return mapping == Mapping.DEFAULT ? "geo_point" : "geo_shape";
        }

        return null;
    }

    private ElasticSearchResponse runCommonQuery(RawQuery query, KeyInformation.IndexRetriever informations, BaseTransaction tx, int size,
                                                 boolean useScroll) throws BackendException{
        final ElasticSearchRequest sr = new ElasticSearchRequest();
        sr.setQuery(compat.queryString(query.getQuery()));
        if (!query.getOrders().isEmpty()) {
            addOrderToQuery(informations, sr, query.getOrders(), query.getStore());
        }
        sr.setFrom(0);
        sr.setSize(size);
        sr.setDisableSourceRetrieval(true);
        try {
            Map<String, Object> requestBody = compat.createRequestBody(sr, query.getParameters());
            if(!useScroll) {
                if (requestBody == null) {
                    requestBody = TRACK_TOTAL_HITS_DISABLED_REQUEST_BODY;
                } else {
                    requestBody.put(TRACK_TOTAL_HITS_PARAMETER, false);
                }
            }
            return client.search(
                getIndexStoreName(query.getStore()),
                requestBody,
                useScroll);
        } catch (final IOException | UncheckedIOException e) {
            throw new PermanentBackendException(e);
        }
    }

    private long runCountQuery(RawQuery query) throws BackendException{
        try {
            return client.countTotal(
                getIndexStoreName(query.getStore()),
                compat.createRequestBody(compat.queryString(query.getQuery()), query.getParameters()));
        } catch (final IOException | UncheckedIOException e) {
            throw new PermanentBackendException(e);
        }
    }

    private void addOrderToQuery(KeyInformation.IndexRetriever informations, ElasticSearchRequest sr, final List<IndexQuery.OrderEntry> orders,
                                 String store) {
        for (final IndexQuery.OrderEntry orderEntry : orders) {
            final String order = orderEntry.getOrder().name();
            final KeyInformation information = informations.get(store).get(orderEntry.getKey());
            final Mapping mapping = Mapping.getMapping(information);
            final Class<?> datatype = orderEntry.getDatatype();
            sr.addSort(orderEntry.getKey(), order.toLowerCase(), convertToEsDataType(datatype, mapping));
        }
    }

    @Override
    public Stream<RawQuery.Result<String>> query(RawQuery query, KeyInformation.IndexRetriever information,
                                                 BaseTransaction tx) throws BackendException {
        final int size = query.hasLimit() ? Math.min(query.getLimit() + query.getOffset(), batchSize) : batchSize;
        final boolean useScroll = size >= batchSize;
        final ElasticSearchResponse response = runCommonQuery(query, information, tx, size, useScroll);
        log.debug("First Executed query [{}] in {} ms", query.getQuery(), response.getTook());
        final Iterator<RawQuery.Result<String>> resultIterator = getResultsIterator(useScroll, response, size);
        final Stream<RawQuery.Result<String>> toReturn
                = StreamSupport.stream(Spliterators.spliteratorUnknownSize(resultIterator, Spliterator.ORDERED),
                false).skip(query.getOffset());
        return query.hasLimit() ? toReturn.limit(query.getLimit()) : toReturn;
    }

    @Override
    public Long totals(RawQuery query, KeyInformation.IndexRetriever information,
                       BaseTransaction tx) throws BackendException {
        long startTime = System.currentTimeMillis();
        long count = runCountQuery(query);
        if(log.isDebugEnabled()){
            log.debug("Executed count query [{}] in {} ms", query.getQuery(), System.currentTimeMillis() - startTime);
        }
        return count;
    }

    @Override
    public boolean supports(KeyInformation information, JanusGraphPredicate janusgraphPredicate) {
        final Class<?> dataType = information.getDataType();
        final Mapping mapping = Mapping.getMapping(information);
        if (mapping!=Mapping.DEFAULT && !AttributeUtils.isString(dataType) &&
                !(mapping==Mapping.PREFIX_TREE && AttributeUtils.isGeo(dataType))) return false;

        if (Number.class.isAssignableFrom(dataType)) {
            return janusgraphPredicate instanceof Cmp;
        } else if (dataType == Geoshape.class) {
            switch(mapping) {
                case DEFAULT:
                    return janusgraphPredicate instanceof Geo && janusgraphPredicate != Geo.CONTAINS;
                case PREFIX_TREE:
                    return janusgraphPredicate instanceof Geo;
            }
        } else if (AttributeUtils.isString(dataType)) {
            switch(mapping) {
                case DEFAULT:
                case TEXT:
                    return janusgraphPredicate == Text.CONTAINS || janusgraphPredicate == Text.CONTAINS_PREFIX
                            || janusgraphPredicate == Text.CONTAINS_REGEX || janusgraphPredicate == Text.CONTAINS_FUZZY;
                case STRING:
                    return janusgraphPredicate instanceof Cmp || janusgraphPredicate==Text.REGEX
                            || janusgraphPredicate==Text.PREFIX  || janusgraphPredicate == Text.FUZZY;
                case TEXTSTRING:
                    return janusgraphPredicate instanceof Text || janusgraphPredicate instanceof Cmp;
            }
        } else if (dataType == Date.class || dataType == Instant.class) {
            return janusgraphPredicate instanceof Cmp;
        } else if (dataType == Boolean.class) {
            return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate == Cmp.NOT_EQUAL;
        } else if (dataType == UUID.class) {
            return janusgraphPredicate == Cmp.EQUAL || janusgraphPredicate==Cmp.NOT_EQUAL;
        }
        return false;
    }


    @Override
    public boolean supports(KeyInformation information) {
        final Class<?> dataType = information.getDataType();
        final Mapping mapping = Mapping.getMapping(information);
        if (Number.class.isAssignableFrom(dataType) || dataType == Date.class || dataType== Instant.class
                || dataType == Boolean.class || dataType == UUID.class) {
            return mapping == Mapping.DEFAULT;
        } else if (AttributeUtils.isString(dataType)) {
            return mapping == Mapping.DEFAULT || mapping == Mapping.STRING
                || mapping == Mapping.TEXT || mapping == Mapping.TEXTSTRING;
        } else if (AttributeUtils.isGeo(dataType)) {
            return mapping == Mapping.DEFAULT || mapping == Mapping.PREFIX_TREE;
        }
        return false;
    }

    @Override
    public String mapKey2Field(String key, KeyInformation information) {
        IndexProvider.checkKeyValidity(key);
        return key.replace(' ', IndexProvider.REPLACEMENT_CHAR);
    }

    @Override
    public IndexFeatures getFeatures() {
        return compat.getIndexFeatures();
    }

    @Override
    public BaseTransactionConfigurable beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new DefaultTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        try {
            client.close();
        } catch (final IOException e) {
            throw new PermanentBackendException(e);
        }

    }

    @Override
    public void clearStorage() throws BackendException {
        try {
            client.deleteIndex(indexName);
        } catch (final Exception e) {
            throw new PermanentBackendException("Could not delete index " + indexName, e);
        } finally {
            close();
        }
    }

    @Override
    public boolean exists() throws BackendException {
        try {
            return client.indexExists(indexName);
        } catch (final IOException e) {
            throw new PermanentBackendException("Could not check if index " + indexName + " exists", e);
        }
    }

    ElasticMajorVersion getVersion() {
        return client.getMajorVersion();
    }

    boolean isUseMappingForES7(){
        return useMappingForES7;
    }

    private static String parameterizedScriptPrepare(String ... lines){
        return Arrays.stream(lines).map(String::trim).collect(Collectors.joining(""));
    }
}
