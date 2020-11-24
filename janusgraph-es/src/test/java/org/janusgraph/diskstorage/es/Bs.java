package org.janusgraph.diskstorage.es;

import java.util.*;

/**
 * @author: ldp
 * @time: 2020/7/25 13:59
 * @jira:
 */
public class Bs {
    public static void main(String[] args) {

    }

    class A {
        public String name;
        public Object value;
        public String cardinality;
        public String startDate;
        public String endDate;
        public String geo;
        public Set<String> dsr;
        public String role;
    }

    class Parmater {
        public List<A> fields;
    }

    private void ff(Parmater params) {
        for (A field : params.fields) {
            //def fieldValueArray = ctx._source.get(field.name);
            ArrayList<Map<String,Object>> fieldValueArray=new ArrayList();
            boolean isExists=false;
            if (fieldValueArray != null) {
                Map<String,Object> existMap=null;
                for (Map<String,Object> simple : fieldValueArray) {
                    if (simple != null && simple.containsKey("value")) {
                        Object v=simple.get("value");
                        if(v!=null&&simple.get("value").equals(field.value)){
                            isExists=true;
                            existMap=simple;
                        }
                    }
                }
                if(isExists&&existMap!=null){
                    if (field.startDate != null) {
                        existMap.put("startDate", field.startDate);
                    }
                    if (field.endDate != null) {
                        existMap.put("endDate", field.endDate);
                    }
                    if (field.role != null) {
                        existMap.put("role", field.role);
                    }
                    if(field.geo!=null){
                        existMap.put("geo",field.geo);
                    }
                    if(field.dsr!=null&&field.dsr.size()>0) {
                        Object dsrObjects = existMap.get("dsr");
                        if (dsrObjects != null) {
                            if (dsrObjects instanceof List) {
                                Set<String> existsDsrs=new HashSet<>((List)dsrObjects);
                                for(String dsr:field.dsr){
                                    if(!existsDsrs.contains(dsr)){
                                        ((List)dsrObjects).add(dsr);
                                    }
                                }
                            } else {
                                existMap.put("dsr",field.dsr);
                            }
                        }else{
                            existMap.put("dsr",field.dsr);
                        }
                    }
                }else{
                    Map<String,Object> newValueMap=new HashMap<>();
                    if(field.value!=null){
                        newValueMap.put("value",field.value);
                    }
                    if (field.startDate != null) {
                        newValueMap.put("startDate", field.startDate);
                    }
                    if (field.endDate != null) {
                        newValueMap.put("endDate", field.endDate);
                    }
                    if (field.role != null) {
                        newValueMap.put("role", field.role);
                    }
                    if(field.geo!=null){
                        newValueMap.put("geo",field.geo);
                    }
                    if(field.dsr!=null&&field.dsr.size()>0) {
                        newValueMap.put("dsr",field.dsr);
                    }
                    fieldValueArray.add(newValueMap);
                }
            }else{
                ArrayList<Map<String,Object>> fieldValueList=new ArrayList();
                Map<String,Object> newValueMap=new HashMap<>();
                if(field.value!=null){
                    newValueMap.put("value",field.value);
                }
                if (field.startDate != null) {
                    newValueMap.put("startDate", field.startDate);
                }
                if (field.endDate != null) {
                    newValueMap.put("endDate", field.endDate);
                }
                if (field.role != null) {
                    newValueMap.put("role", field.role);
                }
                if(field.geo!=null){
                    newValueMap.put("geo",field.geo);
                }
                if(field.dsr!=null&&field.dsr.size()>0) {
                    newValueMap.put("dsr",field.dsr);
                }
                fieldValueList.add(newValueMap);
                //ctx._source.put(field.name,fieldValueList);
            }
        }
    }
}
