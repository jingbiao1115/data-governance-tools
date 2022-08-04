package com.jb.enity.metadata.elasticsearch.index.properties;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

/**
 * @author zhaojb
 * ES Properties 字段
 */
@Data
public class ElasticsearchPropertiesMeta {

    /**
     * 字段名称
     */
    private String  fieldName;

    /**
     * 字段信息
     */
    private JSONObject fieldInfo;


}
