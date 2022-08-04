package com.jb.adapter.elasticsearch;

import com.jb.adapter.IMetadataDriver;
import com.jb.adapter.elasticsearch.impl.ElasticSearchMetadataDriver;

import java.util.List;

/**
 * @author zhaojb
 */
public interface IElasticSearchMetadataDriver extends IMetadataDriver {

    /**
     * 数据源元数据
     */
    ElasticSearchMetadataDriver getCatalogMeta();

    /**
     * 索引元数据
     */
    ElasticSearchMetadataDriver getIndexMetas(List<String > indexs);

    /**
     * 索引Properties
     */
    ElasticSearchMetadataDriver getIndexProperties();

}
