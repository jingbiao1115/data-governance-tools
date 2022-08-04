package com.jb.adapter.hadoop;


import com.jb.adapter.IMetadataDriver;

/**
 * @author zhaojb
 * Hadoop生态元数据采集适配
 */
public interface IHadoopMetadataDriver<T> extends IMetadataDriver {


    /**
     * 数据库信息
     */
    T getHadoopCatalogMeta() throws Exception;

}
