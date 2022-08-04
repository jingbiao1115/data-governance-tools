package com.jb.adapter.elasticsearch.impl;

import cn.hutool.core.util.ObjectUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jb.adapter.elasticsearch.IElasticSearchMetadataDriver;
import com.jb.enity.metadata.elasticsearch.ElasticsearchCatalogMeta;
import com.jb.enity.metadata.elasticsearch.index.ElasticsearchIndexMeta;
import com.jb.enity.metadata.elasticsearch.index.properties.ElasticsearchPropertiesMeta;
import com.jb.enity.parameter.ElasticSearch.ElasticSearchParameter;
import com.jb.enums.CollectDataSourceEnum;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author zhaojb
 * <p>
 * ES元数据采集-适配
 */
public class ElasticSearchMetadataDriver implements IElasticSearchMetadataDriver {

    private CloseableHttpClient httpClient = null;
    private HttpGet httpGet;
    private CloseableHttpResponse response = null;

    private ElasticSearchParameter parameter;

    private ElasticsearchCatalogMeta elasticsearchCatalogMeta;

    private List<ElasticsearchIndexMeta> indexMetas = new ArrayList<>(128);

    public ElasticSearchMetadataDriver(ElasticSearchParameter parameter) {
        this.parameter = parameter;

        this.elasticsearchCatalogMeta = new ElasticsearchCatalogMeta();
        this.elasticsearchCatalogMeta.setCollectDataSource(CollectDataSourceEnum.ELASTIC_SEARCH);
        this.elasticsearchCatalogMeta.setEsHost(parameter.getHost());
    }

    /**
     * 创建连接
     */
    public void createConnection() throws URISyntaxException {
        httpClient = HttpClientBuilder.create().build();

        URIBuilder uriBuilder = new URIBuilder(parameter.getHost());
        /**
         *   uriBuilder.addParameter("lang", input);
         *   uriBuilder.addParameter("user_id", userId);
         */

        //        httpPost = new HttpPost(uriBuilder.build().toString()); //OK
        //        httpPost.setHeader(HttpHeaders.CONTENT_TYPE,DEFAULT_APPLICATION);
        //        httpPost.setEntity(new StringEntity(body));

        httpGet = new HttpGet(uriBuilder.toString());
        httpGet.setHeader(HttpHeaders.AUTHORIZATION,auth(parameter.getUsername(),
                                                         parameter.getPassword()));

    }

    public ElasticSearchMetadataDriver getCatalogMeta() {
        JSONArray result = JSON.parseArray(restfulApi("/_cat/allocation?format=json"));

        this.elasticsearchCatalogMeta.setIndexMetas(this.indexMetas);
        this.elasticsearchCatalogMeta.setIndexNum(this.indexMetas.size());

        //单位gb
        double capacity = 0;

        for (int i = 0;i < result.size();i++) {
            JSONObject jsonObject = result.getJSONObject(i);

            capacity += unitConversion(jsonObject.getString("disk.used"));
        }

        this.elasticsearchCatalogMeta.setCapacity(
                BigDecimal.valueOf(capacity).setScale(2,RoundingMode.HALF_UP).doubleValue());

        return this;
    }

    public ElasticsearchCatalogMeta getElasticsearchCatalogMeta() {
        return elasticsearchCatalogMeta;
    }

    public ElasticSearchMetadataDriver getIndexMetas(List<String> indexs) {

        if (ObjectUtil.isNull(indexs)) {
            indexs = getAllIndexs();
        }

        JSONArray result = JSON.parseArray(restfulApi("/_cat/indices?format=json"));

        for (int i = 0;i < result.size();i++) {
            JSONObject jsonObject = result.getJSONObject(i);

            String indexName = jsonObject.getString("index");

            if (indexs.contains(indexName)) {
                ElasticsearchIndexMeta indexMeta = new ElasticsearchIndexMeta();
                indexMeta.setIndex(indexName);

                indexMeta.setIndexCapacity(
                        BigDecimal.valueOf(
                                        unitConversion(
                                                jsonObject.getString("store.size")))
                                .setScale(2,RoundingMode.HALF_UP).doubleValue());

                indexMeta.setIndexHealth(jsonObject.getString("health"));
                indexMeta.setIndexStatus(jsonObject.getString("status"));
                indexMeta.setIndexUuid(jsonObject.getString("uuid"));
                indexMeta.setIndexPri(jsonObject.getInteger("pri"));
                indexMeta.setIndexRep(jsonObject.getInteger("rep"));
                indexMeta.setIndexDocCount(jsonObject.getInteger("docs.count"));

                this.indexMetas.add(indexMeta);
            }
        }

        return this;
    }

    public List<String> getAllIndexs() {
        List<String> allIndexs = new ArrayList<>(128);

        JSONArray result = JSON.parseArray(restfulApi("/_cat/indices?format=json"));

        for (int i = 0;i < result.size();i++) {
            JSONObject jsonObject = result.getJSONObject(i);

            String indexName = jsonObject.getString("index");

            //.geoip_databases是ES内部数据库,用于系统操作使用及访问
            if(!".geoip_databases".equals(indexName)){
                allIndexs.add(indexName);
            }

        }

        return allIndexs;
    }

    public ElasticSearchMetadataDriver getIndexProperties() {
        JSONObject result = JSON.parseObject(restfulApi("/_mapping?format=json"));

        for (ElasticsearchIndexMeta indexMeta: this.indexMetas) {

            JSONObject jsonObject = result
                    .getJSONObject(indexMeta.getIndex())
                    .getJSONObject("mappings")
                    .getJSONObject("properties");

            Iterator<String> fieldNameIterator = jsonObject.keySet().iterator();

            List<ElasticsearchPropertiesMeta> propertiesMetas = new ArrayList<>(16);

            while (fieldNameIterator.hasNext()) {
                String fieldName = fieldNameIterator.next();

                ElasticsearchPropertiesMeta propertiesMeta = new ElasticsearchPropertiesMeta();
                propertiesMeta.setFieldName(fieldName);
                propertiesMeta.setFieldInfo(jsonObject.getJSONObject(fieldName));

                propertiesMetas.add(propertiesMeta);
            }

            indexMeta.setProperties(propertiesMetas);
        }


        return this;
    }

    /**
     * restful方法
     */
    private String restfulApi(String path) {

        httpGet.setURI(URI.create(parameter.getHost() + path));

        try {
            response = httpClient.execute(httpGet);

            if (ObjectUtil.isNotNull(response)) {

                return EntityUtils.toString(response.getEntity(),StandardCharsets.UTF_8);

            } else {
                throw new RuntimeException("response is not null");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭连接
     */
    public void close() {
        try {
            if (httpClient != null) {
                httpClient.close();
            }
            if (response != null) {
                response.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private String auth(String username,String password) {
        String auth = String.format("%s:%s",username,password);

        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encodedAuth);
    }

    private static double unitConversion(String diskUsed) {
        if (diskUsed.contains("pb")) {
            //pb
            return Double.valueOf(diskUsed.substring(0,diskUsed.indexOf("pb"))) * 1024 * 1024;

        } else if (diskUsed.contains("tb")) {
            //tb
            return Double.valueOf(diskUsed.substring(0,diskUsed.indexOf("tb"))) * 1024;

        } else if (diskUsed.contains("gb")) {
            //gb
            return Double.valueOf(diskUsed.substring(0,diskUsed.indexOf("gb")));

        } else if (diskUsed.contains("mb")) {
            //mb
            return Double.valueOf(diskUsed.substring(0,diskUsed.indexOf("mb"))) / 1024;

        } else if (diskUsed.contains("kb")) {
            //kb
            return Double.valueOf(diskUsed.substring(0,diskUsed.indexOf("kb"))) / (1024 * 1024);

        } else {
            //b
            return Double.valueOf(diskUsed.substring(0,diskUsed.indexOf("b"))) / (1024 * 1024 * 1024);
        }
    }

}
