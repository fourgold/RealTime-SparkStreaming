package com.atguigu.esTest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Jinxin Li
 * @create 2020-12-06 23:00
 */
public class Reader {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端连接池
        JestClientFactory factory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //3.设置ES连接地址
        factory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = factory.getObject();

        //5.构建查询数据对象
        Search search = new Search.Builder("{\n" +
                "  \"query\": {\n" +
                "    \"match\": {\n" +
                "      \"name\": \"zhangsan\"\n" +
                "    }\n" +
                "  }\n" +
                "}").addIndex("test5").addType("_doc").build();

        //6.执行查询操作
        SearchResult searchResult = jestClient.execute(search);

        //7.解析查询结果
        System.out.println(searchResult.getTotal());
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println(hit.index + "--" + hit.id);
        }

        //8.关闭连接
        jestClient.shutdownClient();

    }
}
