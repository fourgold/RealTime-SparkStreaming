package com.atguigu.esTest;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Jinxin Li
 * @create 2020-12-06 23:00
 */
public class ReaderByClass {
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
                "  \n" +
                "  \"aggs\": {\n" +
                "    \"getMaxAge\": {\n" +
                "      \"max\": {\n" +
                "        \"field\": \"age\"\n" +
                "      }\n" +
                "    }\n" +
                "  },\n" +
                "  \"query\": {\n" +
                "    \"match\": {\n" +
                "      \"favo2\": \"喝酒\"\n" +
                "    }\n" +
                "  }\n" +
                "}")
                .addIndex("stu1")
                .addType("_doc")
                .build();

        //6.执行查询操作
        SearchResult searchResult = jestClient.execute(search);

        //7.解析查询结果
        Long total = searchResult.getTotal();//获取查询总数
        System.out.println("查询结果总数"+total);

        //-----------------------
        //查询数据的接收
        //这边可以给一个map
        List<SearchResult.Hit<Map, Void>> hits = searchResult.getHits(Map.class);
        for (SearchResult.Hit<Map, Void> hit : hits) {
            System.out.println(hit.source);//获得打印的明细
        }
        MetricAggregation aggregations = searchResult.getAggregations();
        MaxAggregation maxAge = aggregations.getMaxAggregation("getMaxAge");
        System.out.println("最大年纪为"+maxAge.getMax());
        //8.关闭连接
        jestClient.shutdownClient();

    }
}
