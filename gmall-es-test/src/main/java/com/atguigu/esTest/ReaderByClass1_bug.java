package com.atguigu.esTest;



import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MaxAggregation;
import io.searchbox.core.search.aggregation.MetricAggregation;
import org.apache.lucene.util.QueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.metrics.max.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Jinxin Li
 * @create 2020-12-06 23:00
 */
public class ReaderByClass1_bug {
    public static void main(String[] args) throws IOException {
        //1.创建ES客户端连接池
        JestClientFactory factory = new JestClientFactory();

        //2.创建ES客户端连接地址
        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder("http://hadoop102:9200").build();

        //3.设置ES连接地址
        factory.setHttpClientConfig(httpClientConfig);

        //4.获取ES客户端连接
        JestClient jestClient = factory.getObject();

        //5.创建search对象,这个相当于查询的大括号
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //然后除括号外之外的第一层都是一层方法,一层对象
        //5.1创建查询条件,由于是外层,所以属于方法

        MaxAggregationBuilder maxAge = new MaxAggregationBuilder("age");
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder("favo2", "喝酒");

        searchSourceBuilder.query(queryBuilder);
        searchSourceBuilder.aggregation(maxAge);

        searchSourceBuilder.from(0);

        //6.执行查询操作
        System.out.println(searchSourceBuilder);
        Search build = new Search.Builder(searchSourceBuilder.toString()).build();
        SearchResult searchResult = jestClient.execute(build);

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
        System.out.println(aggregations.getMaxAggregation("getMaxAge").getMax());
        //8.关闭连接
        jestClient.shutdownClient();
    }
}
