package com.atguigu.esTest;

import com.atguigu.bean.Movie;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;

import java.io.IOException;

/**
 * @author Jinxin Li
 * @create 2020-12-06 22:59
 */
public class WriterBatch {
    public static void main(String[] args) throws IOException {
        //1.创建客户端对象
        JestClientFactory jestClientFactory = new JestClientFactory();

//2.设置连接参数
        HttpClientConfig httpClientConfig = new HttpClientConfig
                .Builder("http://hadoop102:9200")
                .build();
        jestClientFactory.setHttpClientConfig(httpClientConfig);

//3.获取客户端对象
        JestClient jestClient = jestClientFactory.getObject();
        Movie movie1 = new Movie("1002", "二十不惑");
        Movie movie2 = new Movie("1003", "三十而立");
        Movie movie3 = new Movie("1004", "寄生虫");

        Bulk bulk = new Bulk.Builder()
                .addAction(new Index.Builder(movie1).id("1002").build())
                .addAction(new Index.Builder(movie1).id("1003").build())
                .addAction(new Index.Builder(movie1).id("1004").build())
                .defaultIndex("movie_test1")
                .defaultType("_doc")
                .build();
        jestClient.execute(bulk);
        jestClient.shutdownClient();

    }
}
