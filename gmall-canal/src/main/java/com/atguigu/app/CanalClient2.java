package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstant;
import com.atguigu.util.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;
/*
将mysql表传输到kafka
 */
public class CanalClient2 {

    public static void main(String[] args) throws InvalidProtocolBufferException {

        //获取Cannal的连接
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                "");
        while (true) {
            canalConnector.connect();//连接客户端
            //订阅指定MySQL的中的仓库(我们在conf中书写的instance.properties配置了连接mysql的实例hadoop102:3306)
            canalConnector.subscribe("gmall200720.*");

            //抓取数据,一次抓多少,这个100相当于一个最大值
            Message message = canalConnector.get(100);//一个message包含多个entry
            if (message.getEntries().size() == 0) {//判断当前抓取的数据是否为空
                System.out.println("没抓取到,程序休整一会儿");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                List<CanalEntry.Entry> entries = message.getEntries();
                for (CanalEntry.Entry entry : entries) {


                    //此时对每一条entry进行解析
                    //每个entry包含多个信息,表信息,等,其中保存表内容的信息是RowData
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        //1.获取表名
                        String tableName = entry.getHeader().getTableName();

                        //2.获取内部数据
                        ByteString storeValue = entry.getStoreValue();

                        //3.将数据反序列化
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        //4.获取事件类型 INSERT UPDATE DELETE CREATE
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //5.获取行改变的记录信息
                        List<CanalEntry.RowData> rowDataList = rowChange.getRowDatasList();

                        handle(tableName, eventType, rowDataList);

                    }

                }
            }
        }


    }

    //将数据发送到kafka
    private static void handle(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDataList) {
        //选择orderInfo表(既有新增,又有修改),求GMV,我们只需要下单数据
        //选择insert是只需要insert数据,不需要修改的数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDataList) {
                System.out.println("发现一条order_info");
                sendToKafka(rowData, GmallConstant.GMALL_ORDER_INFO);

            }//明细表也需要新增
        } else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {
            for (CanalEntry.RowData rowData : rowDataList) {

                //创建json对象,用于存放多个数据
                System.out.println("发现一条order_detail");
                sendToKafka(rowData, GmallConstant.GMALL_ORDER_DETAIL);
            }//用户表是一个新增及变化的表
        } else if ("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType)||CanalEntry.EventType.UPDATE.equals(eventType))) {
                for (CanalEntry.RowData rowData : rowDataList) {
                    System.out.println("发现一条user_info");

                    //创建json对象,用于存放多个数据
                    sendToKafka(rowData, GmallConstant.GMALL_USER_INFO);
                }
        }
    }

    //ctrl+alt+M 提取出方法
    private static void sendToKafka(CanalEntry.RowData rowData, String topic) {
        //创建json对象,用于存放多个数据
        JSONObject jsonObject = new JSONObject();

        //rowData有两套Column,Before(删除等操作)跟after(新增,修改),我们这里新增是需要afterCloumn
        for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
            jsonObject.put(column.getName(), column.getValue());
        }
        System.out.println("发送到"+topic+" " +jsonObject.toString());
        MyKafkaSender.send(topic, jsonObject.toString());
    }
}