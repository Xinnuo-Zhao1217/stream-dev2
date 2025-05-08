package com.zxn.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.zxn.constant.Constant;
import com.zxn.util.*;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.jiao.func.DbusDBCommentFactData2Kafka
 * @Author xinyi.jiao
 * @Date 2025/5/7 15:33
 * @description: TODO 该任务，修复了之前SQL的代码逻辑，在之前的逻辑中使用了FlinkSQL的方法进行了实现，把去重的问题，留给了下游的DWS，这种行为非常的yc
 *  * TODO Before FlinkSQL Left join and use hbase look up join func ,left join 产生的2条异常数据，会在下游做处理，一条为null，一条为未关联上的数据
 *  * TODO After FlinkAPI Async and google guava cache
 */
public class DbusDBCommentFactData2Kafka {
    private static final ArrayList<String> sensitiveWordsLists;

    static {
        sensitiveWordsLists = SensitiveWordsUtils.getSensitiveWordsLists();
    }
    @SneakyThrows
    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);
        // 评论表 取数
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                KafkaUtils.buildKafkaSecureSource(
                        // Kafka 集群的地址，用于连接到 Kafka 服务器
                        Constant.KAFKA_BROKERS,
                        //Kafka  主题
                        Constant.TOPIC_GL,
                        // 消费者组的 ID，这里使用当前日期的字符串表示
                        new Date().toString(),
                        OffsetsInitializer.earliest()
                ),
                // 这里使用 forBoundedOutOfOrderness 方法创建一个有界乱序水印策略，允许数据乱序的最大时间为 3 秒
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        // 为每个事件分配时间戳
                        .withTimestampAssigner((event, timestamp) -> {
                            // 检查事件是否为空
                            if (event != null) {
                                try {
                                    // 将事件字符串解析为 JSON 对象
                                    // 并从 JSON 对象中获取 "ts_ms" 字段的值作为事件的时间戳
                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                } catch (Exception e) {
                                    // 如果解析 JSON 或获取 "ts_ms" 字段值时发生异常
                                    // 打印异常堆栈信息
                                    e.printStackTrace();
                                    // 输出错误信息，提示解析失败
                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                    // 若解析失败，返回时间戳为 0L
                                    return 0L;
                                }
                            }
                            // 如果事件为空，返回时间戳为 0L
                            return 0L;
                        }),
                "kafka_cdc_xn_source"
        ).uid("kafka_cdc_xn_source").name("kafka_cdc_xn_source");
//        kafkaCdcDbSource.print();
//      {"op":"c","after":{"payment_way":"3501","refundable_time":1747264827000,"original_total_amount":"24522.00","order_status":"1001","consignee_tel":"13888155719","trade_body":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 明月灰 游戏智能手机 小米 红米等7件商品","id":1133,"consignee":"吴琛钧","create_time":1746660027000,"coupon_reduce_amount":"0.00","out_trade_no":"858182663635648","total_amount":"24272.00","user_id":78,"province_id":27,"activity_reduce_amount":"250.00"},"source":{"thread":20259,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000004","connector":"mysql","pos":31459615,"name":"mysql_binlog_source","row":0,"ts_ms":1746596801000,"snapshot":"false","db":"realtime_v1","table":"order_info"},"ts_ms":1746596800964}
        DataStream<JSONObject> filteredOrderInfoStream = kafkaCdcDbSource
                //转成json
                .map(JSON::parseObject)
                //过滤 表中是否是 order_info
                .filter(json -> json.getJSONObject("source").getString("table").equals("order_info"))
                .uid("kafka_cdc_xn_order_source").name("kafka_cdc_xn_order_source");
//        5> {"op":"c","after":{"payment_way":"3501","consignee":"韦明永","create_time":1746654652000,"refundable_time":1747259452000,"original_total_amount":"69.00","coupon_reduce_amount":"0.00","order_status":"1001","out_trade_no":"432269165763778","total_amount":"69.00","user_id":353,"province_id":28,"consignee_tel":"13257824651","trade_body":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M02干玫瑰等1件商品","id":1103,"activity_reduce_amount":"0.00"},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":31242377,"name":"mysql_binlog_source","thread":20261,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746596799000,"snapshot":"false","db":"realtime_v1","table":"order_info"},"ts_ms":1746596799639}
//        filteredOrderInfoStream.print();
//         评论表进行进行升维处理 和hbase的维度进行关联补充维度数据
        DataStream<JSONObject> filteredStream = kafkaCdcDbSource
                .map(JSON::parseObject)
                .filter(json -> json.getJSONObject("source").getString("table").equals("comment_info"))
                .keyBy(json -> json.getJSONObject("after").getString("appraise"));
        // {"op":"c","after":{"create_time":1746624077000,"user_id":178,"appraise":"1201","comment_txt":"评论内容：44237268662145286925725839461514467765118653811952","nick_name":"珠珠","sku_id":14,"id":85,"spu_id":4,"order_id":1010},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":30637591,"name":"mysql_binlog_source","thread":20256,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746596796000,"snapshot":"false","db":"realtime_v1","table":"comment_info"},"ts_ms":1746596796319}
//        filteredStream.print();
   //异步连接hbase
        DataStream<JSONObject> enrichedStream = AsyncDataStream
                .unorderedWait(
                        filteredStream,
                        new AsyncHbaseDimBaseDicFunc(),
                        60,
                        TimeUnit.SECONDS,
                        100
                ).uid("xn_hbase_dim_base_dic_func")
                .name("xn_hbase_dim_base_dic_func");
//        5> {"op":"c","after":{"create_time":1746624077000,"user_id":178,"appraise":"1201","comment_txt":"评论内容：44237268662145286925725839461514467765118653811952","nick_name":"珠珠","sku_id":14,"id":85,"spu_id":4,"order_id":1010,"dic_name":"N/A"},"source":{"file":"mysql-bin.000004","connector":"mysql","pos":30637591,"name":"mysql_binlog_source","thread":20256,"row":0,"server_id":1,"version":"1.9.7.Final","ts_ms":1746596796000,"snapshot":"false","db":"realtime_v1","table":"comment_info"},"ts_ms":1746596796319}
        enrichedStream.print();


        // 创建一个 SingleOutputStreamOperator 类型的流，用于存储转换后的订单评论数据
// 输入输出的数据类型均为 JSONObject
        SingleOutputStreamOperator<JSONObject> orderCommentMap = enrichedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject){
                        JSONObject resJsonObj = new JSONObject();
                        Long tsMs = jsonObject.getLong("ts_ms");
                        JSONObject source = jsonObject.getJSONObject("source");
                        String dbName = source.getString("db");
                        String tableName = source.getString("table");
                        String serverId = source.getString("server_id");
                        if (jsonObject.containsKey("after")) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            resJsonObj.put("ts_ms", tsMs);
                            resJsonObj.put("db", dbName);
                            resJsonObj.put("table", tableName);
                            resJsonObj.put("server_id", serverId);
                            resJsonObj.put("appraise", after.getString("appraise"));
                            resJsonObj.put("commentTxt", after.getString("comment_txt"));
                            resJsonObj.put("op", jsonObject.getString("op"));
                            resJsonObj.put("nick_name", jsonObject.getString("nick_name"));
                            resJsonObj.put("create_time", after.getLong("create_time"));
                            resJsonObj.put("user_id", after.getLong("user_id"));
                            resJsonObj.put("sku_id", after.getLong("sku_id"));
                            resJsonObj.put("id", after.getLong("id"));
                            resJsonObj.put("spu_id", after.getLong("spu_id"));
                            resJsonObj.put("order_id", after.getLong("order_id"));
                            resJsonObj.put("dic_name", after.getString("dic_name"));
                            return resJsonObj;
                        }
                        return null;
                    }
                })
                .uid("xn-order_comment_data")
                .name("xn-order_comment_data");
//        5> {"op":"c","create_time":1746659107000,"commentTxt":"评论内容：81253521526538839584668472639195946148295293662286","sku_id":34,"server_id":"1","dic_name":"N/A","appraise":"1201","user_id":260,"id":94,"spu_id":12,"order_id":1132,"ts_ms":1746596801219,"db":"realtime_v1","table":"comment_info"}
//        orderCommentMap.print();

//        创建一个 SingleOutputStreamOperator 类型的流，用于存储转换后的订单信息数据
        SingleOutputStreamOperator<JSONObject> orderInfoMapDs = filteredOrderInfoStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject inputJsonObj){
                String op = inputJsonObj.getString("op");
                long tm_ms = inputJsonObj.getLongValue("ts_ms");
                JSONObject dataObj;
                if (inputJsonObj.containsKey("after") && !inputJsonObj.getJSONObject("after").isEmpty()) {
                    dataObj = inputJsonObj.getJSONObject("after");
                } else {
                    dataObj = inputJsonObj.getJSONObject("before");
                }
//                 创建一个新的 JSON 对象用于存储最终结果
                JSONObject resultObj = new JSONObject();
                resultObj.put("op", op);
                resultObj.put("tm_ms", tm_ms);
                // 将 dataObj 中的所有键值对添加到结果对象中
                resultObj.putAll(dataObj);
                //返回结果
                return resultObj;
            }
        }).uid("xn-order_info_data").name("map-order_info_data");
//        5> {"op":"c","payment_way":"3501","consignee":"吴琛钧","create_time":1746660027000,"refundable_time":1747264827000,"original_total_amount":"24522.00","coupon_reduce_amount":"0.00","order_status":"1001","out_trade_no":"858182663635648","total_amount":"24272.00","user_id":78,"province_id":27,"tm_ms":1746596800964,"consignee_tel":"13888155719","trade_body":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 明月灰 游戏智能手机 小米 红米等7件商品","id":1133,"activity_reduce_amount":"250.00"}
//        orderInfoMapDs.print();
//         orderCommentMap.order_id join orderInfoMapDs.id
        // 对 orderCommentMap 流按照订单 ID 进行分组，创建一个 KeyedStream
// 分组后，相同订单 ID 的数据会被分到同一组，方便后续处理
        KeyedStream<JSONObject, String> keyedOrderCommentStream = orderCommentMap.keyBy(data -> data.getString("order_id"));
//        keyedOrderCommentStream.print("keyedOrderCommentStream===>");
        KeyedStream<JSONObject, String> keyedOrderInfoStream = orderInfoMapDs.keyBy(data -> data.getString("id"));
//        keyedOrderInfoStream.print("keyedOrderInfoStream===>");
        // {"info_original_total_amount":"56092.00","info_activity_reduce_amount":"1199.90","commentTxt":"评论内容：52198813817222113474133821791377912858419193882331","info_province_id":8,"info_payment_way":"3501","info_create_time":1746624020000,"info_refundable_time":1747228820000,"info_order_status":"1002","id":84,"spu_id":3,"table":"comment_info","info_tm_ms":1746596796189,"info_operate_time":1746624052000,"op":"c","create_time":1746624077000,"info_user_id":178,"info_op":"u","info_trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等6件商品","sku_id":11,"server_id":"1","dic_name":"好评","info_consignee_tel":"13316189177","info_total_amount":"54892.10","info_out_trade_no":"692358523797933","appraise":"1201","user_id":178,"info_id":1010,"info_coupon_reduce_amount":"0.00","order_id":1010,"info_consignee":"彭永","ts_ms":1746596796318,"db":"realtime_v1"}
        // 使用 intervalJoin 方法对两个 KeyedStream 进行区间连接操作
// 连接操作会将两个流中时间戳满足一定区间条件的数据进行关联
        SingleOutputStreamOperator<JSONObject> orderMsgAllDs = keyedOrderCommentStream.intervalJoin(keyedOrderInfoStream)
                // 设置时间区间为 -1 分钟到 1 分钟
                .between(Time.minutes(-1), Time.minutes(1))
                // 使用自定义的 IntervalJoinOrderCommentAndOrderInfoFunc 函数处理连接后的数据
                .process(new IntervalJoinOrderCommentAndOrderInfoFunc())
                .uid("xn_join_order_comment_and_order_info_func").name("xn_join_order_comment_and_order_info_func");

//        orderMsgAllDs.print();
//        3> {"info_original_total_amount":"25833.00","info_activity_reduce_amount":"250.00","commentTxt":"评论内容：98447872974976121594211456927638734474962114275797","info_province_id":1,"info_payment_way":"3501","info_create_time":1746568665000,"info_refundable_time":1747173465000,"info_order_status":"1004","id":79,"spu_id":3,"table":"comment_info","info_tm_ms":1746518022798,"info_operate_time":1746568722000,"op":"c","create_time":1746568722000,"info_user_id":150,"info_op":"u","info_trade_body":"十月稻田 辽河长粒香 东北大米 5kg等6件商品","sku_id":8,"server_id":"1","dic_name":"N/A","info_consignee_tel":"13684686721","info_total_amount":"25583.00","info_out_trade_no":"726565562182984","appraise":"1201","user_id":150,"info_id":924,"info_coupon_reduce_amount":"0.00","order_id":924,"info_consignee":"舒炎德","ts_ms":1746518022776,"db":"realtime_v1"}

        // 通过AI 生成评论数据，`Deepseek 7B` 模型即可
        // {"info_original_total_amount":"1299.00","info_activity_reduce_amount":"0.00","commentTxt":"\n\n这款Redmi 10X虽然价格亲民，但续航能力一般且相机效果平平，在同类产品中竞争力不足。","info_province_id":32,"info_payment_way":"3501","info_create_time":1746566254000,"info_refundable_time":1747171054000,"info_order_status":"1004","id":75,"spu_id":2,"table":"comment_info","info_tm_ms":1746518021300,"info_operate_time":1746563573000,"op":"c","create_time":1746563573000,"info_user_id":149,"info_op":"u","info_trade_body":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 明月灰 游戏智能手机 小米 红米等1件商品","sku_id":7,"server_id":"1","dic_name":"好评","info_consignee_tel":"13144335624","info_total_amount":"1299.00","info_out_trade_no":"199223184973112","appraise":"1201","user_id":149,"info_id":327,"info_coupon_reduce_amount":"0.00","order_id":327,"info_consignee":"范琳","ts_ms":1746518021294,"db":"realtime_v1"}
        SingleOutputStreamOperator<JSONObject> supplementDataMap = orderMsgAllDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                jsonObject.put("commentTxt", CommonGenerateTempLate.GenerateComment(jsonObject.getString("dic_name"), jsonObject.getString("info_trade_body")));
                return jsonObject;
            }
        }).uid("xn-generate_comment").name("xn-generate_comment");


//        supplementDataMap.print();

        SingleOutputStreamOperator<JSONObject> suppleMapDs = supplementDataMap.map(new RichMapFunction<JSONObject, JSONObject>() {
            // transient关键字表示该字段不会被序列化
            private transient Random random;
            // 重写RichMapFunction的open方法，该方法会在算子初始化时调用
            @Override
            public void open(Configuration parameters){
                random = new Random();
            }
            // 接收一个JSONObject类型的元素，处理后返回一个新的JSONObject类型的元素
            @Override
            public JSONObject map(JSONObject jsonObject){
                // 生成一个0到1之间的随机双精度浮点数
                // 判断该随机数是否小于0.2，即有20%的概率进入if语句块
                if (random.nextDouble() < 0.2) {
                    // 从敏感词列表sensitiveWordsLists中随机选取一个敏感词
                    // 并将其追加到jsonObject中的commentTxt字段的值后面，用逗号分隔
                    jsonObject.put("commentTxt", jsonObject.getString("commentTxt") + "," + SensitiveWordsUtils.getRandomElement(sensitiveWordsLists));
                    // 打印一条错误级别的日志信息，显示修改后的jsonObject
                    System.err.println("change commentTxt: " + jsonObject);
                }
                return jsonObject;
            }
        }).uid("xn-sensitive-words").name("xn-sensitive-words");

//        suppleMapDs.print();


        SingleOutputStreamOperator<JSONObject> suppleTimeFieldDs = suppleMapDs.map(new RichMapFunction<JSONObject, JSONObject>() {
//            写 RichMapFunction 的 map 方法，此方法会对 suppleMapDs 数据流里的每个元素开展处理。
            @Override
            public JSONObject map(JSONObject jsonObject){
                // 接着调用 DateTimeUtils 工具类的 format 方法，按照 "yyyyMMdd" 格式对该日期进行格式化。
                // 最后把格式化后的日期字符串作为 "ds" 字段添加到 jsonObject 中。
                jsonObject.put("ds", DateTimeUtils.format(new Date(jsonObject.getLong("ts_ms")), "yyyyMMdd"));
                return jsonObject;
            }
        }).uid("add json ds_xn").name("add json ds_xn");
        suppleTimeFieldDs.print();
//        6> {"msg":"十月稻田大米质量差，口感不佳，不推荐购买。","consignee":"舒炎德","violation_grade":"","user_id":150,"violation_msg":"","is_violation":0,"ts_ms":1746518022784,"ds":"20250506"}

        //专换类型 然后存入kafka
        suppleTimeFieldDs.map(js -> js.toJSONString())
                .sinkTo(
                        KafkaUtils.buildKafkaSink(Constant.KAFKA_BROKERS, Constant.TOPIC_FACT)
                ).uid("xn_db_fact_comment_sink").name("xn_db_fact_comment_sink");



        env.execute();
    }
}
