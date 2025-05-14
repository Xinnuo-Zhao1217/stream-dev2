//package com.zxn.func;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.zxn.constant.Constant;
//import com.zxn.util.KafkaUtils;
//
//import lombok.SneakyThrows;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Date;
//import java.util.List;
//
///**
// * @Package com.zxn.func.DbusBanBlackListUserInfo2Kafka
// * @Author xinnuo.zhao
// * @Date 2025/5/8 10:46
// * @description: 名单封禁 Task 04
// */
//public class DbusBanBlackListUserInfo2Kafka {
//    @SneakyThrows
//    public static void main(String[] args) {
//        System.setProperty("HADOOP_USER_NAME","root");
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
////        EnvironmentSettingUtils.defaultParameter(env);
//            //从kafka 读取数据
//        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
//                KafkaUtils.buildKafkaSource(
//                        Constant.KAFKA_BROKERS,
//                        Constant.TOPIC_FACT,
//                        new Date().toString(),
//                        OffsetsInitializer.earliest()
//                ),
//                WatermarkStrategy.noWatermarks(),
//                "xy_fact_comment_source"
//        ).uid("xy_fact_comment_source").name("xy_fact_comment_source");
////        4> {"info_original_total_amount":"13654.00","info_activity_reduce_amount":"669.90","commentTxt":"小米电视E65X质量差，频繁蓝屏，售后服务也不好。","info_province_id":30,"info_payment_way":"3501","ds":"20250507","info_create_time":1746659067000,"info_refundable_time":1747263867000,"info_order_status":"1001","id":93,"spu_id":6,"table":"comment_info","info_tm_ms":1746596800871,"op":"c","create_time":1746659107000,"info_user_id":260,"info_op":"c","info_trade_body":"小米电视E65X 65英寸 全面屏 4K超高清HDR 蓝牙遥控内置小爱 2+8GB AI人工智能液晶网络平板电视 L65M5-EA等4件商品","sku_id":20,"server_id":"1","dic_name":"N/A","info_consignee_tel":"13275382153","info_total_amount":"12984.10","info_out_trade_no":"387222756456942","appraise":"1202","user_id":260,"info_id":1132,"info_coupon_reduce_amount":"0.00","order_id":1132,"info_consignee":"苏克","ts_ms":1746596801218,"db":"realtime_v1"}
////        kafkaCdcDbSource.print();
//            //对元素进行一个转换
//        SingleOutputStreamOperator<JSONObject> mapJsonStr = kafkaCdcDbSource.map(JSON::parseObject).uid("to_json_string").name("to_json_string");
////        mapJsonStr.print();
//
//        //使用布隆过滤器
//        SingleOutputStreamOperator<JSONObject> bloomFilterDs = mapJsonStr.keyBy(data -> data.getLong("order_id"))
//                .filter(new FilterBloomDeduplicatorFunc(1000000, 0.01));
////        1> {"info_original_total_amount":"56092.00","info_activity_reduce_amount":"1199.90","commentTxt":"这款iPhone 12质量差，信号不稳定，电池寿命短，非常失望！","info_province_id":8,"info_payment_way":"3501","ds":"20250507","info_create_time":1746624020000,"info_refundable_time":1747228820000,"info_order_status":"1004","id":85,"spu_id":4,"table":"comment_info","info_tm_ms":1746596796334,"info_operate_time":1746624077000,"op":"c","create_time":1746624077000,"info_user_id":178,"info_op":"u","info_trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等6件商品","sku_id":14,"server_id":"1","dic_name":"N/A","info_consignee_tel":"13316189177","info_total_amount":"54892.10","info_out_trade_no":"692358523797933","appraise":"1201","user_id":178,"info_id":1010,"info_coupon_reduce_amount":"0.00","order_id":1010,"info_consignee":"彭永","ts_ms":1746596796319,"db":"realtime_v1"}
////        bloomFilterDs.print();
//        //使用MapCheckRedisSensitiveWordsFunc函数对每个元素进行与 Redis 敏感词检查相关的处理，生成一个新的包含JSONObject类型元素的数据流
//        SingleOutputStreamOperator<JSONObject> SensitiveWordsDs = bloomFilterDs.map(new MapCheckRedisSensitiveWordsFunc())
//                .uid("MapCheckRedisSensitiveWord")
//                .name("MapCheckRedisSensitiveWord");
////        4> {"msg":"小米电视4A 70英寸反应慢，画质差，不值得购买。,原味内衣","consignee":"穆素云","violation_grade":"P0","user_id":26,"violation_msg":"原味内衣","is_violation":1,"ts_ms":1746518019186,"ds":"20250506"}
////        SensitiveWordsDs.print();
//        //对 SensitiveWordsDs 数据流中的每个 JSONObject 元素进行二次敏感词检查，如果 is_violation 字段的值为 0 且找到了敏感词，则更新 violation_grade 和 violation_msg 字段
//        SingleOutputStreamOperator<JSONObject> secondCheckMap = SensitiveWordsDs.map(new RichMapFunction<JSONObject, JSONObject>() {
//            @Override
//            public JSONObject map(JSONObject jsonObject) {
//                if (jsonObject.getIntValue("is_violation") == 0) {
//                    String msg = jsonObject.getString("msg");
//                    List<String> msgSen = SensitiveWordHelper.findAll(msg);
//                    if (msgSen.size() > 0) {
//                        jsonObject.put("violation_grade", "P1");
//                        jsonObject.put("violation_msg", String.join(", ", msgSen));
//                    }
//                }
//                return jsonObject;
//            }
//        }).uid("second sensitive word check").name("second sensitive word check");
//
//        secondCheckMap.print();
////        1> {"msg":"这款iPhone 12质量差，信号不稳定，频繁死机，完全不值这个价！","consignee":"彭永","violation_grade":"","user_id":178,"violation_msg":"","is_violation":0,"ts_ms":1746596796319,"ds":"20250507"}
//
//
//        // 将结果 存入 doris
////        secondCheckMap
////                .map(new BeanToJsonStrMapFunction<>())
////                .sinkTo(FlinkSinkUtil.getDorisSink("xinnuo_result_sensitive_words_user"));
//        env.execute();
//    }
//}
