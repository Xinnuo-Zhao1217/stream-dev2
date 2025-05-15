package com.zxn.dwd;

import com.alibaba.fastjson.JSONObject;
import com.zxn.constant.Constant;
import com.zxn.func.*;
import com.zxn.util.EnvironmentSettingUtils;
import com.zxn.util.FlinkSourceUtil;
import com.zxn.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;

/**
 * @Package com.zxn.dwd.DwdAge
 * @Author zhao.xinnuo
 * @Date 2025/5/15 9:39
 * @description: DwdAge
 */
public class DwdAge {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(1);

        //TODO 品类   订单 --> 明细&用户- -> sku --> 品牌 --> 三级品类 --> 二级品类 --> 一级品类 --> 日志
        //关联所有的表，求出每个uid年龄的判断条件（json类型），根据年龄段求出总和，年龄段进行修改

        //TODO 1.读取Kafka数据源
        //获取 DWD_APP 数据
        //{"birthday":"1990-12-28","create_time":1745852862000,"gender":"M","weight":"65","ageGroup":"30-34","constellation":"摩羯座","unit_height":"cm","Era":"1990年代","name":"戴振","id":680,"unit_weight":"kg","ts_ms":1747019493924,"age":33,"height":"174"}
        DataStreamSource<String> AppSource = env.fromSource(FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, Constant.TOPIC_DB),
                // 设置水位线
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    if (event != null){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ), "kafka_source");
        // 转为JSON格式
        SingleOutputStreamOperator<JSONObject> User = AppSource.map(JSONObject::parseObject);
        //获取ods事实表数据
        //DbSource:{"before":{"id":1,"tm_name":"Redmi2","logo_url":"111","create_time":1639440000000,"operate_time":null},"after":{"id":1,"tm_name":"Redmi","logo_url":"111","create_time":1639440000000,"operate_time":null},"source":{"version":"1.9.7.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1747019648000,"snapshot":"false","db":"gmall2024","sequence":null,"table":"base_trademark","server_id":1,"gtid":null,"file":"mysql-bin.000033","pos":33631,"row":0,"thread":104,"query":null},"op":"u","ts_ms":1747019648229,"transaction":null}
        DataStreamSource<String> Db = env.fromSource(FlinkSourceUtil.getKafkaSource("realtime_v1", "ods_db"),
                // 设置水位线
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    if (event != null){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts_ms");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ), "kafka_source");
        SingleOutputStreamOperator<JSONObject> DbSource = Db.map(JSONObject::parseObject).filter(o -> !o.getString("op").equals("d"));
        //获取ods日志数据
        //{"common":{"ar":"230000","ba":"Xiaomi","ch":"xiaomi","is_new":"1","md":"Xiaomi 9","mid":"mid_482509","os":"Android 10.0","uid":"443","vc":"v2.1.134"},"page":{"during_time":12740,"last_page_id":"mine","page_id":"orders_unpaid"},"ts":1747031731000}
        DataStreamSource<String> LogSource = env.fromSource(FlinkSourceUtil.getKafkaSource("ods_log", "ods_log"),
                // 设置水位线
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                                    if (event != null){
                                        try {
                                            return JSONObject.parseObject(event).getLong("ts");
                                        }catch (Exception e){
                                            e.printStackTrace();
                                            System.err.println("Failed to parse event as JSON or get ts: " + event);
                                            return 0L;
                                        }
                                    }
                                    return 0L;
                                }
                        ), "kafka_source");

        //TODO 2.过滤出 order_info,order_detail 表  日志表：搜索词,设备信息
        //过滤搜索词 和设备信息
        //{"uid":"128","os":"Android","keyword":"新款","ts":1747031730000}
        SingleOutputStreamOperator<JSONObject> pageLog = LogSource.map(JSONObject::parseObject).map(new LogFilterFunc())
                .uid("filter page log").name("filter page log");

        // 去重
        // {"uid":"247","os":"iOS","keyword":"书籍","ts":1747031731000}
//        SingleOutputStreamOperator<JSONObject> BloomLog = pageLog.keyBy(o -> o.getString("uid"))
//                .filter(new PublicFilterBloomFunc(1000000, 0.0001, "uid", "ts"))
//                .uid("Filter Log Bloom").name("Filter Log Bloom");

        //{"uid":"51","os":"Android","ts":1747031723000}
        SingleOutputStreamOperator<JSONObject> newestLog = pageLog.keyBy(json -> json.getInteger("uid"))
                .process(new LatestRecordProcessFunction())
                .filter(o -> o.getString("keyword") != null && o.containsKey("keyword") )
                .setParallelism(1).uid("Newest Log ").name("Newest Log");

//        newestLog.print("log-->");

        // 过滤出 order_info 表
        SingleOutputStreamOperator<JSONObject> OrderInfo = DbSource.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"))
                .uid("filter_order_info").name("filter_order_info");

        // 去重数据
        //info-->: {"op":"r","after":{"payment_way":"3501","refundable_time":1747386484000,"original_total_amount":8197.0,"order_status":"1003","consignee_tel":"13754128222","trade_body":"Apple iPhone 12 (A2404) 64GB 白色 支持移动联通电信5G 双卡双待手机等1件商品","id":3555,"operate_time":1746781692000,"consignee":"萧予","create_time":1746781684000,"expire_time":1746782284000,"coupon_reduce_amount":0.0,"out_trade_no":"315795359827319","total_amount":8197.0,"user_id":865,"province_id":8,"activity_reduce_amount":0.0},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall2024","table":"order_info"},"ts_ms":1747019484283}
        SingleOutputStreamOperator<JSONObject> BloomInfo = OrderInfo.keyBy(o -> o.getJSONObject("after").getInteger("id"))
                .filter(new PublicFilterBloomFunc(1000000, 0.0001, "id", "ts_ms"))
                .uid("Filter info Bloom").name("Filter info Bloom");
//        BloomInfo.print("info-->");

        // 过滤出 order_detail 表
        SingleOutputStreamOperator<JSONObject> OrderDetail = DbSource.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"))
                .uid("filter_order_detail").name("filter_order_detail");

        // 去重数据
        //detail-->:{"op":"r","after":{"sku_num":1,"create_time":1746801886000,"split_coupon_amount":30.0,"sku_id":26,"sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Y01复古红 百搭气质 璀璨金钻哑光唇膏 ","order_price":129.0,"id":5062,"order_id":3602,"split_activity_amount":0.0,"split_total_amount":99.0},"source":{"server_id":0,"version":"1.9.7.Final","file":"","connector":"mysql","pos":0,"name":"mysql_binlog_source","row":0,"ts_ms":0,"snapshot":"false","db":"gmall2024","table":"order_detail"},"ts_ms":1747019482665}
        SingleOutputStreamOperator<JSONObject> BloomDetail = OrderDetail.keyBy(o -> o.getJSONObject("after").getInteger("id"))
                .filter(new PublicFilterBloomFunc(1000000, 0.0001, "id", "ts_ms"))
                .uid("Filter detail Bloom").name("Filter detail Bloom");
//        BloomDetail.print("detail-->");

        // 过滤出 category_compare_dic 表
        // {"op":"c","after":{"category_name":"保健品","search_category":"健康与养生","id":8},"source":{"thread":161,"server_id":1,"version":"1.9.7.Final","file":"mysql-bin.000036","connector":"mysql","pos":2310633,"name":"mysql_binlog_source","row":0,"ts_ms":1747222192000,"snapshot":"false","db":"gmall2024","table":"category_compare_dic"},"ts_ms":1747222191269}
        SingleOutputStreamOperator<JSONObject> CategoryCompare = DbSource
                .filter(data -> data.getJSONObject("source").getString("table").equals("category_compare_dic") && data.getJSONObject("after") != null)
                .uid("filter_category_compare_dic").name("filter_category_compare_dic");
//        CategoryCompare.print("CategoryCompare-->");
        //TODO 3.关联

        // detail & info
        //4> {"create_time":1746809812000,"user_id":892,"total_amount":8197.0,"sku_id":8,"id":5078,"order_id":3615}
        SingleOutputStreamOperator<JSONObject> intervalDetailInfo = BloomDetail
                .keyBy(o -> o.getJSONObject("after").getInteger("order_id"))
                .intervalJoin(BloomInfo.keyBy(o -> o.getJSONObject("after").getInteger("id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject object = new JSONObject();
                        // detail 所需表字段
                        object.put("id", jsonObject.getJSONObject("after").getInteger("id"));
                        object.put("sku_id", jsonObject.getJSONObject("after").getInteger("sku_id"));
                        object.put("order_id", jsonObject.getJSONObject("after").getInteger("order_id"));
                        object.put("create_time", jsonObject.getJSONObject("after").getLong("create_time"));

                        // info 所需表字段
                        object.put("user_id", jsonObject2.getJSONObject("after").getInteger("user_id"));
                        object.put("original_total_amount", jsonObject2.getJSONObject("after").getDouble("original_total_amount"));
                        collector.collect(object);
                    }
                }).setParallelism(1).uid("intervalJoin detail & info").name("intervalJoin detail & info");

        //去重
        //{"create_time":1746828708000,"user_id":551,"total_amount":9787.0,"sku_id":31,"id":5196,"order_id":3702}
        SingleOutputStreamOperator<JSONObject> BloomIntervalDetailInfo = intervalDetailInfo.keyBy(o -> o.getInteger("id"))
                .filter(new PublicFilterBloomFunc(1000000, 0.0001, "id", "create_time"));

        // new & sku 异步IO
        //joinNewSku-->:{"create_time":1746775813000,"tm_id":8,"user_id":222,"total_amount":99.0,"sku_id":28,"sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Z03女王红 性感冷艳 璀璨金钻哑光唇膏 ","id":4969,"order_id":3537,"category3_id":477}
        SingleOutputStreamOperator<JSONObject> joinNewSku = BloomIntervalDetailInfo.map(new RichMapFunction<JSONObject, JSONObject>() {
            private Connection HBaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                HBaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(HBaseConn);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 获取skuId
                Integer skuId = jsonObject.getInteger("sku_id");
                // 获取对应 skuId 对应的 HBase 数据
                JSONObject row = HBaseUtil.getRow(HBaseConn, Constant.HBASE_NAMESPACE, "dim_sku_info", skuId.toString(), JSONObject.class);
                if (row != null) {
                    jsonObject.put("category3_id", row.getInteger("category3_id"));
                    jsonObject.put("tm_id", row.getInteger("tm_id"));
                }

                return jsonObject;
            }
        }).setParallelism(1).uid("join new & sku").name("join new & sku");
//        joinNewSku.print("joinNewSku-->");

        // new & user
        //user-->: {"user":{"birthday":"1996-05-08","create_time":1654646400000,"gender":"M","weight":"53","ageGroup":"25-29","constellation":"金牛座","unit_height":"cm","Era":"1990年代","name":"欧阳茂进","id":13,"unit_weight":"kg","ts_ms":1747019493901,"age":29,"height":"151"},"Age":{"create_time":1746787559000,"tm_id":7,"user_id":13,"total_amount":18509.0,"sku_id":24,"sku_name":"金沙河面条 原味银丝挂面 龙须面 方便速食拉面 清汤面 900g","id":5019,"order_id":3576,"category3_id":803}}
        SingleOutputStreamOperator<JSONObject> joinNewUser = joinNewSku.keyBy(o -> o.getInteger("user_id"))
                .intervalJoin(User.keyBy(o -> o.getInteger("id")))
                .between(Time.seconds(-60), Time.seconds(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {

                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        JSONObject object = new JSONObject();
                        object.put("Age",jsonObject);
                        object.put("user", jsonObject2);
                        collector.collect(object);
                    }
                }).setParallelism(1).uid("intervalJoin new & user").name("intervalJoin new & user");
//        joinNewUser.print("joinNewUser-->");

        // 关联base_trademark(tm_name) & base_category3(c2_id)
        // {"user":{"birthday":"1993-07-07","create_time":1744054226000,"gender":"M","weight":"58","ageGroup":"30-34","constellation":"巨蟹座","unit_height":"cm","Era":"1990年代","name":"郑彪博","id":122,"unit_weight":"kg","ts_ms":1747019493904,"age":30,"height":"158"},"Age":{"category_name":"手机","create_time":1746826754000,"sku_id":6,"tm_name":"Redmi","category1_id":2,"tm_id":1,"user_id":122,"total_amount":1299.0,"sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 8GB+128GB 冰雾白 游戏智能手机 小米 红米","id":5152,"order_id":3669,"category3_id":61,"category2_id":13}}
        SingleOutputStreamOperator<JSONObject> joinNewTradeCate = joinNewUser.map(new RichMapFunction<JSONObject, JSONObject>() {
            private Connection HBaseConn;

            @Override
            public void open(Configuration parameters) throws Exception {
                HBaseConn = HBaseUtil.getHBaseConnection();
            }

            @Override
            public void close() throws Exception {
                HBaseUtil.closeHBaseConnection(HBaseConn);
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                // 获取 c3Id
                JSONObject age = jsonObject.getJSONObject("Age");
                Integer category3_id = age.getInteger("category3_id");
                // 获取对应 c3Id 对应的 HBase 数据
                JSONObject row1 = HBaseUtil.getRow(HBaseConn, Constant.HBASE_NAMESPACE, "dim_base_category3", category3_id.toString(), JSONObject.class);
                if (row1 != null) {
                    age.put("category2_id", row1.getInteger("category2_id"));
                }

                // 获取 tmId
                Integer tmId = age.getInteger("tm_id");
                // 获取对应 c3Id 对应的 HBase 数据
                JSONObject row2 = HBaseUtil.getRow(HBaseConn, Constant.HBASE_NAMESPACE, "dim_base_trademark", tmId.toString(), JSONObject.class);
                if (row2 != null) {
                    age.put("tm_name", row2.getString("tm_name"));
                }

                // 获取 c2Id
                Integer category2_id = age.getInteger("category2_id");
                // 获取对应 c2Id 对应的 HBase 数据
                JSONObject row3 = HBaseUtil.getRow(HBaseConn, Constant.HBASE_NAMESPACE, "dim_base_category2", category2_id.toString(), JSONObject.class);
                if (row3 != null) {
                    age.put("category1_id", row3.getInteger("category1_id"));
                }

                // 获取 c1Id
                Integer category1_id = age.getInteger("category1_id");
                // 获取对应 c2Id 对应的 HBase 数据
                JSONObject row4 = HBaseUtil.getRow(HBaseConn, Constant.HBASE_NAMESPACE, "dim_base_category1", category1_id.toString(), JSONObject.class);
                if (row4 != null) {
                    age.put("category_name", row4.getString("name"));
                }

                return jsonObject;
            }
        }).setParallelism(1).uid("join new & trademark & category ").name("join new & trademark & category ");
//        joinNewTradeCate.print("joinNewTradeCate-->");

        // 关联日志表
        // {"user":{"birthday":"1998-09-09","create_time":1746826137000,"gender":"M","weight":"61","ageGroup":"25-29","constellation":"处女座","unit_height":"cm","Era":"1990年代","name":"杨福","id":972,"unit_weight":"kg","ts_ms":1747019493934,"age":25,"height":"152"},"Age":{"category_name":"手机","create_time":1746826196000,"os":"Android","sku_id":5,"tm_name":"Redmi","category1_id":2,"tm_id":1,"user_id":972,"total_amount":999.0,"sku_name":"Redmi 10X 4G Helio G85游戏芯 4800万超清四摄 5020mAh大电量 小孔全面屏 128GB大存储 4GB+128GB 明月灰 游戏智能手机 小米 红米","id":5171,"order_id":3681,"category3_id":61,"category2_id":13,"ts":1747031715000}}
        SingleOutputStreamOperator<JSONObject> joinAll = joinNewTradeCate.keyBy(o -> o.getJSONObject("Age").getInteger("user_id"))
                .intervalJoin(newestLog.keyBy(o -> o.getInteger("uid")))
                .between(Time.minutes(-60), Time.minutes(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            JSONObject age = jsonObject.getJSONObject("Age");
                            age.put("os", jsonObject2.getString("os"));
                            age.put("keyword", jsonObject2.getString("keyword"));
                            age.put("ts", jsonObject2.getLong("ts"));
                            collector.collect(jsonObject);
                        }catch (Exception e){
                            e.printStackTrace();
                            System.out.println("报错信息 " +  jsonObject);
                        }


                    }
                }).setParallelism(1).uid("intervalJoin new & bloomLog").name("intervalJoin new & bloomLog");
//        joinAll.print("joinAll-->");


        // 对时间段和价格敏感度做处理
        // {"user":{"birthday":"2007-01-09","create_time":1746790830000,"gender":"M","weight":"60","ageGroup":"18-24","constellation":"摩羯座","unit_height":"cm","Era":"2000年代","name":"东郭宁","id":955,"unit_weight":"kg","ts_ms":1747019493933,"age":18,"height":"183"},"Age":{"category_name":"个护化妆","create_time":1746790919000,"os":"Android","sku_id":30,"price_sensitive":"高价商品","tm_name":"CAREMiLLE","category1_id":8,"tm_id":9,"user_id":955,"total_amount":21667.2,"sku_name":"CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎唇膏 M02干玫瑰","id":5041,"order_id":3588,"category3_id":477,"category2_id":54,"time_period":"晚上","ts":1747031731000}}
        SingleOutputStreamOperator<JSONObject> timePriceMap = joinAll.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                if (jsonObject.getJSONObject("Age") != null) {
                    JSONObject age = jsonObject.getJSONObject("Age");

                    // 时间段
                    age.put("time_period", JudgmentFunc.timePeriodJudgment(age.getLong("create_time")));

                    // 价格敏感度
                    age.put("price_sensitive", JudgmentFunc.priceCategoryJudgment(age.getDouble("original_total_amount")));
                }
                return jsonObject;
            }
        }).setParallelism(1).uid("timePriceMap").name("timePriceMap");
//        timePriceMap.print("timePriceMap-->");

        // 关联搜索词分类表
        // {"user":{"birthday":"1997-07-15","gender":"F","weight":"53","ageGroup":"25-29","constellation":"巨蟹座","unit_height":"cm","Era":"1990年代","name":"蒋枝思","id":338,"unit_weight":"kg","ts_ms":1747221450430,"age":26,"height":"154"},"Age":{"category_name":"个护化妆","create_time":1747248448000,"os":"Android","sku_id":28,"price_sensitive":"高价商品","tm_name":"索芙特","category1_id":8,"tm_id":8,"user_id":338,"total_amount":8596.0,"id":5377,"keyword":"智能手机","order_id":3827,"category3_id":477,"category2_id":54,"time_period":"凌晨","ts":1747221577000},"keyword_type":"科技与数码"}
        SingleOutputStreamOperator<JSONObject> joinSearchCate = timePriceMap.keyBy(o -> o.getJSONObject("Age").getString("keyword"))
                .intervalJoin(CategoryCompare.keyBy(o -> o.getJSONObject("after").getString("category_name")))
                .between(Time.minutes(-60), Time.minutes(60))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        if (jsonObject == null || jsonObject2 == null) {
                            return; // 或者记录日志
                        }

                        JSONObject age = jsonObject.getJSONObject("Age");
                        if (age == null) {
                            return;
                        }

                        JSONObject after = jsonObject2.getJSONObject("after");
                        if (after == null || !after.containsKey("search_category")) {
                            return;
                        }

                        String searchCategory = after.getString("search_category");
                        if (searchCategory == null) {
                            return;
                        }

                        try {
                            age.put("keyword_type", searchCategory);
                            collector.collect(jsonObject);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("报错信息 " + searchCategory);
                        }
                    }
                }).setParallelism(1).uid("intervalJoin new & searchCate").name("intervalJoin new & searchCate");

//        joinSearchCate.print("joinSearchCate-->");

        //去重
//        SingleOutputStreamOperator<JSONObject> searchCateBloom = joinSearchCate.keyBy(o -> o.getJSONObject("user").getInteger("id"))
//                .filter(new PublicFilterBloomFunc(1000000, 0.0001, "id", "ts_ms"))
//                .uid("Filter SearchCate Bloom").name("Filter SearchCate Bloom");

//        SingleOutputStreamOperator<JSONObject> winSearchCate = joinSearchCate.keyBy(o -> o.getJSONObject("user").getInteger("id"))
//                .window(TumblingEventTimeWindows.of(Time.minutes(2)))
//                .reduce((value1, value2) -> value2)
//                .uid("reduce SearchCate Bloom")
//                .name("reduce SearchCate Bloom");

        SingleOutputStreamOperator<JSONObject> filter = joinSearchCate.filter(o -> o.getJSONObject("user").getString("ageGroup").equals("18-24"));
        // 计算各个维度的分数
        //AgeEachScore-->> {"user":{"birthday":"1977-02-14","gender":"home","weight":"77","ageGroup":"40-49","constellation":"水瓶座","unit_height":"cm","Era":"1970年代","name":"伏聪澜","id":997,"unit_weight":"kg","ts_ms":1747221454365,"age":48,"height":"159"},"Age":{"category_name":"手机","create_time":1747226824000,"os":"iOS","sku_id":12,"price_sensitive":"高价商品","original_total_amount":9197.0,"keyword_type":"健康与养生","tm_name":"苹果","category1_id":2,"tm_id":2,"user_id":997,"id":5314,"keyword":"保健品","order_id":3782,"category3_id":61,"category2_id":13,"time_period":"晚上","ts":1747221566000},"ageScore":{"tm_score":0.04000000000000001,"price_sensitive_score":0.075,"time_period_score":0.04000000000000001,"category_score":0.27,"os_score":0.03,"keyword_type_score":0.12}}
        SingleOutputStreamOperator<JSONObject> AgeEachScore = filter.map(new AgeScoreFunc());
//        AgeEachScore.print("AgeEachScore-->");

        // 求和
        //{"user":{"birthday":"2001-07-15","gender":"home","weight":"40","ageGroup":"18-24","constellation":"巨蟹座","unit_height":"cm","Era":"2000年代","name":"沈娣","id":221,"unit_weight":"kg","new_ageGroup":"40-49","ts_ms":1747222186982,"age":22,"height":"159"},"Age":{"category_name":"电脑办公","create_time":1747236324000,"os":"Android","sku_id":15,"price_sensitive":"高价商品","original_total_amount":9799.0,"keyword_type":"健康与养生","tm_name":"联想","category1_id":6,"tm_id":3,"user_id":221,"id":5581,"keyword":"健康食品","order_id":3970,"category3_id":287,"category2_id":33,"time_period":"夜间","ts":1747221577000},"ageScore":{"tm_score":0.18,"price_sensitive_score":0.015,"time_period_score":0.09,"category_score":0.06,"os_score":0.08,"TotalScore":0.440,"keyword_type_score":0.015}}
        SingleOutputStreamOperator<JSONObject> TotalScore = AgeEachScore.map(new ScoreCalculator());

        TotalScore.print();



        env.execute("DwdAge");
    }




    // 计算加权求和
    public static class ScoreCalculator implements MapFunction<JSONObject, JSONObject> {
        @Override
        public JSONObject map(JSONObject value) throws Exception {
            JSONObject ageScore = value.getJSONObject("ageScore");
            JSONObject user = value.getJSONObject("user");
            if (ageScore == null) return value;


            // 使用BigDecimal保证精度
            BigDecimal total = BigDecimal.ZERO;
            for (String key : ageScore.keySet()) {
                if ("TotalScore".equals(key)) continue;

                Object val = ageScore.get(key);
                if (val instanceof Number) {
                    // 通过字符串转换避免精度丢失
                    BigDecimal num = new BigDecimal(val.toString());
                    total = total.add(num);
                }
            }

            // 四舍五入保留3位小数
            total = total.setScale(3, RoundingMode.HALF_UP);
            Double aDouble = new Double(total.doubleValue());

//            System.out.println(aDouble + "double");
//            System.out.println(total + "BigDecimal");

            if (aDouble >= 0.75){
                user.put("new_ageGroup", "18-24");
            } else if (aDouble >= 0.69) {
                user.put("new_ageGroup", "25-29");
            } else if (aDouble >= 0.585) {
                user.put("new_ageGroup", "30-34");
            } else if (aDouble >= 0.47) {
                user.put("new_ageGroup", "35-39");
            } else if (aDouble >= 0.365) {
                user.put("new_ageGroup", "40-49");
            } else if (aDouble >= 0.26) {
                user.put("new_ageGroup", "50+");
            }

            // 更新 TotalScore
            ageScore.put("TotalScore", total);
            return value;
        }
    }
}

