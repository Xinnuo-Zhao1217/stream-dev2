package com.zxn.funcc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.zxn.constant.Constant;
import com.zxn.dim.DimBaseCategory;
import com.zxn.func.AggregateUserDataProcessFunction;
import com.zxn.func.MapDeviceAndSearchMarkModelFunc;
import com.zxn.func.MapDeviceInfoAndSearchKetWordMsgFunc;
import com.zxn.util.ConfigUtils;
import com.zxn.util.EnvironmentSettingUtils;
import com.zxn.util.JdbcUtils;
import com.zxn.util.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Connection;
import java.time.Duration;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;


/**
 * @Package com.label.DbusUserInfo6BaseLabel
 * @Author xinnuo.zhao
 * @Date 2025/5/12 10:01
 * @description: 01 Task 6 BaseLine
 */

public class DbusUserInfo6BaseLabel2Kafka {
    // Kafka连接配置：从常量类获取Kafka集群地址
    private static final String kafka_botstrap_servers = Constant.KAFKA_BROKERS;
    // CDC变更数据主题：存储MySQL数据库变更记录（如用户信息、订单数据）
    private static final String kafka_cdc_db_topic = "realtime_v2_db";
    // 页面日志主题：存储用户行为日志（如浏览、搜索、点击）
    private static final String kafka_page_log_topic = "realtime_v2_page_log";

    // 类目维度数据：存储三级类目关联信息（如"手机→智能手机→安卓手机"）
    private static final List<DimBaseCategory> dim_base_categories;
    // 数据库连接：用于读取维度表数据和写入结果

    private static final Connection connection;
    private static final double device_rate_weight_coefficient = 0.1; // 设备权重系数
    private static final double search_rate_weight_coefficient = 0.15; // 搜索权重系数
    private static final double time_rate_weight_coefficient = 0.1;    // 时间权重系数
    private static final double amount_rate_weight_coefficient = 0.15;    // 价格权重系数
    private static final double brand_rate_weight_coefficient = 0.2;    // 品牌权重系数
    private static final double category_rate_weight_coefficient = 0.3; // 类目权重系数

    // 静态初始化块：在类加载时执行，初始化数据库连接和维度数据
    static {
        try {
            // 1. 获取MySQL数据库连接（使用工具类封装的连接池）
            connection = JdbcUtils.getMySQLConnection(
                    Constant.MYSQL_URL,
                    Constant.MYSQL_USER_NAME,
                    Constant.MYSQL_PASSWORD
            );

            // 2. 查询三级类目关联数据（商品分类体系）
            //    - 从base_category3表（三级类目）关联到base_category2（二级类目）和base_category1（一级类目）
            //    - 例如："手机" → "智能手机" → "安卓手机"
            String sql = "SELECT b3.id,                          \n" +
                    "            b3.name as b3name,              \n" +
                    "            b2.name as b2name,              \n" +
                    "            b1.name as b1name               \n" +
                    "     FROM realtime_v1.base_category3 as b3  \n" +
                    "     JOIN realtime_v1.base_category2 as b2  \n" +
                    "     ON b3.category2_id = b2.id             \n" +
                    "     JOIN realtime_v1.base_category1 as b1  \n" +
                    "     ON b2.category1_id = b1.id";

            // 3. 将查询结果映射为DimBaseCategory对象列表（静态缓存）
            //    - 后续用于商品分类匹配和用户偏好分析
            dim_base_categories = JdbcUtils.queryList2(connection, sql, DimBaseCategory.class, false);
        } catch (Exception e) {
            // 初始化失败时抛出运行时异常（终止程序）
            throw new RuntimeException("初始化数据库连接或维度数据失败", e);
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        // 1. 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为4
        env.setParallelism(4);

        // 2. 从Kafka消费CDC变更数据（用户信息、订单等）
        SingleOutputStreamOperator<String> kafkaCdcDbSource = env.fromSource(
                // 构建Kafka安全数据源（封装了SASL/SSL认证）
                KafkaUtils.buildKafkaSecureSource(
                        kafka_botstrap_servers,        // Kafka集群地址
                        kafka_cdc_db_topic,            // 消费主题（数据库变更日志）
                        new Date().toString(),         // 消费者组ID（使用时间戳确保唯一性）
                        OffsetsInitializer.earliest()  // 从最早的消息开始消费
                ),
                // 设置水位线策略（处理乱序数据）
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                            try {
                                JSONObject jsonObject = JSONObject.parseObject(event);
                                if (jsonObject != null && jsonObject.containsKey("ts_ms")) {
                                    return jsonObject.getLong("ts_ms");  // 使用消息自带时间戳
                                }
                            } catch (Exception e) {

                                e.printStackTrace();
                                System.err.println("解析Kafka消息时间戳失败: " + event);
                            }
                            return 0L;
                        }),
                "kafka_cdc_db_source"
        ).uid("kafka_cdc_db_source").name("Kafka CDC数据源");

        // 从Kafka消费页面日志数据（用户行为记录）
        SingleOutputStreamOperator<String> kafkaPageLogSource = env.fromSource(
                // 构建Kafka安全数据源（与CDC数据源配置一致）
                KafkaUtils.buildKafkaSecureSource(
                        kafka_botstrap_servers,        // Kafka集群地址
                        kafka_page_log_topic,          // 页面日志主题
                        new Date().toString(),         // 消费者组ID（使用时间戳确保唯一性）
                        OffsetsInitializer.earliest()  // 从最早的消息开始消费
                ),
                // 设置水位线策略（与CDC数据源一致，容忍3秒乱序）
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> {
                            try {
                                JSONObject jsonObject = JSONObject.parseObject(event);
                                // 从消息中提取时间戳字段（用于事件时间处理）
                                if (jsonObject != null && jsonObject.containsKey("ts_ms")) {
                                    return jsonObject.getLong("ts_ms");
                                }
                            } catch (Exception e) {
                                // 解析失败时打印错误日志并返回默认值
                                e.printStackTrace();
                                System.err.println("解析页面日志时间戳失败: " + event);
                            }
                            return 0L;  // 默认时间戳（可能导致数据进入"迟到数据"处理逻辑）
                        }),
                "kafka_page_log_source"  // 数据源名称（用于监控和调试）
        ).uid("kafka_page_log_source").name("Kafka页面日志数据源");

        // 将CDC数据从String解析为JSONObject（方便后续处理）
        SingleOutputStreamOperator<JSONObject> dataConvertJsonDs = kafkaCdcDbSource.map(JSON::parseObject)
                .uid("convert_json_cdc_db").name("CDC数据JSON解析");

        // 将页面日志数据从String解析为JSONObject
        SingleOutputStreamOperator<JSONObject> dataPageLogConvertJsonDs = kafkaPageLogSource.map(JSON::parseObject)
                .uid("convert_json_page_log").name("页面日志JSON解析");

        // 从页面日志中提取设备信息和搜索关键词

        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = dataPageLogConvertJsonDs.map(new MapDeviceInfoAndSearchKetWordMsgFunc())
                .uid("extract_device_search").name("提取设备信息和搜索词");

        // 过滤掉UID为空的记录（无效数据）
        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());

        // 按用户ID进行分组（KeyBy操作）
        // 后续操作将按用户ID隔离，确保同一用户的数据由同一并行实例处理
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));

        // 去重处理：过滤同一用户的重复时间戳数据
        // ProcessFilterRepeatTsDataFunc：自定义ProcessFunction，维护每个用户的最新时间戳
        // 目的：避免重复计算，确保数据准确性
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new ProcessFilterRepeatTsDataFunc());
        // 2分钟滚动窗口：按用户ID聚合页面行为数据

        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(data -> data.getString("uid"))
                .process(new AggregateUserDataProcessFunction())
                .keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2)
                .uid("2min_window_aggregation").name("用户行为2分钟窗口聚合");

        // 设备与搜索词打分模型：基于窗口聚合结果计算用户偏好
        SingleOutputStreamOperator<JSONObject> mapDeviceAndSearchRateResultDs = win2MinutesPageLogsDs.map(
                new MapDeviceAndSearchMarkModelFunc(dim_base_categories, device_rate_weight_coefficient, search_rate_weight_coefficient)
        );
        // 过滤用户信息表数据（从CDC变更流中筛选）
        SingleOutputStreamOperator<JSONObject> userInfoDs = dataConvertJsonDs.filter(
                data -> data.getJSONObject("source").getString("table").equals("user_info")
        ).uid("filter_user_info").name("过滤用户信息表");

        // 过滤订单主表数据
        SingleOutputStreamOperator<JSONObject> cdcOrderInfoDs = dataConvertJsonDs.filter(
                data -> data.getJSONObject("source").getString("table").equals("order_info")
        ).uid("filter_order_info").name("过滤订单主表");

        // 过滤订单明细表数据
        SingleOutputStreamOperator<JSONObject> cdcOrderDetailDs = dataConvertJsonDs.filter(
                data -> data.getJSONObject("source").getString("table").equals("order_detail")
        ).uid("filter_order_detail").name("过滤订单明细表");

        // 订单主表数据格式转换（提取关键字段）

        SingleOutputStreamOperator<JSONObject> mapCdcOrderInfoDs = cdcOrderInfoDs.map(new MapOrderInfoDataFunc());

        // 订单明细表数据格式转换（提取关键字段）

        SingleOutputStreamOperator<JSONObject> mapCdcOrderDetailDs = cdcOrderDetailDs.map(new MapOrderDetailFunc());

        // 过滤空ID的订单数据（确保数据完整性）
        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderInfoDs = mapCdcOrderInfoDs.filter(
                data -> data.getString("id") != null && !data.getString("id").isEmpty()
        );
        SingleOutputStreamOperator<JSONObject> filterNotNullCdcOrderDetailDs = mapCdcOrderDetailDs.filter(
                data -> data.getString("order_id") != null && !data.getString("order_id").isEmpty()
        );

        // 按订单ID分组（订单主表按主键ID，订单明细表按外键order_id）
        KeyedStream<JSONObject, String> keyedStreamCdcOrderInfoDs = filterNotNullCdcOrderInfoDs.keyBy(data -> data.getString("id"));
        KeyedStream<JSONObject, String> keyedStreamCdcOrderDetailDs = filterNotNullCdcOrderDetailDs.keyBy(data -> data.getString("order_id"));

        // 订单主表与明细表时间窗口关联（±2分钟容错）

        SingleOutputStreamOperator<JSONObject> processIntervalJoinOrderInfoAndDetailDs = keyedStreamCdcOrderInfoDs.intervalJoin(keyedStreamCdcOrderDetailDs)
                .between(Time.minutes(-2), Time.minutes(2))  // 时间窗口范围（主表事件前后2分钟内的明细表数据）
                .process(new IntervalDbOrderInfoJoinOrderDetailProcessFunc());

        // 订单数据去重：基于明细ID去重（处理CDC的UPDATE/DELETE事件可能导致的重复数据）

        SingleOutputStreamOperator<JSONObject> processDuplicateOrderInfoAndDetailDs = processIntervalJoinOrderInfoAndDetailDs.keyBy(data -> data.getString("detail_id"))
                .process(new processOrderInfoAndDetailFunc());


        // 品类、品牌、年龄、时间打分模型（Base4）

        SingleOutputStreamOperator<JSONObject> mapOrderInfoAndDetailModelDs = processDuplicateOrderInfoAndDetailDs.map(
                new MapOrderAndDetailRateModelFunc(dim_base_categories, time_rate_weight_coefficient, amount_rate_weight_coefficient, brand_rate_weight_coefficient, category_rate_weight_coefficient)
        );

// 处理用户生日格式（从天数转换为日期字符串）

        SingleOutputStreamOperator<JSONObject> finalUserInfoDs = userInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject after = jsonObject.getJSONObject("after");
                if (after != null && after.containsKey("birthday")) {
                    Integer epochDay = after.getInteger("birthday");
                    if (epochDay != null) {
                        LocalDate date = LocalDate.ofEpochDay(epochDay);
                        after.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                    }
                }
                return jsonObject;
            }
        });

// 过滤用户扩展信息表数据（从CDC变更流中筛选）

        SingleOutputStreamOperator<JSONObject> userInfoSupDs = dataConvertJsonDs.filter(data -> data.getJSONObject("source").getString("table").equals("user_info_sup_msg"))
                .uid("filter_user_info_sup").name("过滤用户扩展信息表");

// 转换用户基本信息格式

        SingleOutputStreamOperator<JSONObject> mapUserInfoDs = finalUserInfoDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            // 提取用户ID
                            result.put("uid", after.getString("id"));
                            // 提取用户名
                            result.put("uname", after.getString("name"));
                            // 提取用户等级
                            result.put("user_level", after.getString("user_level"));
                            // 提取登录名
                            result.put("login_name", after.getString("login_name"));
                            // 提取电话号码
                            result.put("phone_num", after.getString("phone_num"));
                            // 提取电子邮件
                            result.put("email", after.getString("email"));
                            // 提取性别，如果性别为空则设置为"home"
                            result.put("gender", after.getString("gender") != null? after.getString("gender") : "home");
                            // 提取生日
                            result.put("birthday", after.getString("birthday"));
                            // 提取时间戳
                            result.put("ts_ms", jsonObject.getLongValue("ts_ms"));

                            String birthdayStr = after.getString("birthday");
                            if (birthdayStr != null && !birthdayStr.isEmpty()) {
                                try {
                                    // 将生日字符串解析为LocalDate对象
                                    LocalDate birthday = LocalDate.parse(birthdayStr, DateTimeFormatter.ISO_DATE);
                                    // 获取当前日期
                                    LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                                    // 计算年龄
                                    int age = calculateAge(birthday, currentDate);
                                    // 计算年代
                                    int decade = birthday.getYear() / 10 * 10;
                                    result.put("decade", decade);
                                    result.put("age", age);
                                    // 计算星座
                                    String zodiac = getZodiacSign(birthday);
                                    result.put("zodiac_sign", zodiac);
                                } catch (Exception e) {
                                    // 如果发生异常，打印堆栈跟踪信息
                                    e.printStackTrace();
                                }
                            }
                        }
                        return result;
                    }
                })
                .uid("map_user_info").name("转换用户基本信息");

// 转换用户扩展信息格式

        SingleOutputStreamOperator<JSONObject> mapUserInfoSupDs = userInfoSupDs.map(new RichMapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) {
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            // 提取用户ID
                            result.put("uid", after.getString("uid"));
                            // 提取身高单位
                            result.put("unit_height", after.getString("unit_height"));
                            // 提取创建时间戳
                            result.put("create_ts", after.getLong("create_ts"));
                            // 提取体重
                            result.put("weight", after.getString("weight"));
                            // 提取体重单位
                            result.put("unit_weight", after.getString("unit_weight"));
                            // 提取身高
                            result.put("height", after.getString("height"));
                            // 提取时间戳
                            result.put("ts_ms", jsonObject.getLong("ts_ms"));
                        }
                        return result;
                    }
                })
                .uid("sup_userinfo_sup").name("sup userinfo sup");

// 过滤掉uid为空的用户基本信息数据

        SingleOutputStreamOperator<JSONObject> finalUserinfoDs = mapUserInfoDs.filter(data -> data.containsKey("uid") &&!data.getString("uid").isEmpty());

// 过滤掉uid为空的用户扩展信息数据

        SingleOutputStreamOperator<JSONObject> finalUserinfoSupDs = mapUserInfoSupDs.filter(data -> data.containsKey("uid") &&!data.getString("uid").isEmpty());

// 按用户ID对用户基本信息数据进行分组

        KeyedStream<JSONObject, String> keyedStreamUserInfoDs = finalUserinfoDs.keyBy(data -> data.getString("uid"));

// 按用户ID对用户扩展信息数据进行分组

        KeyedStream<JSONObject, String> keyedStreamUserInfoSupDs = finalUserinfoSupDs.keyBy(data -> data.getString("uid"));
        // base6Line

        /*
        {"birthday":"1979-07-06","decade":1970,"uname":"鲁瑞","gender":"home","zodiac_sign":"巨蟹座","weight":"52","uid":"302","login_name":"9pzhfy3admw3","unit_height":"cm","user_level":"1","phone_num":"13275315996","unit_weight":"kg","email":"9pzhfy3admw3@gmail.com","ts_ms":1747052360573,"age":45,"height":"164"}
        {"birthday":"2005-08-12","decade":2000,"uname":"潘国","gender":"M","zodiac_sign":"狮子座","weight":"68","uid":"522","login_name":"toim614z6zf","unit_height":"cm","user_level":"1","phone_num":"13648187991","unit_weight":"kg","email":"toim614z6zf@hotmail.com","ts_ms":1747052368281,"age":19,"height":"181"}
        {"birthday":"1997-09-06","decade":1990,"uname":"南宫纨","gender":"F","zodiac_sign":"处女座","weight":"53","uid":"167","login_name":"4tjk9p8","unit_height":"cm","user_level":"1","phone_num":"13913669538","unit_weight":"kg","email":"hij36hcc@3721.net","ts_ms":1747052360467,"age":27,"height":"167"}
        */
        SingleOutputStreamOperator<JSONObject> processIntervalJoinUserInfo6BaseMessageDs = keyedStreamUserInfoDs.intervalJoin(keyedStreamUserInfoSupDs)
                .between(Time.minutes(-5), Time.minutes(5))
                .process(new IntervalJoinUserInfoLabelProcessFunc())
                .uid("process intervalJoin order info")
                .name("process intervalJoin order info");


        processIntervalJoinUserInfo6BaseMessageDs.print();

// 将订单信息和详情模型数据发送到Kafka主题"kafka_label_base4_topic"
//        processIntervalJoinUserInfo6BaseMessageDs.map(data -> data.toJSONString())
//                        .sinkTo(
//                                KafkaUtils.buildKafkaSink(kafka_botstrap_servers,"kafka_label_base6_topic")
//                        );
//// 将订单信息和详情模型数据发送到Kafka主题"kafka_label_base4_topic"
//        mapOrderInfoAndDetailModelDs.map(data -> data.toJSONString())
//                        .sinkTo(
//                                KafkaUtils.buildKafkaSink(kafka_botstrap_servers,"kafka_label_base4_topic")
//                        );
// 将设备和搜索评分结果数据发送到Kafka主题"kafka_label_base2_topic"
//        mapDeviceAndSearchRateResultDs.map(data -> data.toJSONString())
//                        .sinkTo(
//                                KafkaUtils.buildKafkaSink(kafka_botstrap_servers,"kafka_label_base2_topic")
//                        );
// 打印连接后的用户基本信息和扩展信息
        processIntervalJoinUserInfo6BaseMessageDs.print("processIntervalJoinUserInfo6BaseMessageDs: ");
// 打印设备和搜索评分结果数据
        mapDeviceAndSearchRateResultDs.print("mapDeviceAndSearchRateResultDs: ");
// 打印订单信息和详情模型数据
        mapOrderInfoAndDetailModelDs.print("mapOrderInfoAndDetailModelDs: ");



        env.execute("DbusUserInfo6BaseLabel");
    }


    // 计算年龄的方法
    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }

    // 根据出生日期获取星座的方法

    private static String getZodiacSign(LocalDate birthDate) {
        int month = birthDate.getMonthValue();
        int day = birthDate.getDayOfMonth();

        // 星座日期范围定义
        if ((month == 12 && day >= 22) || (month == 1 && day <= 19)) return "摩羯座";
        else if (month == 1 || month == 2 && day <= 18) return "水瓶座";
        else if (month == 2 || month == 3 && day <= 20) return "双鱼座";
        else if (month == 3 || month == 4 && day <= 19) return "白羊座";
        else if (month == 4 || month == 5 && day <= 20) return "金牛座";
        else if (month == 5 || month == 6 && day <= 21) return "双子座";
        else if (month == 6 || month == 7 && day <= 22) return "巨蟹座";
        else if (month == 7 || month == 8 && day <= 22) return "狮子座";
        else if (month == 8 || month == 9 && day <= 22) return "处女座";
        else if (month == 9 || month == 10 && day <= 23) return "天秤座";
        else if (month == 10 || month == 11 && day <= 22) return "天蝎座";
        else return "射手座";

    }
}