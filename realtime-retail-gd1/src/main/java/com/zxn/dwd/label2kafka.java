package com.zxn.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zxn.util.KafkaUtil;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Flink作业：从Kafka消费用户数据，进行ETL处理并关联用户基本信息和扩展信息
 * 最终将结果输出到下游
 */
public class label2kafka {

    @SneakyThrows
    public static void main(String[] args) {
        // 创建执行环境并设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 从Kafka主题中消费数据
        DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "xinyi_jiao_yw", "label2kafka");

        // 将JSON字符串解析为JSONObject，过滤空数据，并设置事件时间戳
        SingleOutputStreamOperator<JSONObject> kafkaJson = kafkaSource
                .map(JSON::parseObject)
                .filter(data -> !data.isEmpty())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long l) {
                        return jsonObject.getLong("ts_ms"); // 使用消息中的时间戳字段
                    }
                }));


        // 1. 处理用户基本信息数据
        SingleOutputStreamOperator<JSONObject> dbJsonDS1 = kafkaJson
                .filter(json->json.getJSONObject("source").getString("table").equals("user_info")) // 过滤user_info表
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        // 创建结果对象，提取并转换用户基本信息
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("id", after.getString("id"));
                            result.put("uname", after.getString("name"));
                            result.put("user_level", after.getString("user_level"));
                            result.put("login_name", after.getString("login_name"));
                            result.put("phone_num", after.getString("phone_num"));
                            result.put("email", after.getString("email"));
                            result.put("gender", after.getString("gender") != null ? after.getString("gender") : "home");
                            result.put("birthday", after.getString("birthday"));
                            result.put("ts_ms", jsonObject.getLongValue("ts_ms"));

                            // 处理生日字段，计算年龄、年代和星座
                            Integer birthdayStr = after.getInteger("birthday");
                            if (birthdayStr != null) {
                                try {
                                    LocalDate date = LocalDate.ofEpochDay(birthdayStr);
                                    // 格式化生日
                                    result.put("birthday", date.format(DateTimeFormatter.ISO_DATE));
                                    String birthdayStr1 = result.getString("birthday");
                                    String substring = birthdayStr1.substring(0,3);
                                    // 计算年代
                                    result.put("decade", substring + "0");

                                    // 计算年龄
                                    LocalDate birthday = LocalDate.parse(birthdayStr1, DateTimeFormatter.ISO_DATE);
                                    LocalDate currentDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
                                    int age = calculateAge(birthday, currentDate);
                                    result.put("age", age);

                                    // 计算星座
                                    String zodiac = getZodiacSign(birthday);
                                    result.put("zodiac_sign", zodiac);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        return result;
                    }
                });

        // 2. 处理用户扩展信息数据
        SingleOutputStreamOperator<JSONObject> dbJsonDS2 = kafkaJson
                .filter(json->json.getJSONObject("source").getString("table").equals("user_info_sup_msg")) // 过滤user_info_sup_msg表
                .map(new MapFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        // 创建结果对象，提取用户扩展信息
                        JSONObject result = new JSONObject();
                        if (jsonObject.containsKey("after") && jsonObject.getJSONObject("after") != null) {
                            JSONObject after = jsonObject.getJSONObject("after");
                            result.put("uid", after.getString("uid"));
                            result.put("unit_height", after.getString("unit_height"));
                            result.put("create_ts", after.getLong("create_ts"));
                            result.put("weight", after.getString("weight"));
                            result.put("unit_weight", after.getString("unit_weight"));
                            result.put("height", after.getString("height"));
                            result.put("ts_ms", jsonObject.getLong("ts_ms"));
                            return result;
                        }
                        return null;
                    }
                });


        // 3. 双流时间窗口关联：将用户基本信息和扩展信息进行关联
        // 使用interval join，允许时间偏移±30分钟
        SingleOutputStreamOperator<JSONObject> userInfo = dbJsonDS1
                .keyBy(o->o.getString("id"))
                .intervalJoin(dbJsonDS2.keyBy(o->o.getString("uid")))
                .between(Time.minutes(-30), Time.minutes(30))
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        // 当两个流的数据匹配时，合并信息
                        JSONObject result = new JSONObject();
                        if (jsonObject1.getString("id").equals(jsonObject2.getString("uid"))){
                            result.putAll(jsonObject1); // 添加基本信息
                            result.put("height",jsonObject2.getString("height")); // 添加身高
                            result.put("unit_height",jsonObject2.getString("unit_height")); // 添加身高单位
                            result.put("weight",jsonObject2.getString("weight")); // 添加体重
                            result.put("unit_weight",jsonObject2.getString("unit_weight")); // 添加体重单位
                        }
                        collector.collect(result); // 输出关联结果
                    }
                });

        // 打印关联结果（实际生产中可能输出到Kafka或其他存储）
        userInfo.print();

        // 执行Flink作业
        env.execute();
    }

    // 计算年龄的辅助方法
    private static int calculateAge(LocalDate birthDate, LocalDate currentDate) {
        return Period.between(birthDate, currentDate).getYears();
    }

    // 根据生日计算星座的辅助方法
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