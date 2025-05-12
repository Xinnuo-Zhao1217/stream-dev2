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
import java.time.format.DateTimeFormatter;

/**
 * @Package com.zxn.dwd.la
 * @Author zhao.xinnuo
 * @Date 2025/5/12 21:39
 * @description:
 */
public class label2kafka {
        public static SingleOutputStreamOperator<JSONObject> removeSourceFields(SingleOutputStreamOperator<JSONObject> userInfoOutputDS1) {
            return userInfoOutputDS1.map(new MapFunction<JSONObject, JSONObject>() {
                @Override
                public JSONObject map(JSONObject jsonObject) throws Exception {
                    JSONObject after = jsonObject.getJSONObject("after");
                    after.put("op",jsonObject.getString("op"));
                    after.put("ts",jsonObject.getString("ts_ms"));
                    return after;
                }
            });
        }
        public static SingleOutputStreamOperator<JSONObject> assignTimestampsAndWatermarks(SideOutputDataStream<JSONObject> userInfoOutputDS) {
            return userInfoOutputDS.assignTimestampsAndWatermarks(
                    WatermarkStrategy
                            .<JSONObject>forMonotonousTimestamps()
                            .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                                @Override
                                public long extractTimestamp(JSONObject jsonObject, long l) {
                                    return jsonObject.getLong("ts_ms");
                                }
                            })
            );
        }
        @SneakyThrows
        public static void main(String[] args) {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(4);

            DataStreamSource<String> kafkaSource = KafkaUtil.getKafkaSource(env, "xinnuo_zhao_gd1", "label2kafka");

            // 过滤出订单信息
            final OutputTag<JSONObject> orderInfoJsonDS = new OutputTag<JSONObject>("order_info"){};
            // 过滤出购物车信息
            final OutputTag<JSONObject> cartInfoJsonDS = new OutputTag<JSONObject>("cart_info"){};
            // 过滤出订单详情信息
            final OutputTag<JSONObject> orderDetailJsonDS = new OutputTag<JSONObject>("order_detail"){};
            // 过滤出评论信息
            final OutputTag<JSONObject> commentInfoJsonDS = new OutputTag<JSONObject>("comment_info"){};
            // 过滤出收藏信息
            final OutputTag<JSONObject> favorInfoJsonDS = new OutputTag<JSONObject>("favor_info"){};
            final OutputTag<JSONObject> userInfoSupMsgJsonDS = new OutputTag<JSONObject>("user_info_sup_msg"){};
            final OutputTag<JSONObject> userInfoJsonDS = new OutputTag<JSONObject>("user_info"){};

            SingleOutputStreamOperator<Object> dbJsonDS = kafkaSource.map(JSON::parseObject).process(new ProcessFunction<JSONObject, Object>() {
                @Override
                public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, Object>.Context context, Collector<Object> collector) throws Exception {
                    String table = jsonObject.getJSONObject("source").getString("table");
                    if (table.equals("order_info")) {
                        context.output(orderInfoJsonDS, jsonObject);
                    } else if (table.equals("order_detail")) {
                        context.output(orderDetailJsonDS, jsonObject);
                    } else if (table.equals("cart_info")) {
                        context.output(cartInfoJsonDS, jsonObject);
                    } else if (table.equals("comment_info")) {
                        context.output(commentInfoJsonDS, jsonObject);
                    } else if (table.equals("favor_info")) {
                        context.output(favorInfoJsonDS, jsonObject);
                    }else if (table.equals("user_info_sup_msg")) {
                        context.output(userInfoSupMsgJsonDS, jsonObject);
                    }else if (table.equals("user_info")) {
                        context.output(userInfoJsonDS, jsonObject);
                    }
                    collector.collect(jsonObject);
                }
            });
            // 获取测流数据
            SideOutputDataStream<JSONObject> orderInfoOutputDS = dbJsonDS.getSideOutput(orderInfoJsonDS);
            SideOutputDataStream<JSONObject> cartInfoOutputDS = dbJsonDS.getSideOutput(cartInfoJsonDS);
            SideOutputDataStream<JSONObject> orderDetailOutputDS = dbJsonDS.getSideOutput(orderDetailJsonDS);
            SideOutputDataStream<JSONObject> commentInfoOutputDS = dbJsonDS.getSideOutput(commentInfoJsonDS);
            SideOutputDataStream<JSONObject> favorInfoOutputDS = dbJsonDS.getSideOutput(favorInfoJsonDS);
            SideOutputDataStream<JSONObject> userInfoSupMsgOutputDS = dbJsonDS.getSideOutput(userInfoSupMsgJsonDS);
            SideOutputDataStream<JSONObject> userInfoOutputDS = dbJsonDS.getSideOutput(userInfoJsonDS);

//水位线
            SingleOutputStreamOperator<JSONObject> orderInfoOutputDS1 = assignTimestampsAndWatermarks(orderInfoOutputDS);
            SingleOutputStreamOperator<JSONObject> cartInfoOutputDS1 = assignTimestampsAndWatermarks(cartInfoOutputDS);
            SingleOutputStreamOperator<JSONObject> orderDetailOutputDS1 = assignTimestampsAndWatermarks(orderDetailOutputDS);
            SingleOutputStreamOperator<JSONObject> commentInfoOutputDS1 = assignTimestampsAndWatermarks(commentInfoOutputDS);
            SingleOutputStreamOperator<JSONObject> favorInfoOutputDS1 = assignTimestampsAndWatermarks(favorInfoOutputDS);
            SingleOutputStreamOperator<JSONObject> userInfoSupMsgOutputDS1 = assignTimestampsAndWatermarks(userInfoSupMsgOutputDS);
            SingleOutputStreamOperator<JSONObject> userInfoOutputDS1 = assignTimestampsAndWatermarks(userInfoOutputDS);

            // 对字段进行优化（过滤）
            SingleOutputStreamOperator<JSONObject> userInfoOutputDS2 = removeSourceFields(userInfoOutputDS1);
            SingleOutputStreamOperator<JSONObject> userInfoSupMsgOutputDS2 = removeSourceFields(userInfoSupMsgOutputDS1);
            SingleOutputStreamOperator<JSONObject> favorInfoOutputDS2 = removeSourceFields(favorInfoOutputDS1);
            SingleOutputStreamOperator<JSONObject> commentInfoOutputDS2 = removeSourceFields(commentInfoOutputDS1);
            SingleOutputStreamOperator<JSONObject> orderDetailOutputDS12 = removeSourceFields(orderDetailOutputDS1);
            SingleOutputStreamOperator<JSONObject> cartInfoOutputDS2 = removeSourceFields(cartInfoOutputDS1);
            SingleOutputStreamOperator<JSONObject> orderInfoOutputDS2 = removeSourceFields(orderInfoOutputDS1);

            // 异步io
            SingleOutputStreamOperator<JSONObject> userInfo = userInfoOutputDS2
                    .keyBy(o->o.getString("id"))
                    .intervalJoin(userInfoSupMsgOutputDS2.keyBy(o->o.getString("uid")))
                    .between(Time.minutes(-30), Time.minutes(30))
                    .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                        @Override
                        public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                            // 对数据进行转换，如用户的生日进行转换
                            if (jsonObject1 != null && jsonObject1.containsKey("birthday")) {
                                Integer epochDay = jsonObject1.getInteger("birthday");
                                if (epochDay != null) {
                                    LocalDate date = LocalDate.ofEpochDay(epochDay);
                                    jsonObject1.put("birthday", date.format(DateTimeFormatter.ISO_DATE));

                                }
                            }
                            if (jsonObject1 != null) {
                                jsonObject1.put("user_info_sup_msg", jsonObject2);
                                String string = jsonObject1.getString("birthday");
                                String substring = string.substring(0,3);
                                String substring1 = string.substring(0,4);

                                System.out.println(substring1);
                                // 获取年份
                                jsonObject1.put("nianDai", substring + "0");

                                // 获取年龄
                                jsonObject1.put("age", substring1);
                                jsonObject1.put("age",LocalDate.now().getYear()-jsonObject1.getInteger("age"));

                                // 获取性别
                                if(jsonObject1.getString("gender")==null){
                                    jsonObject1.put("gender","H");
                                }
                            }
                            collector.collect(jsonObject1);

                        }
                    });
            userInfo.print();

            env.execute();
        }
}
