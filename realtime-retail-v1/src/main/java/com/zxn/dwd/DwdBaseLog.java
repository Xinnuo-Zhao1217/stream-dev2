package com.zxn.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zxn.constant.Constant;
import com.zxn.util.FlinkSinkUtil;
import com.zxn.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * @Package com.zxn.dwd.DwdBaseLog
 * @Author zhao.xinnuo
 * @Date 2025/5/4 18:52
 * @description: DwdBaseLog
 */
public class DwdBaseLog {

    // 定义常量
    private static final String START = "start";
    private static final String ERR = "err";
    private static final String DISPLAY = "display";
    private static final String ACTION = "action";
    private static final String PAGE = "page";

    public static void main(String[] args) throws Exception {
        // 获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为4
        env.setParallelism(4);
        // 启用检查点，间隔为5000毫秒，检查点模式为精确一次
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        // 获取Kafka数据源，使用自定义工具类获取，指定主题为Constant.TOPIC_LOG，消费者组为"dwd_log"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_LOG, "dwd_log");
        // 从Kafka源创建DataStreamSource，不使用水位线，源名称为"Kafka_Source"
        DataStreamSource<String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        // 注释掉的代码，原本功能可能是将Kafka读取的数据打印到控制台，此处先注释
        // kafkaStrDS.print();
        // 定义一个输出标签，用于标记脏数据，标签id为"dirtyTag"
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};
        // 对Kafka读取的字符串流进行处理，转换为JSONObject类型的单输出流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                  // 处理函数，接收输入字符串、上下文和输出收集器
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 将输入的JSON字符串解析为JSONObject对象
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            // 将解析后的JSONObject对象收集到输出流中
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            // 如果解析过程中出现异常，将原始字符串作为脏数据输出到指定标签
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );

//        jsonObjDS.print();
// 获取之前处理过程中标记的脏数据的侧输出流
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
// 将脏数据的侧输出流写入到Kafka的"dirty_data"主题中
        dirtyDS.sinkTo(FlinkSinkUtil.getKafkaSink("dirty_data"));

// 根据JSONObject中"common"对象下的"mid"字段的值对数据流进行分组，得到分组后的流
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

// 对分组后的流进行映射处理
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    // 声明一个状态，用于存储上次访问日期
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 创建一个ValueState描述符，用于描述存储上次访问日期的状态
                        ValueStateDescriptor<String> valueStateDescriptor =
                                new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        // 为状态描述符启用生存时间（TTL）配置，设置生存时间为10秒，更新类型为在创建和写入时更新
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());
                        // 通过运行时上下文获取并初始化lastVisitDateState状态
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        // 这里应该是具体的映射处理逻辑，但代码未完整展示
                        // 后续可能会基于lastVisitDateState状态对输入的jsonObj进行处理并返回
                        return jsonObj;
                    }
                }
        );
//        fixedDS.print();

        //定义侧输出流标签
        // 定义不同类型日志的侧输出标签，用于后续分流操作
// 错误日志的侧输出标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {};
// 启动日志的侧输出标签
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
// 曝光日志的侧输出标签
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
// 动作日志的侧输出标签
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};

// 对fixedDS流进行分流处理，使用ProcessFunction将不同类型的日志数据分流到不同的侧输出流中
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        // 处理错误日志
                        // 从JSON对象中获取错误信息部分
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        // 如果存在错误信息
                        if (errJsonObj != null) {
                            // 将包含错误信息的完整JSON对象以字符串形式输出到错误日志的侧输出流
                            ctx.output(errTag, jsonObj.toJSONString());
                            // 从当前JSON对象中移除错误信息字段，避免后续重复处理
                            jsonObj.remove("err");
                        }

                        // 处理启动日志
                        // 从JSON对象中获取启动信息部分
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        // 如果存在启动信息
                        if (startJsonObj != null) {
                            // 将包含启动信息的完整JSON对象以字符串形式输出到启动日志的侧输出流
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            // 处理页面日志
                            // 从JSON对象中获取公共信息部分
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            // 从JSON对象中获取页面信息部分
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            // 从JSON对象中获取时间戳
                            Long ts = jsonObj.getLong("ts");

                            // 处理曝光日志
                            // 从JSON对象中获取曝光信息数组
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            // 如果曝光信息数组不为空且有元素
                            if (displayArr != null && displayArr.size() > 0) {
                                // 遍历曝光信息数组
                                for (int i = 0; i < displayArr.size(); i++) {
                                    // 获取当前曝光信息对象
                                    JSONObject dispalyJsonObj = displayArr.getJSONObject(i);
                                    // 创建一个新的JSON对象，用于存储组合后的曝光信息
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    // 将公共信息添加到新对象中
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    // 将页面信息添加到新对象中
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    // 将当前曝光信息添加到新对象中
                                    newDisplayJsonObj.put("display", dispalyJsonObj);
                                    // 将时间戳添加到新对象中
                                    newDisplayJsonObj.put("ts", ts);
                                    // 将新对象以字符串形式输出到曝光日志的侧输出流
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                // 从当前JSON对象中移除曝光信息数组，避免后续重复处理
                                jsonObj.remove("displays");
                            }

                            // 处理动作日志
                            // 从JSON对象中获取动作信息数组
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            // 如果动作信息数组不为空且有元素
                            if (actionArr != null && actionArr.size() > 0) {
                                // 遍历动作信息数组
                                for (int i = 0; i < actionArr.size(); i++) {
                                    // 获取当前动作信息对象
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    // 创建一个新的JSON对象，用于存储组合后的动作信息
                                    JSONObject newActionJsonObj = new JSONObject();
                                    // 将公共信息添加到新对象中
                                    newActionJsonObj.put("common", commonJsonObj);
                                    // 将页面信息添加到新对象中
                                    newActionJsonObj.put("page", pageJsonObj);
                                    // 将当前动作信息添加到新对象中
                                    newActionJsonObj.put("action", actionJsonObj);
                                    // 将新对象以字符串形式输出到动作日志的侧输出流
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                // 从当前JSON对象中移除动作信息数组，避免后续重复处理
                                jsonObj.remove("actions");
                            }

                            // 将处理后的JSON对象以字符串形式输出到主输出流，即页面日志流
                            out.collect(jsonObj.toJSONString());
                        }
                    }
                }
        );

// 从pageDS流中获取各个侧输出流
// 获取错误日志的侧输出流
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
// 获取启动日志的侧输出流
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
// 获取曝光日志的侧输出流
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
// 获取动作日志的侧输出流
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);

// 打印各个流的数据，方便调试查看
        pageDS.print("页面:");
        errDS.print("错误:");
        startDS.print("启动:");
        displayDS.print("曝光:");
        actionDS.print("动作:");

// 创建一个Map，用于存储不同类型的日志流，方便后续统一处理
        Map<String, DataStream<String>> streamMap = new HashMap<>();
// 将错误日志流存入Map，键为ERR常量
        streamMap.put(ERR, errDS);
// 将启动日志流存入Map，键为START常量
        streamMap.put(START, startDS);
// 将曝光日志流存入Map，键为DISPLAY常量
        streamMap.put(DISPLAY, displayDS);
// 将动作日志流存入Map，键为ACTION常量
        streamMap.put(ACTION, actionDS);
// 将页面日志流存入Map，键为PAGE常量
        streamMap.put(PAGE, pageDS);

// 将各个流的数据写入对应的Kafka主题
// 将页面日志流的数据写入页面日志对应的Kafka主题
        streamMap
                .get(PAGE)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
// 将错误日志流的数据写入错误日志对应的Kafka主题
        streamMap
                .get(ERR)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
// 将启动日志流的数据写入启动日志对应的Kafka主题
        streamMap
                .get(START)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
// 将曝光日志流的数据写入曝光日志对应的Kafka主题
        streamMap
                .get(DISPLAY)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
// 将动作日志流的数据写入动作日志对应的Kafka主题
        streamMap
                .get(ACTION)
                .sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));

// 执行Flink作业，作业名称为"dwd_log"
        env.execute("dwd_log");
    }



}
