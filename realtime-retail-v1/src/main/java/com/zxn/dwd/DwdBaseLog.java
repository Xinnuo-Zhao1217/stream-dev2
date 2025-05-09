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
 * @description: DwdBaseLog 日志
 * 通过kafka把flume采集的日志数据 进行进一步的划分，并存入到新的kafka
 */
public class DwdBaseLog {

    // 定义常量
    private static final String START = "start";
    private static final String ERR = "err";
    private static final String DISPLAY = "display";
    private static final String ACTION = "action";
    private static final String PAGE = "page";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_LOG, "dwd_log");
        DataStreamSource<String> kafkaStrDS = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // kafkaStrDS.print();
        // 定义一个输出标签，用于标记脏数据，标签id为"dirtyTag"
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};
        // 对Kafka读取的字符串流进行处理，转换为JSONObject类型的单输出流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                  // 处理函数，接收输入字符串、上下文和输出收集器
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );

//        jsonObjDS.print();
// 获取之前处理过程中标记的脏数据的侧输出流
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.sinkTo(FlinkSinkUtil.getKafkaSink("dirty_data"));

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
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
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

                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        // 如果存在错误信息
                        if (errJsonObj != null) {

                            ctx.output(errTag, jsonObj.toJSONString());

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
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            // 处理曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            // 如果曝光信息数组不为空且有元素
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    JSONObject dispalyJsonObj = displayArr.getJSONObject(i);
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", dispalyJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }

                            // 处理动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }

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

        pageDS.print("页面:");
        errDS.print("错误:");
        startDS.print("启动:");
        displayDS.print("曝光:");
        actionDS.print("动作:");

// 创建一个Map，用于存储不同类型的日志流，方便后续统一处理
        Map<String, DataStream<String>> streamMap = new HashMap<>();
        streamMap.put(ERR, errDS);
        streamMap.put(START, startDS);
        streamMap.put(DISPLAY, displayDS);
        streamMap.put(ACTION, actionDS);
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
