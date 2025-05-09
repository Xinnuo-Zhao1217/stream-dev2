package com.zxn.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zxn.bean.CartAddUuBean;
import com.zxn.fonction.BeanToJsonStrMapFunction;
import com.zxn.util.DateFormatUtil;
import com.zxn.util.FlinkSinkUtil;
import com.zxn.util.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.zxn.dws.DwsTradeCartAddUuWindow
 * @Author zhao.xinnuo
 * @Date 2025/5/5 14:54
 * @description:实现了一个实时统计不同时间窗口内去重加购用户数的功能，并将结果存储到 Doris 数据库中，
 * 以便后续的数据分析和处理
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);


        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 从自定义工具类FlinkSourceUtil中获取Kafka数据源，指定Kafka主题为"dwd_trade_cart_add_xinnuo_zhao"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_xinyi_jiao_add", "dws_trade_cart_add_uu_window");

        // 从Kafka数据源读取数据，创建一个DataStreamSource对象，指定水位线策略为不生成水位线
        // 并为数据源命名为"Kafka_Source"
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");


        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts_ms") * 1000;
                                    }
                                }
                        )
        );


        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));

        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 定义一个状态变量，用于存储用户上次加购的日期
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) {

                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastCartDateState", String.class);

                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());

                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                        String lastCartDate = lastCartDateState.value();

                        Long ts = jsonObj.getLong("ts_ms");
                        // 将时间戳转换为日期字符串
                        String curCartDate = DateFormatUtil.tsToDate(ts);

                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {

                            out.collect(jsonObj);

                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );

        // 对过滤后的加购记录进行全量窗口操作，使用滚动事件时间窗口，窗口大小为2秒
        AllWindowedStream<JSONObject, TimeWindow> windowDS = cartUUDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(2)));

        // 对窗口内的数据进行聚合操作，统计每个窗口内的去重加购用户数
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(

                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {

                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {

                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {

                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {

                        return null;
                    }
                },
                // 定义窗口函数，将聚合结果转换为CartAddUuBean对象
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) {

                        Long cartUUCt = values.iterator().next();

                        long startTs = window.getStart() / 1000;
                        long endTs = window.getEnd() / 1000;
                        // 将开始时间戳和结束时间戳转换为日期时间字符串
                        String stt = DateFormatUtil.tsToDateTime(startTs);
                        String edt = DateFormatUtil.tsToDateTime(endTs);
                        // 将开始时间戳转换为日期字符串
                        String curDate = DateFormatUtil.tsToDate(startTs);

                        out.collect(new CartAddUuBean(
                                stt,
                                edt,
                                curDate,
                                cartUUCt
                        ));
                    }
                }
        );


        SingleOutputStreamOperator<String> operator = aggregateDS
                .map(new BeanToJsonStrMapFunction<>());


        operator.print();

        // 将转换后的JSON字符串写入Doris数据库，使用自定义工具类FlinkSinkUtil获取Doris数据源
        operator.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

        env.execute("DwsTradeCartAddUuWindow");
    }
}
