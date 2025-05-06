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
 * @description: DwsTradeCartAddUuWindow
 */
public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        // 创建Flink流执行环境，用于配置和执行流处理作业
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置作业的并行度为1，即所有任务都在一个并行实例中执行
        env.setParallelism(1);

        // 启用检查点机制，每5000毫秒（5秒）进行一次检查点操作，采用精确一次（EXACTLY_ONCE）的处理语义
        // 确保在故障恢复时数据处理的一致性，不会出现数据重复或丢失的情况
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置重启策略为固定延迟重启，最多重启3次，每次重启间隔3000毫秒（3秒）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 从自定义工具类FlinkSourceUtil中获取Kafka数据源，指定Kafka主题为"dwd_trade_cart_add_xinnuo_zhao"
        // 以及消费者组ID为"dws_trade_cart_add_uu_window"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_xinyi_jiao_add", "dws_trade_cart_add_uu_window");

        // 从Kafka数据源读取数据，创建一个DataStreamSource对象，指定水位线策略为不生成水位线
        // 并为数据源命名为"Kafka_Source"
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // 将从Kafka读取的字符串数据解析为JSONObject对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 为解析后的JSONObject对象分配时间戳和水位线，使用单调递增的水位线策略
        // 从JSONObject对象中提取"ts_ms"字段作为事件时间戳，并乘以1000转换为毫秒
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

        // 按照用户ID对数据进行分组，将相同用户ID的数据分到同一个组中
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));

        // 对分组后的数据进行处理，过滤掉同一用户在同一天内的重复加购记录
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 定义一个状态变量，用于存储用户上次加购的日期
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态描述符，指定状态名称为"lastCartDateState"，状态类型为String
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastCartDateState", String.class);
                        // 为状态设置过期时间，过期时间为1天，即状态数据在1天后会自动过期
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        // 从运行时上下文获取状态对象
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 从状态中获取用户上次加购的日期
                        String lastCartDate = lastCartDateState.value();
                        // 获取当前加购记录的时间戳
                        Long ts = jsonObj.getLong("ts_ms");
                        // 将时间戳转换为日期字符串
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        // 如果上次加购日期为空，或者当前加购日期与上次加购日期不同，则认为是新的加购记录
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            // 将新的加购记录输出
                            out.collect(jsonObj);
                            // 更新状态，将当前加购日期存储到状态中
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
                // 定义聚合函数，用于计算窗口内的去重加购用户数
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        // 初始化累加器，初始值为0
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        // 每收到一条加购记录，累加器加1
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        // 返回累加器的最终结果
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        // 由于是全量窗口，不需要合并操作，返回null
                        return null;
                    }
                },
                // 定义窗口函数，将聚合结果转换为CartAddUuBean对象
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) {
                        // 获取窗口内的去重加购用户数
                        Long cartUUCt = values.iterator().next();
                        // 获取窗口的开始时间戳和结束时间戳，并转换为秒
                        long startTs = window.getStart() / 1000;
                        long endTs = window.getEnd() / 1000;
                        // 将开始时间戳和结束时间戳转换为日期时间字符串
                        String stt = DateFormatUtil.tsToDateTime(startTs);
                        String edt = DateFormatUtil.tsToDateTime(endTs);
                        // 将开始时间戳转换为日期字符串
                        String curDate = DateFormatUtil.tsToDate(startTs);
                        // 创建CartAddUuBean对象，并将其输出
                        out.collect(new CartAddUuBean(
                                stt,
                                edt,
                                curDate,
                                cartUUCt
                        ));
                    }
                }
        );

        // 将CartAddUuBean对象转换为JSON字符串
        SingleOutputStreamOperator<String> operator = aggregateDS
                .map(new BeanToJsonStrMapFunction<>());

        // 打印转换后的JSON字符串，用于调试
        operator.print();

        // 将转换后的JSON字符串写入Doris数据库，使用自定义工具类FlinkSinkUtil获取Doris数据源
        operator.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

        // 执行Flink作业，作业名称为"DwsTradeCartAddUuWindow"
        env.execute("DwsTradeCartAddUuWindow");
    }
}
