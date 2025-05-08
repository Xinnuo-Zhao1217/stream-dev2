package com.zxn.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zxn.bean.TradeProvinceOrderBean;
import com.zxn.constant.Constant;
import com.zxn.fonction.BeanToJsonStrMapFunction;
import com.zxn.util.DateFormatUtil;
import com.zxn.util.FlinkSinkUtil;
import com.zxn.util.FlinkSourceUtil;
import com.zxn.util.HBaseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;

/**
 * @Package com.zxn.dws.DwsTradeProvinceOrderWindow - 地图
 * @Author zhao.xinnuo
 * @Date 2025/5/5 14:57
 * @description: 从 Kafka 读取订单数据，按省份进行去重、聚合、窗口计算，并从 HBase 获取省份名称，最终将处理结果写入 Doris 数据库的功能
 * 过滤完成之后的订单数据关联hbase存储的省份 流表join
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        // 获取Flink流执行环境，用于后续构建和执行流处理任务
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置作业并行度为1，即所有任务在一个并行实例上执行
        env.setParallelism(1);

        // 启用检查点机制，每5000毫秒（5秒）进行一次检查点操作，采用精确一次语义
        // 确保在故障恢复时数据处理的一致性，无数据重复或丢失
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置重启策略为固定延迟重启，最多尝试重启3次，每次重启间隔3000毫秒（3秒）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 从自定义工具类FlinkSourceUtil获取Kafka数据源，指定Kafka主题为"dwd_trade_order_detail_xinnuo_zhao"
        // 消费者组ID为"dws_trade_province_order_window"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_order_detail", "dws_trade_province_order_window");

        // 从Kafka数据源创建数据流，不使用水位线策略，命名为"Kafka_Source"
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // 对从Kafka读取的字符串数据进行处理，转换为JSONObject对象并输出
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        // 根据订单明细ID对JSONObject数据进行分组，形成KeyedStream
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        // 对分组后的数据进行处理，去除重复数据并对前一条数据的"split_total_amount"字段取反
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 定义状态变量，存储上一个JSONObject对象
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态描述符，指定状态名称和类型
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        // 设置状态的生存时间为10秒，超时后状态数据会被清除
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        // 从运行时上下文获取状态对象
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );

        // 为处理后的数据分配时间戳和水位线，采用单调递增水位线策略
        // 从JSONObject中提取"ts_ms"字段作为时间戳（乘以1000转换为毫秒）
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
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

        // 将JSONObject数据映射为TradeProvinceOrderBean对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) {
                        String provinceId = jsonObj.getString("province_id");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts_ms");
                        String orderId = jsonObj.getString("order_id");
                        return TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderAmount(splitTotalAmount)
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                //.ts_ms(ts)
                                .build();
                    }
                }
        );

//        beanDS.print(); // 注释掉的打印语句，可用于调试查看数据

        // 根据省份ID对TradeProvinceOrderBean数据进行分组，形成KeyedStream
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        // 对分组后的数据应用滚动事件时间窗口，窗口大小为10秒
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 对窗口内的数据进行聚合和处理，先进行Reduce操作，再进行WindowFunction操作
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) {
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) {
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        long startTs = window.getStart() / 1000;
                        long endTs = window.getEnd() / 1000;
                        String stt = DateFormatUtil.tsToDateTime(startTs);
                        String edt = DateFormatUtil.tsToDateTime(endTs);
                        String curDate = DateFormatUtil.tsToDate(startTs);
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
        );

//        reduceDS.print(); // 注释掉的打印语句，可用于调试查看数据

        // 对聚合处理后的数据进行映射，从HBase获取省份名称并添加到TradeProvinceOrderBean中
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = reduceDS.map(
                new RichMapFunction<TradeProvinceOrderBean, TradeProvinceOrderBean>() {
                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TradeProvinceOrderBean map(TradeProvinceOrderBean orderBean) throws Exception {
                        String spuId = orderBean.getProvinceId();
                        JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_province", spuId, JSONObject.class);
                        orderBean.setProvinceName(skuInfoJsonObj.getString("name"));
                        return orderBean;
                    }
                }
        ).setParallelism(1);

        // 将TradeProvinceOrderBean对象转换为JSON字符串
        SingleOutputStreamOperator<String> sink = withProvinceDS.map(new BeanToJsonStrMapFunction<>());
        sink.print(); // 打印转换后的JSON字符串，用于调试

        // 将转换后的JSON字符串写入Doris数据库，使用自定义工具类FlinkSinkUtil获取Doris数据源
        sink.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));

        // 执行Flink作业，作业名称为"DwsTradeProvinceOrderWindow"
        env.execute("DwsTradeProvinceOrderWindow");
    }
}
