package com.zxn.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zxn.bean.TradeSkuOrderBean;
import com.zxn.constant.Constant;
import com.zxn.fonction.BeanToJsonStrMapFunction;
import com.zxn.fonction.DimAsyncFunction;
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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

/**
 * @Package com.zxn.dws.DwsTradeSkuOrderWindow
 * @Author zhao.xinnuo
 * @Date 2025/5/5 15:04
 * @description: DwsTradeSkuOrderWindow
 */
public class DwsTradeSkuOrderWindow {
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

        // 从自定义工具类FlinkSourceUtil中获取Kafka数据源，指定Kafka主题为"dwd_trade_order_detail_xinnuo_zhao"
        // 以及消费者组ID为"dws_trade_sku_order_window"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_order_detail_chenming", "dws_trade_sku_order_window");

        // 从Kafka数据源读取数据，创建一个DataStreamSource对象，指定水位线策略为不生成水位线
        // 并为数据源命名为"Kafka_Source"
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // 将从Kafka读取的字符串数据转换为JSONObject对象，过滤掉空值
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        if (value != null) {
                            JSONObject jsonObj = JSON.parseObject(value);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        // 注释掉的打印语句，用于调试查看jsonObjDS中的数据
//        jsonObjDS.print();

        // 按照订单明细ID对数据进行分组，将相同订单明细ID的数据分到同一个组中
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        // 对分组后的数据进行去重处理，只保留最新的记录
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    // 定义一个状态变量，用于存储上一次处理的JSONObject对象
                    private ValueState<JSONObject> lastJsonObjState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态描述符，指定状态名称为"lastJsonObjState"，状态类型为JSONObject
                        ValueStateDescriptor<JSONObject> valueStateDescriptor = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        // 从运行时上下文获取状态对象
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 从状态中获取上一次处理的JSONObject对象
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            // 如果状态为空，将当前的JSONObject对象存入状态，并注册一个5秒后的定时器
                            lastJsonObjState.update(jsonObj);
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            // 如果状态不为空，比较当前记录和上一次记录的时间戳
                            String lastTs = lastJsonObj.getString("ts");
                            String curTs = jsonObj.getString("ts");
                            if (curTs.compareTo(lastTs) >= 0) {
                                // 如果当前记录的时间戳大于等于上一次记录的时间戳，更新状态
                                lastJsonObjState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        // 当定时器触发时，将状态中的数据发送到下游，并清除状态
                        JSONObject jsonObj = lastJsonObjState.value();
                        out.collect(jsonObj);
                        lastJsonObjState.clear();
                    }
                }
        );

        // 注释掉的打印语句，用于调试查看distinctDS中的数据
//        distinctDS.print();

        // 为去重后的数据分配时间戳和水位线，使用单调递增的水位线策略
        // 从JSONObject对象中提取"ts_ms"字段作为事件时间戳，并乘以1000转换为毫秒
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );

        // 注释掉的打印语句，用于调试查看withWatermarkDS中的数据
//        withWatermarkDS.print();

        // 将JSONObject对象转换为TradeSkuOrderBean对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) {
                        // 从JSONObject对象中提取所需的字段
                        String skuId = jsonObj.getString("sku_id");
                        BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                        BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                        BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts") * 1000;
                        // 构建TradeSkuOrderBean对象并返回
                        return TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(splitOriginalAmount)
                                .couponReduceAmount(splitCouponAmount)
                                .activityReduceAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .ts_ms(ts)
                                .build();
                    }
                }
        );

        // 注释掉的打印语句，用于调试查看beanDS中的数据
//        beanDS.print();

        // TODO 6. 按照SKU ID对TradeSkuOrderBean对象进行分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);

        // TODO 7. 对分组后的数据应用滚动处理时间窗口，窗口大小为10秒
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS.window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // TODO 8. 对窗口内的数据进行聚合操作
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                // 定义ReduceFunction，用于对窗口内的数据进行聚合
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) {
                        // 累加原始金额、活动优惠金额、优惠券优惠金额和订单金额
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                // 定义ProcessWindowFunction，用于对窗口处理后的结果进行进一步处理
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) {
                        // 获取窗口内的第一个TradeSkuOrderBean对象
                        TradeSkuOrderBean orderBean = elements.iterator().next();
                        // 获取窗口的时间范围
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        // 设置窗口的开始时间、结束时间和日期
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        // 将处理后的对象输出
                        out.collect(orderBean);
                    }
                }
        );

        // 注释掉的打印语句，用于调试查看reduceDS中的数据
//        reduceDS.print();

        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
                        orderBean.setSkuName(dimJsonObj.getString("sku_name"));
                        orderBean.setSpuId(dimJsonObj.getString("spu_id"));
                        orderBean.setCategory3Id(dimJsonObj.getString("category3_id"));
                        orderBean.setTrademarkId(dimJsonObj.getString("tm_id"));
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public String getRowKey(TradeSkuOrderBean orderBean) {
                        return orderBean.getSkuId();
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        withSkuInfoDS.print();
        //TODO 10.关联spu维度
//        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
//                withSkuInfoDS,
//                new DimAsyncFunction<TradeSkuOrderBean>() {
//                    @Override
//                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
//                        orderBean.setSpuName(dimJsonObj.getString("spu_name"));
//                    }
//
//                    @Override
//                    public String getTableName() {
//                        return "dim_spu_info";
//                    }
//
//                    @Override
//                    public String getRowKey(TradeSkuOrderBean orderBean) {
//                        return orderBean.getSpuId();
//                    }
//                },
//                60,
//                TimeUnit.SECONDS
//        );
////        withSpuInfoDS.print();
//        //TODO 11.关联tm维度
//        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = AsyncDataStream.unorderedWait(
//                withSpuInfoDS,
//                new DimAsyncFunction<TradeSkuOrderBean>() {
//                    @Override
//                    public void addDims(TradeSkuOrderBean orderBean, JSONObject dimJsonObj) {
//                        orderBean.setTrademarkName(dimJsonObj.getString("tm_name"));
//                    }
//
//                    @Override
//                    public String getTableName() {
//                        return "dim_base_trademark";
//                    }
//
//                    @Override
//                    public String getRowKey(TradeSkuOrderBean orderBean) {
//                        return orderBean.getTrademarkId();
//                    }
//                },
//                60,
//                TimeUnit.SECONDS
//        );
//        //TODO 12.关联category3维度
//        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = AsyncDataStream.unorderedWait(
//                withTmDS,
//                new DimAsyncFunction<TradeSkuOrderBean>() {
//                    @Override
//                    public String getRowKey(TradeSkuOrderBean bean) {
//                        return bean.getCategory3Id();
//                    }
//
//                    @Override
//                    public String getTableName() {
//                        return "dim_base_category3";
//                    }
//
//                    @Override
//                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
//                        bean.setCategory3Name(dim.getString("name"));
//                        bean.setCategory2Id(dim.getString("category2_id"));
//                    }
//                },
//                120,
//                TimeUnit.SECONDS
//        );
//
//        //TODO 13.关联category2维度
//        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = AsyncDataStream.unorderedWait(
//                c3Stream,
//                new DimAsyncFunction<TradeSkuOrderBean>() {
//                    @Override
//                    public String getRowKey(TradeSkuOrderBean bean) {
//                        return bean.getCategory2Id();
//                    }
//
//                    @Override
//                    public String getTableName() {
//                        return "dim_base_category2";
//                    }
//
//                    @Override
//                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
//                        bean.setCategory2Name(dim.getString("name"));
//                        bean.setCategory1Id(dim.getString("category1_id"));
//                    }
//                },
//                120,
//                TimeUnit.SECONDS
//        );
//
//        //TODO 14.关联category1维度
//        SingleOutputStreamOperator<TradeSkuOrderBean> withC1DS = AsyncDataStream.unorderedWait(
//                c2Stream,
//                new DimAsyncFunction<TradeSkuOrderBean>() {
//                    @Override
//                    public String getRowKey(TradeSkuOrderBean bean) {
//                        return bean.getCategory1Id();
//                    }
//
//                    @Override
//                    public String getTableName() {
//                        return "dim_base_category1";
//                    }
//
//                    @Override
//                    public void addDims(TradeSkuOrderBean bean, JSONObject dim) {
//                        bean.setCategory1Name(dim.getString("name"));
//                    }
//                },
//                120,
//                TimeUnit.SECONDS
//        );
//
//        withC1DS.print("======>");

//        //TODO 15.将关联的结果写到Doris表中
//        withC1DS
//                .map(new BeanToJsonStrMapFunction<>())
//                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));

        env.execute("DwsTradeSkuOrderWindow");
    }
}

