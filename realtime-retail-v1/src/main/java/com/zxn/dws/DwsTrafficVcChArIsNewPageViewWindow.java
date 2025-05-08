package com.zxn.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zxn.bean.TrafficVcChArIsNewPageViewBean;
import com.zxn.fonction.BeanToJsonStrMapFunction;
import com.zxn.util.DateFormatUtil;
import com.zxn.util.FlinkSinkUtil;
import com.zxn.util.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.zxn.dws.DwsTrafficVcChArIsNewPageViewWindow
 * @Author zhao.xinnuo
 * @Date 2025/5/5 15:08
 * @description: 分组，统计页面浏览量（PV）、独立访客数（UV）、会话数（SV）以及页面停留总时长，
 * 并将统计结果按 10 秒的滚动窗口进行聚合，最后将聚合结果以 JSON 字符串的形式写入 Doris 数据库
 */
public class DwsTrafficVcChArIsNewPageViewWindow {
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

        // 从自定义工具类FlinkSourceUtil中获取Kafka数据源，指定Kafka主题为"dwd_traffic_page_xinnuo_zhao"
        // 以及消费者组ID为"dws_traffic_vc_ch_ar_is_new_page_view_window"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_traffic_page_chenming", "dws_traffic_vc_ch_ar_is_new_page_view_window");

        // 从Kafka数据源读取数据，创建一个DataStreamSource对象，指定水位线策略为不生成水位线
        // 并为数据源命名为"Kafka_Source"
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // 注释掉的打印语句，用于调试查看从Kafka读取的数据
//        kafkaStrDS.print();

        // 将从Kafka读取的字符串数据转换为JSONObject对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 按照设备唯一标识（mid）对数据进行分组，将相同mid的数据分到同一个组中
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // 将分组后的数据转换为TrafficVcChArIsNewPageViewBean对象，并进行UV和SV的统计
        SingleOutputStreamOperator<TrafficVcChArIsNewPageViewBean> beanDS = midKeyedDS.map(
                new RichMapFunction<JSONObject, TrafficVcChArIsNewPageViewBean>() {
                    // 定义一个状态变量，用于存储设备的上次访问日期
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态描述符，指定状态名称为"lastVisitDateState"，状态类型为String
                        // 并设置状态的生存时间为1天，过期后自动清除
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        // 从运行时上下文获取状态对象
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public TrafficVcChArIsNewPageViewBean map(JSONObject jsonObj) throws Exception {
                        // 从JSONObject中提取公共信息和页面信息
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                        // 从状态中获取设备的上次访问日期
                        String lastVisitDate = lastVisitDateState.value();
                        // 获取当前访问的时间戳，并转换为日期字符串
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        // 初始化UV计数为0
                        long uvCt = 0L;
                        // 如果上次访问日期为空或者与当前日期不同，则认为是新的UV访问，UV计数加1
                        // 并更新状态中的上次访问日期为当前日期
                        if (StringUtils.isEmpty(lastVisitDate) || !lastVisitDate.equals(curVisitDate)) {
                            uvCt = 1L;
                            lastVisitDateState.update(curVisitDate);
                        }

                        // 获取上一页的ID
                        String lastPageId = pageJsonObj.getString("last_page_id");
                        // 如果上一页ID为空，则认为是会话开始，SV计数加1
                        Long svCt = StringUtils.isEmpty(lastPageId) ? 1L : 0L;

                        // 构建TrafficVcChArIsNewPageViewBean对象并返回
                        return new TrafficVcChArIsNewPageViewBean(
                                "",
                                "",
                                "",
                                commonJsonObj.getString("vc"),
                                commonJsonObj.getString("ch"),
                                commonJsonObj.getString("ar"),
                                commonJsonObj.getString("is_new"),
                                uvCt,
                                svCt,
                                1L,
                                pageJsonObj.getLong("during_time"),
                                ts
                        );
                    }
                }
        );

        // 注释掉的打印语句，用于调试查看转换后的TrafficVcChArIsNewPageViewBean对象数据
        beanDS.print();

        // 为转换后的数据分配时间戳和水位线，使用单调递增的水位线策略
        // 从TrafficVcChArIsNewPageViewBean对象中提取时间戳字段作为事件时间戳
        SingleOutputStreamOperator<TrafficVcChArIsNewPageViewBean> withWatermarkDS = beanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<TrafficVcChArIsNewPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<TrafficVcChArIsNewPageViewBean>() {
                                    @Override
                                    public long extractTimestamp(TrafficVcChArIsNewPageViewBean bean, long recordTimestamp) {
                                        return bean.getTs();
                                    }
                                }
                        )
        );

        // 注释掉的打印语句，用于调试查看分配时间戳和水位线后的数据
        withWatermarkDS.print();

        // 按照版本（vc）、渠道（ch）、地区（ar）和是否为新用户（is_new）对数据进行分组
        KeyedStream<TrafficVcChArIsNewPageViewBean, Tuple4<String, String, String, String>> dimKeyedDS = withWatermarkDS.keyBy(
                new KeySelector<TrafficVcChArIsNewPageViewBean, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(TrafficVcChArIsNewPageViewBean bean) {
                        return Tuple4.of(bean.getVc(),
                                bean.getCh(),
                                bean.getAr(),
                                bean.getIsNew());
                    }
                }
        );

        // 对分组后的数据应用滚动事件时间窗口，窗口大小为10秒
        WindowedStream<TrafficVcChArIsNewPageViewBean, Tuple4<String, String, String, String>, TimeWindow> windowDS
                = dimKeyedDS.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 对窗口内的数据进行聚合操作，包括PV、UV、SV和页面停留总时长的累加
        SingleOutputStreamOperator<TrafficVcChArIsNewPageViewBean> reduceDS = windowDS.reduce(
                // 定义ReduceFunction，用于对窗口内的数据进行聚合
                new ReduceFunction<TrafficVcChArIsNewPageViewBean>() {
                    @Override
                    public TrafficVcChArIsNewPageViewBean reduce(TrafficVcChArIsNewPageViewBean value1, TrafficVcChArIsNewPageViewBean value2) {
                        // 累加PV计数
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        // 累加UV计数
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        // 累加SV计数
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
                        // 累加页面停留总时长
                        value1.setDurSum(value1.getDurSum() + value2.getDurSum());
                        return value1;
                    }
                },
                // 定义WindowFunction，用于对窗口处理后的结果进行进一步处理
                new WindowFunction<TrafficVcChArIsNewPageViewBean, TrafficVcChArIsNewPageViewBean, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<TrafficVcChArIsNewPageViewBean> input, Collector<TrafficVcChArIsNewPageViewBean> out) {
                        // 获取窗口内的第一个TrafficVcChArIsNewPageViewBean对象
                        TrafficVcChArIsNewPageViewBean pageViewBean = input.iterator().next();
                        // 获取窗口的开始时间和结束时间，并转换为日期时间字符串和日期字符串
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        // 设置TrafficVcChArIsNewPageViewBean对象的窗口开始时间、结束时间和日期
                        pageViewBean.setStt(stt);
                        pageViewBean.setEdt(edt);
                        pageViewBean.setCur_date(curDate);
                        // 将处理后的对象输出
                        out.collect(pageViewBean);
                    }
                }
        );

        // 注释掉的打印语句，用于调试查看聚合后的数据
        reduceDS.print();

        // 将聚合后的TrafficVcChArIsNewPageViewBean对象转换为JSON字符串
        SingleOutputStreamOperator<String> jsonMap = reduceDS.map(new BeanToJsonStrMapFunction<>());

        // 打印转换后的JSON字符串，用于调试
        jsonMap.print();

        // 将转换后的JSON字符串写入Doris数据库，使用自定义工具类FlinkSinkUtil获取Doris数据源
        jsonMap.sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));

        // 执行Flink作业，作业名称为"DwsTrafficVcChArIsNewPageViewWindow"
        env.execute("DwsTrafficVcChArIsNewPageViewWindow");
    }
}
