package com.zxn.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zxn.bean.TrafficHomeDetailPageViewBean;
import com.zxn.fonction.BeanToJsonStrMapFunction;
import com.zxn.util.DateFormatUtil;
import com.zxn.util.FlinkSinkUtil;
import com.zxn.util.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.zxn.dws.DwsTrafficHomeDetailPageViewWindow
 * @Author zhao.xinnuo
 * @Date 2025/5/5 15:06
 * @description: DwsTrafficHomeDetailPageViewWindow
 */
public class DwsTrafficHomeDetailPageViewWindow {
    public static void main(String[] args) throws Exception {
        // 获取Flink流执行环境，后续用于构建和执行流处理任务
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置作业的并行度为1，即所有任务在一个并行实例上执行
        env.setParallelism(1);

        // 启用检查点机制，每5000毫秒（5秒）进行一次检查点操作，采用精确一次（EXACTLY_ONCE）语义
        // 确保在故障恢复时数据处理的一致性，无数据重复或丢失
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 从自定义工具类FlinkSourceUtil获取Kafka数据源，指定Kafka主题为"dwd_traffic_page"
        // 消费者组ID为"dws_traffic_home_detail_page_view_window"
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_traffic_page_chenming", "dws_traffic_home_detail_page_view_window");

        // 从Kafka数据源创建数据流，不使用水位线策略，命名为"Kafka_Source"
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // TODO 1. 对流中数据类型进行转换，将json字符串转换为JSONObject对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // TODO 2. 过滤出首页以及详情页的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        // 从JSONObject的"page"字段中获取page_id
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        // 判断page_id是否为"home"或"good_detail"，若是则返回true，保留该数据
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                }
        );
        // 注释掉的打印语句，用于调试查看过滤后的数据
        filterDS.print();

        // TODO 3. 指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        // 从JSONObject中提取"ts"字段作为事件时间戳
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );

        // TODO 4. 按照mid进行分组，mid字段从JSONObject的"common"字段中获取
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        // TODO 5. 使用flink的状态编程，判断是否为首页以及详情页的独立访客，并将结果封装为统计的实体类对象
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                    // 定义状态变量，分别存储首页和详情页的上次访问日期
                    private ValueState<String> homeLastVisitDateState;
                    private ValueState<String> detailLastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化首页上次访问日期的状态描述符，设置状态名称和类型，并设置状态生存时间为1天
                        ValueStateDescriptor<String> homeValueStateDescriptor = new ValueStateDescriptor<>("homeLastVisitDateState", String.class);
                        homeValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        homeLastVisitDateState = getRuntimeContext().getState(homeValueStateDescriptor);

                        // 初始化详情页上次访问日期的状态描述符，设置状态名称和类型，并设置状态生存时间为1天
                        ValueStateDescriptor<String> detailValueStateDescriptor = new ValueStateDescriptor<>("detailLastVisitDateState", String.class);
                        detailValueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        detailLastVisitDateState = getRuntimeContext().getState(detailValueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        // 从JSONObject的"page"字段中获取page_id
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");

                        // 从JSONObject中获取时间戳
                        Long ts = jsonObj.getLong("ts");
                        // 将时间戳转换为日期字符串
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        // 初始化首页和详情页的独立访客计数
                        long homeUvCt = 0L;
                        long detailUvCt = 0L;

                        if ("home".equals(pageId)) {
                            // 获取首页的上次访问日期
                            String homeLastVisitDate = homeLastVisitDateState.value();
                            // 判断当前日期与上次访问日期是否不同或上次访问日期为空
                            if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                                // 若不同，则首页独立访客计数加1，并更新首页上次访问日期状态
                                homeUvCt = 1L;
                                homeLastVisitDateState.update(curVisitDate);
                            }
                        } else {
                            // 获取详情页的上次访问日期
                            String detailLastVisitDate = detailLastVisitDateState.value();
                            // 判断当前日期与上次访问日期是否不同或上次访问日期为空
                            if (StringUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDate.equals(curVisitDate)) {
                                // 若不同，则详情页独立访客计数加1，并更新详情页上次访问日期状态
                                detailUvCt = 1L;
                                detailLastVisitDateState.update(curVisitDate);
                            }
                        }

                        // 如果首页或详情页的独立访客计数不为0，则将统计结果封装为TrafficHomeDetailPageViewBean对象并输出
                        if (homeUvCt != 0L || detailUvCt != 0L) {
                            out.collect(new TrafficHomeDetailPageViewBean(
                                    "", "", "", homeUvCt, detailUvCt, ts
                            ));
                        }
                    }
                }
        );
        // 注释掉的打印语句，用于调试查看处理后的数据
        beanDS.print();

        // TODO 6. 对数据应用滚动事件时间窗口，窗口大小为10秒
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = beanDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // TODO 7. 对窗口内的数据进行聚合操作
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(
                // 定义ReduceFunction，用于聚合首页和详情页的独立访客计数
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) {
                        // 累加首页独立访客计数
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        // 累加详情页独立访客计数
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                },
                // 定义ProcessAllWindowFunction，用于处理窗口聚合后的结果
                new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> elements, Collector<TrafficHomeDetailPageViewBean> out) {
                        // 获取窗口内的第一个TrafficHomeDetailPageViewBean对象
                        TrafficHomeDetailPageViewBean viewBean = elements.iterator().next();
                        // 获取窗口的时间范围
                        TimeWindow window = context.window();
                        // 将窗口开始时间转换为日期时间字符串
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        // 将窗口结束时间转换为日期时间字符串
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        // 将窗口开始时间转换为日期字符串
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        // 设置TrafficHomeDetailPageViewBean对象的相关时间字段
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCurDate(curDate);
                        // 输出处理后的TrafficHomeDetailPageViewBean对象
                        out.collect(viewBean);
                    }
                }
        );

        // 将TrafficHomeDetailPageViewBean对象转换为JSON字符串
        SingleOutputStreamOperator<String> jsonMap = reduceDS
                .map(new BeanToJsonStrMapFunction<>());

        // 打印转换后的JSON字符串，用于调试
        jsonMap.print();

        // 将转换后的JSON字符串写入Doris数据库，使用自定义工具类FlinkSinkUtil获取Doris数据源
        jsonMap.sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));

        // 执行Flink作业，作业名称为"DwsTrafficHomeDetailPageViewWindow"
        env.execute("DwsTrafficHomeDetailPageViewWindow");
    }
}
