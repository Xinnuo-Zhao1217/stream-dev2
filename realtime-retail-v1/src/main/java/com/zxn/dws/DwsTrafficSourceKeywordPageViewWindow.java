package com.zxn.dws;

import com.zxn.constant.Constant;
import com.zxn.fonction.KeywordUDTF;
import com.zxn.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Package com.zxn.dws.DwsTrafficSourceKeywordPageViewWindow
 * @Author zhao.xinnuo
 * @Date 2025/5/5 15:07
 * @description: 对来自 Kafka 的流量页面日志数据进行处理，提取搜索关键词并分词，
 * 按 5 秒时间窗口统计各关键词的出现次数，最后把处理结果存入 Doris 数据库
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.enableCheckpointing(5000L);

        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        // 在流表环境中执行SQL语句，创建一个名为"page_log"的表
        // 该表包含common（键值对映射）、page（键值对映射）字段，以及从消息元数据中获取的时间戳字段ts

        tableEnv.executeSql("create table page_log(\n" +
                "     common map<string,string>,\n" +
                "     page map<string,string>,\n" +
                "     ts TIMESTAMP(3) METADATA FROM 'timestamp', \n" +
                "     WATERMARK FOR ts AS ts - INTERVAL '3' SECOND \n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));

        tableEnv.executeSql("select * from page_log").print();


        Table searchTable = tableEnv.sqlQuery("select \n" +
                "   page['item']  fullword,\n" +
                "   ts \n" +
                " from page_log\n" +
                " where page['last_page_id'] = 'search' and page['item_type'] ='keyword' and page['item'] is not null");

        tableEnv.createTemporaryView("search_table",searchTable);

        searchTable.execute().print();


        Table splitTable = tableEnv.sqlQuery("SELECT keyword,ts FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");

        tableEnv.createTemporaryView("split_table",splitTable);

        tableEnv.executeSql("select * from split_table").print();

        // 在split_table临时视图上执行SQL查询，使用TUMBLE窗口函数对数据进行窗口划分

        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "  date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "  date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "  date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "  keyword,\n" +
                "  count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "  TUMBLE(TABLE split_table, DESCRIPTOR(ts), INTERVAL '5' second))\n" +
                "  GROUP BY window_start, window_end, keyword");

        resTable.execute().print();

        // 在流表环境中执行SQL语句，创建一个名为"dws_traffic_source_keyword_page_view_window"的表

        tableEnv.executeSql("create table dws_traffic_source_keyword_page_view_window(" +
                "  stt string, " +  // 2023-07-11 14:14:14
                "  edt string, " +
                "  cur_date string, " +
                "  keyword string, " +
                "  keyword_count bigint " +
                ")with(" +
                " 'connector' = 'doris'," +
                " 'fenodes' = '" + Constant.DORIS_FE_NODES + "'," +
                "  'table.identifier' = '" + Constant.DORIS_DATABASE + ".dws_traffic_source_keyword_page_view_window'," +
                "  'username' = 'admin'," +
                "  'password' = 'zh1028,./', " +
                "  'sink.properties.format' = 'json', " +
                "  'sink.buffer-count' = '4', " +
                "  'sink.buffer-size' = '4086'," +
                "  'sink.enable-2pc' = 'false', " + // 测试阶段可以关闭两阶段提交,方便测试
                "  'sink.properties.read_json_by_line' = 'true' " +
                ")");
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");

        env.execute("DwsTrafficSourceKeywordPageViewWindow");
    }
}
