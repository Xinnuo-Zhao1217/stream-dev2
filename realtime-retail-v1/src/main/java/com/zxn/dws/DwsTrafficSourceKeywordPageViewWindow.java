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
 * @description: DwsTrafficSourceKeywordPageViewWindow
 */
public class DwsTrafficSourceKeywordPageViewWindow {
    public static void main(String[] args) throws Exception {
        // 获取Flink流执行环境，用于后续构建和执行流处理任务
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置作业的并行度为1，即所有任务在一个并行实例上执行
        env.setParallelism(1);

        // 创建流表执行环境，用于在流处理中使用SQL进行数据操作
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 启用检查点机制，每5000毫秒（5秒）进行一次检查点操作
        // 未指定检查点模式，默认为AT_LEAST_ONCE
        env.enableCheckpointing(5000L);

        // 在流表环境中创建一个临时系统函数，函数名为"ik_analyze"，对应的实现类为KeywordUDTF
        tableEnv.createTemporarySystemFunction("ik_analyze", KeywordUDTF.class);

        // 在流表环境中执行SQL语句，创建一个名为"page_log"的表
        // 该表包含common（键值对映射）、page（键值对映射）字段，以及从消息元数据中获取的时间戳字段ts
        // 并定义了水位线，ts减去3秒作为水位线
        // 表的连接器使用自定义工具类SQLUtil获取的Kafka连接器定义，指定了Kafka主题和相关配置
        tableEnv.executeSql("create table page_log(\n" +
                "     common map<string,string>,\n" +
                "     page map<string,string>,\n" +
                "     ts TIMESTAMP(3) METADATA FROM 'timestamp', \n" +
                "     WATERMARK FOR ts AS ts - INTERVAL '3' SECOND \n" +
                ")" + SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE,"dws_traffic_source_keyword_page_view_window"));
        // 注释掉的打印语句，用于调试查看page_log表的数据
        tableEnv.executeSql("select * from page_log").print();

        // 在page_log表上执行SQL查询，筛选出上一页ID为'search'、项目类型为'keyword'且项目不为空的数据
        // 提取出page['item']作为完整关键词fullword和时间戳ts字段，结果存储在searchTable表中
        Table searchTable = tableEnv.sqlQuery("select \n" +
                "   page['item']  fullword,\n" +
                "   ts \n" +
                " from page_log\n" +
                " where page['last_page_id'] = 'search' and page['item_type'] ='keyword' and page['item'] is not null");
        // 将searchTable表注册为临时视图，方便后续SQL查询引用
        tableEnv.createTemporaryView("search_table",searchTable);
        // 注释掉的打印语句，用于调试查看searchTable表的数据
        searchTable.execute().print();
//
        // 在search_table临时视图上执行SQL查询，使用LATERAL TABLE语法调用之前定义的ik_analyze函数
        // 将fullword字段进行分词处理，生成关键词keyword，并与时间戳ts一起存储在splitTable表中
        Table splitTable = tableEnv.sqlQuery("SELECT keyword,ts FROM search_table,\n" +
                "LATERAL TABLE(ik_analyze(fullword)) t(keyword)");
        // 将splitTable表注册为临时视图，方便后续SQL查询引用
        tableEnv.createTemporaryView("split_table",splitTable);
        // 注释掉的打印语句，用于调试查看splitTable表的数据
        tableEnv.executeSql("select * from split_table").print();

        // 在split_table临时视图上执行SQL查询，使用TUMBLE窗口函数对数据进行窗口划分
//        窗口大小为5秒，按窗口开始时间、窗口结束时间和关键词keyword进行分组
//        计算每个窗口内每个关键词的出现次数keyword_count，并格式化窗口开始时间、结束时间和日期
        Table resTable = tableEnv.sqlQuery("SELECT \n" +
                "  date_format(window_start, 'yyyy-MM-dd HH:mm:ss') stt,\n" +
                "  date_format(window_end, 'yyyy-MM-dd HH:mm:ss') edt,\n" +
                "  date_format(window_start, 'yyyy-MM-dd') cur_date,\n" +
                "  keyword,\n" +
                "  count(*) keyword_count\n" +
                "  FROM TABLE(\n" +
                "  TUMBLE(TABLE split_table, DESCRIPTOR(ts), INTERVAL '5' second))\n" +
                "  GROUP BY window_start, window_end, keyword");
        // 注释掉的打印语句，用于调试查看resTable表的数据
        resTable.execute().print();

        // 在流表环境中执行SQL语句，创建一个名为"dws_traffic_source_keyword_page_view_window"的表
        // 该表用于将处理后的数据写入Doris数据库，包含开始时间stt、结束时间edt、日期cur_date、关键词keyword和关键词计数keyword_count字段
        // 表的连接器配置使用Doris，指定了Doris的FE节点地址、数据库名、表名、用户名、密码等信息
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
        // 将resTable表中的数据插入到"dws_traffic_source_keyword_page_view_window"表中，即将处理后的数据写入Doris数据库
        resTable.executeInsert("dws_traffic_source_keyword_page_view_window");

        // 执行Flink作业，作业名称为"DwsTrafficSourceKeywordPageViewWindow"
        env.execute("DwsTrafficSourceKeywordPageViewWindow");
    }
}
