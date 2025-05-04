package com.zxn.constant;

/**
 * @Package com.zxn.constant.Constant
 * @Author zhao.xinnuo
 * @Date 2025/5/04 19:07
 * @description:
 */
public class Constant {
    public static final String KAFKA_BROKERS = "cdh01:9092,cdh02:9092,cdh03:9092";

    public static final String TOPIC_DB = "zxn_yw";
    public static final String TOPIC_LOG = "zxn_log";

    public static final String MYSQL_HOST = "10.39.48.36";
    public static final int MYSQL_PORT = 3306;
    public static final String MYSQL_USER_NAME = "root";
    public static final String MYSQL_PASSWORD = "Zh1028,./";
    public static final String HBASE_NAMESPACE = "ns_zxn";

    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String MYSQL_URL = "jdbc:mysql://10.39.48.36:3306?useSSL=false";

    public static final String TOPIC_DWD_TRAFFIC_START = "dwd_zxn_start";
    public static final String TOPIC_DWD_TRAFFIC_ERR = "dwd_zxn_err";
    public static final String TOPIC_DWD_TRAFFIC_PAGE = "dwd_zxn_page";
    public static final String TOPIC_DWD_TRAFFIC_ACTION = "dwd_zxn_action";
    public static final String TOPIC_DWD_TRAFFIC_DISPLAY = "dwd_zxn_display";

    public static final String TOPIC_DWD_INTERACTION_COMMENT_INFO = "dwd_interaction_zxn_info";
    public static final String TOPIC_DWD_TRADE_CART_ADD = "dwd_trade_zxno_add";

    public static final String TOPIC_DWD_TRADE_ORDER_DETAIL = "dwd_trade_order_detail";

    public static final String TOPIC_DWD_TRADE_ORDER_CANCEL = "dwd_trade_zxno_cancel";

    public static final String TOPIC_DWD_TRADE_ORDER_PAYMENT_SUCCESS = "dwd_trade_zxn_payment_success";
    public static final String TOPIC_DWD_TRADE_ORDER_REFUND = "dwd_trade_zxn_refund";

    public static final String TOPIC_DWD_TRADE_REFUND_PAYMENT_SUCCESS = "dwd_zxn_refund_payment_success";

    public static final String TOPIC_DWD_USER_REGISTER = "dwd_user_register";

    public static final String DORIS_FE_NODES="10.39.48.33:8030";
    public static final String DORIS_DATABASE="dev_zxn";
}

