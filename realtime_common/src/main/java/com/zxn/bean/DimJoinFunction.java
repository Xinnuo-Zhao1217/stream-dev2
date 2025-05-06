package com.zxn.bean;

import com.alibaba.fastjson.JSONObject;

/**
 * @Package com.zxn.retail.v1.realtime.bean.DimJoinFunction
 * @Author xinnuo.zhao
 * @Date 2025/5/5 8:46
 * @description: DimJoinFunction
 */

public interface DimJoinFunction<T> {
    void addDims(T obj, JSONObject dimJsonObj) ;

    String getTableName() ;

    String getRowKey(T obj) ;
}
