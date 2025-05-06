package com.zxn.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.zxn.retail.v1.realtime.bean.TradeProvinceOrderAmount
 * @Author xinnuo.zhao
 * @Date 2025/5/5 11:19
 * @description: TradeProvinceOrderAmount
 */

@Data
@AllArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String provinceName;
    // 下单金额
    Double orderAmount;
}
