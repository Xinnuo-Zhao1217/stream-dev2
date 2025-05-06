package com.zxn.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Package com.zxn.retail.v1.realtime.bean.TrafficUvCt
 * @Author xinnuo.zhao
 * @Date 2025/5/5 13:56
 * @description: TrafficUvCt
 */

@Data
@AllArgsConstructor
public class TrafficUvCt {
    // 渠道
    String ch;
    // 独立访客数
    Integer uvCt;
}
