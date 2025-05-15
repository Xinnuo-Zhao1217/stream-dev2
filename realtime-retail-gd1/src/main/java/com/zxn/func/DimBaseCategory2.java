package com.zxn.func;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.zxn.func.DimBaseCategory2
 * @Author zhao.xinnuo
 * @Date 2025/5/15 9:14
 * @description: DimBaseCategory2
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory2 implements Serializable {

    private String id;
    private String bcname;
    private String btname;
}
