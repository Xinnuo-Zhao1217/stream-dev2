package com.zxn.dim;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Package com.zxn.dim.DimBaseCategory
 * @Author zhao.xinnuo
 * @Date 2025/5/14 14:09
 * @description: DimBaseCategory
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimBaseCategory implements Serializable {

    private String id;
    private String b3name;
    private String b2name;
    private String b1name;


}
