package com.zxn.dim;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Package com.zxn.dim.DimCategoryCompare
 * @Author zhao.xinnuo
 * @Date 2025/5/14 14:28
 * @description: DimCategoryCompare
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DimCategoryCompare {
    private Integer id;
    private String categoryName;
    private String searchCategory;
}
