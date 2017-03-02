package com.niuwa.hadoop.chubao.rules;

import com.alibaba.fastjson.JSONObject;

public class BaseRules{
    public static boolean joinRule(JSONObject resultObj, String... cols) {
        for (String col : cols) {
            if (resultObj.get(col) == null) {
                return false;
            }
        }
        return true;
    }
}
