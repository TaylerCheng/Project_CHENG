package com.niuwa.hadoop.chubao.rules;

import com.alibaba.fastjson.JSONObject;
import com.niuwa.hadoop.chubao.utils.ChubaoDateUtil;
import com.niuwa.hadoop.chubao.utils.ChubaoUtil;
/**
 * 
 * 大小额规则<br> 
 * 〈功能详细描述〉
 *
 * @author maliqiang
 * @see [相关类/方法]（可选）
 * @since [产品/模块版本] （可选）
 */
public class Rules extends BaseRules{
	
	private final static int TOTAL_CALLS_NUM_FROM_TEL_LIBRARY = 5;
	private final static int TOTAL_DIFF_NUM_FROM_TEL_LIBRARY = 3;
	
    // rule 1 客户激活时间距当前不少于3个月
    public static boolean isMatchedRule_1(Long activation) {
        return ChubaoDateUtil.compareDateAfterMonth(-3, activation * 1000);
    }    
    
    // rule 2.1 通话时间属于最近三个月
    // 服务于 "rule2 客户最近三个月通话记录数不少于100条"
    public static boolean isMatchedRule_2_1(Long callTime) {
    	
        return (!ChubaoDateUtil.compareDateAfterMonth(-3, callTime * 1000) &&
        		ChubaoDateUtil.compareDateAfterMonth(0, callTime * 1000)
        		);
    }
    
    // rule 2 客户最近三个月通话记录数不少于100条
    public static boolean isMatchedRule_2(int sum) {
        return sum >= 100;
    }

    // baseRule 日期少于6个月并且呼出类型为1并且电话类型必须合适
    public static boolean callLogBaseRule(JSONObject form) {
        if (form.getLong("call_date") == null) {
            return false;
        }
        
        return (!ChubaoDateUtil.compareDateAfterMonth(-6, form.getLong("call_date") * 1000))
        		&& ChubaoDateUtil.compareDateAfterMonth(0, form.getLong("call_date") * 1000)
                && form.getIntValue("call_type") == 1 && ChubaoUtil.telVilidate(form.getString("other_phone"));
    }

    /**
     * type1,typ2,typ3
     * 
     * @param resultObj
     * @return
     */
    public static boolean rule3(JSONObject resultObj) {
        int contact_sum = resultObj.getInteger("contact_sum");
        double call_true_rate_type = resultObj.getDouble("call_true_rate_type");
        resultObj.put("rule_type", 0);
        if (100 <= contact_sum && 0.3 <= call_true_rate_type && call_true_rate_type < 0.5) {
            resultObj.put("rule_type", 3);
            return true;
        } else if (50 <= contact_sum && 0.4 <= call_true_rate_type && call_true_rate_type < 0.5) {
            resultObj.put("rule_type", 3);
            return true;
        } else if (20 <= contact_sum && contact_sum < 100 && 0.5 <= call_true_rate_type) {
            resultObj.put("rule_type", 2);
            return true;
        } else if (100 <= contact_sum && 0.5 <= call_true_rate_type) {
            resultObj.put("rule_type", 1);
            return true;
        }
        // TODO 正式环境改为false
        return false;
    }
    
    public static boolean isMatchedRule4(int totalCallsFromTelLibrary, int totalDiffNumFromTelLibrary){
    	return !(calledNotLessThanNTimesFromTelLibrary(totalCallsFromTelLibrary, TOTAL_CALLS_NUM_FROM_TEL_LIBRARY) || 
    			calledNotLessThanNDiffNumFromTelLibrary(totalDiffNumFromTelLibrary, 
    					TOTAL_DIFF_NUM_FROM_TEL_LIBRARY));
    }

    public static boolean rule4(JSONObject resultObj) {
        if (resultObj.getBoolean("max_contact_call")
                || (resultObj.getIntValue("rule_type") == 3 && 0.5 < resultObj.getDouble("call_top5_perct_type"))) {
            return true;
        }
        // TODO 正式环境改为false
        return false;
    }
    
    //rule5:借款(小额)最大逾期天数高于15天或者进入过m2
    public static boolean rule5(JSONObject resultObj){
        if(resultObj.getInteger("loan_max_out_day")>15 ||
        		resultObj.getInteger("m2_records")>0){
            return false;
        }
        
        return true;
    }

    //rule6: 排除客户装载APP库中APP数量>=2
    public static boolean rule6(int appNum){
    	return !(appNum >= 2);
    }

    //rule7: 排除通讯录外手机号码挂断率>0.6
    public static boolean rule7(double breakRatio){
    	return !(breakRatio > 0.6);
    }
    
    private static boolean calledNotLessThanNTimesFromTelLibrary(int totalCallsFromTelLibrary, int n){
    	return totalCallsFromTelLibrary >= n;
    }
    
    private static boolean calledNotLessThanNDiffNumFromTelLibrary(int totalDiffNumFromTelLibrary, int n){
    	return totalDiffNumFromTelLibrary >= n;
    }

    //rule8: 排除客户的good电话拨打占比<0.15 & bad电话拨打次数占比<0.55 或 good电话拨打次数占比<0.3 & bad电话拨打次数占比>=0.55
    public static boolean rule8(double good_cnt_rate, double bad_cnt_rate) {
        boolean excluded =
                (good_cnt_rate < 0.15 && bad_cnt_rate < 0.55) || (good_cnt_rate < 0.3 && bad_cnt_rate >= 0.55);
        return !excluded;
    }

    //rule9:排除触宝渠道判定三个月内逾期次数>3
    public static boolean rule9(int user_loan_overdue) {
        return !(user_loan_overdue > 3);
    }
}
