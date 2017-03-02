package com.niuwa.hadoop.chubao.rules;


public class LargeRules extends BaseRules {
    // 大额规则1 客户小额借款无任何逾期记录
    public static boolean largeRule1(int out_days_records) {
        if(out_days_records>0){
            return false;
        }
        return true;
    }

    // 大额规则2 客户至少有3笔小额借款记录
    public static boolean largeRule2(int sum) {
        if (sum >= 3) {
            return true;
        }
        return false;
    }

    /**
     * 大额规则3（参数计算）: <br>
     * 判断是否为优质借款记录(借款期限>20且还款期限占比>50%)
     *
     * @param lineObj
     * @return
     */
    public static boolean largeRule3_param(int loan_days,double repay_rate) {
        if (loan_days > 20 && repay_rate > 0.5) {
            return true;
        }
        return false;
    }
    
    /**
     * 大额规则3: <br>
     * 客户至少有1笔优质借款记录
     *
     * @param lineObj
     * @return
     */
    public static boolean largeRule3(int superior_loans) {
        if (superior_loans>=1) {
            return true;
        }
        return false;
    }
}
