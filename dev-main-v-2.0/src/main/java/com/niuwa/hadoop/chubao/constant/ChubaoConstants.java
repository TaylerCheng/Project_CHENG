package com.niuwa.hadoop.chubao.constant;

/**
 * @author： Cheng Guang
 * @date： 2017/2/27.
 */
public interface ChubaoConstants {

    /*  触宝小额基础费率
     * （A=通讯录外手机号码挂断率<0.5 B=通讯录外手机号码挂断率>=0.5或空）
     */
    public static final double FIRST_A_BASE_FEE_RATE = 0.0375;//首次A
    public static final double FIRST_B_BASE_FEE_RATE = 0.0475;//首次B
    public static final double OLD_A_HAVE_OVERDUE_BASE_FEE_RATE  = 0.0375;//非首次有逾期A
    public static final double OLD_B_HAVE_OVERDUE_BASE_FEE_RATE  = 0.0475;//非首次有逾期B
    public static final double OLD_NO_OVERDUE_BASE_FEE_RATE  = 0.0275;//非首次无逾期

}
