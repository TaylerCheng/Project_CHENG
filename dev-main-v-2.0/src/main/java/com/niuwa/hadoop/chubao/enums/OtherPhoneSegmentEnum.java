package com.niuwa.hadoop.chubao.enums;

/**
 *  用户联系号码定段
 *
 * @author： Cheng Guang
 * @date： 2017/2/23.
 */
public enum  OtherPhoneSegmentEnum {
    GOOD("good", "优质联系号码"), MID("mid", "一般联系号码"), BAD("bad", "其他联系号码");

    OtherPhoneSegmentEnum(String segment, String define) {
        this.segment = segment;
        this.define = define;
    }

    private String segment;
    private String define;

    public String getSegment() {
        return segment;
    }

    public void setSegment(String segment) {
        this.segment = segment;
    }

    public String getDefine() {
        return define;
    }

    public void setDefine(String define) {
        this.define = define;
    }
}
