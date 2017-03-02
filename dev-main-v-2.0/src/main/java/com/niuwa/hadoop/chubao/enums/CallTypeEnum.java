package com.niuwa.hadoop.chubao.enums;

/**
 * 通话类型枚举类
 *
 * @author： Cheng Guang
 * @date： 2017/2/23.
 */
public enum CallTypeEnum {
    CALLED(0, "被叫（呼入）"), CALLING(1, "主叫（呼出）");

    int id;
    String type;

    CallTypeEnum(int id, String type) {
        this.id = id;
        this.type = type;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
