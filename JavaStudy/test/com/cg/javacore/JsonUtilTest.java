package com.cg.javacore;

import com.cg.javacore.cloneobject.Father;
import com.cg.javacore.cloneobject.Student;
import com.cg.javacore.json.JsonUtil;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;

/**
 * @author： Cheng Guang
 * @date： 2017/2/3.
 */
public class JsonUtilTest {

    @Test
    public void test001() throws Exception {
        Father father = new Father();
        System.out.println(JsonUtil.serialize(father));
    }

    @Test
    public void test002() throws Exception {
        String jsonStr = "{\"name\":\"cheng\",\"age\":17,\"sex\":\"男\"}";
        Father father = JsonUtil.deserialize(jsonStr, Father.class);
        System.out.println(JsonUtil.serialize(father));
    }

    @Test
    public void test003() throws Exception {
//        int i = -1;
//        int sum = +i;
//        System.out.println(sum);


        String user_id = "user_id";
        String other_phone = "";
        String key = user_id + "\t" + other_phone;

        String[] keys = key.toString().split("\t");
        System.out.println(keys.length);
    }

}
