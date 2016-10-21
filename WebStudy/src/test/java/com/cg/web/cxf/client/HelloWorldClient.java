package com.cg.web.cxf.client;

import com.cg.web.cxf.jaxws.HelloWorld;
import com.cg.web.pojo.User;
import org.apache.cxf.jaxws.JaxWsProxyFactoryBean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Cheng Guang on 2016/9/6.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring/spring-cxf-consumer.xml" })
public class HelloWorldClient {

    @Autowired
    private HelloWorld client;

    @Test
    public void testWithoutSpring() {
        JaxWsProxyFactoryBean svr = new JaxWsProxyFactoryBean();
        svr.setServiceClass(HelloWorld.class);
        svr.setAddress("http://localhost:8080/webstudy/webservice/helloWorld");
        HelloWorld hw = (HelloWorld) svr.create();
        User user = new User();
        user.setName("Cheng");
        user.setSex("test");
        System.out.println(hw.sayHiToUser(user));
    }

    @Test
    public void testBySpring() {
        User user1 = new User();
        user1.setName("Cheng");
        user1.setSex("男");

        User user2 = new User();
        user2.setName("freeman");
        user1.setSex("女");

        List<User> userList = new ArrayList<User>();
        userList.add(user1);
        userList.add(user2);

        /**
         * 使用Cxf+Spring，可以像在本地一样调用远程方法
         */
        String[] res = client.SayHiToUserList(userList);
        System.out.println(res[0]);
        System.out.println(res[1]);
    }
}