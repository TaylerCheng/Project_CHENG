package com.cg.dubbox;

/**
 * Created by Cheng Guang on 2016/9/23.
 */

import com.cg.dubbox.service.UserService;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

public class DemoConsumer {

    @Autowired
    public UserService userService;

    @Test
    public void dubboTest() throws InterruptedException {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
                "classpath*:spring/spring-dubbo-consumer.xml");
        context.start();
        System.out.println(userService.getUser(1L));
    }

    public static void main(String[] args) {

        final String port = "8888";

        //测试Rest服务
        //        getUser("http://localhost:" + port + "/services/users/1.json");
        //        getUser("http://localhost:" + port + "/services/users/1.xml");

        //测试常规服务
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
                "classpath*:spring/spring-dubbo-consumer.xml");
        context.start();

        UserService userService = (UserService) context.getBean("userService");
        System.out.println(userService.getUser(1L));
    }

    private static void getUser(String url) {
        System.out.println("Getting user via " + url);
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(url);
        Response response = target.request().get();
        try {
            if (response.getStatus() != 200) {
                throw new RuntimeException("Failed with HTTP error code : " + response.getStatus());
            }
            System.out.println("Successfully got result: " + response.readEntity(String.class));
        } finally {
            response.close();
            client.close();
        }
    }
}
