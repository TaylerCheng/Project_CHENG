package com.cg.dubbox.consumer;

/**
 * Created by Cheng Guang on 2016/9/29.
 */

import com.cg.dubbox.api.service.DataRestService;
import com.cg.dubbox.api.service.MyService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;

public class DemoConsumer {

    public static void main(String[] args) {
        //测试常规服务
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
                "classpath*:spring-dubbo-consumer.xml");
        context.start();
        //使用dubbox框架，可以像调用本地服务一样调用远程程序
        MyService userService = context.getBean(MyService.class);
        System.out.println("结果-----------------: " + userService.getUserInXML().getUsername());

        DataRestService infoCollectDataRestService = context.getBean(DataRestService.class);
        System.out.println("结果-----------------: " + infoCollectDataRestService.getCitiesData());

        //测试Restful服务
        //        String url = "http://localhost:8888/services/json/hello";
        //        System.out.println("Successfully got result: " + getUser(url));
    }

    private static String getUser(String url) {
        System.out.println("Getting user via " + url);
        Client client = ClientBuilder.newClient();
        WebTarget target = client.target(url);
        Response response = target.request().get();
        try {
            if (response.getStatus() != 200) {
                throw new RuntimeException("Failed with HTTP error code : " + response.getStatus());
            }
            return response.readEntity(String.class);
        } finally {
            response.close();
            client.close();
        }
    }
}
