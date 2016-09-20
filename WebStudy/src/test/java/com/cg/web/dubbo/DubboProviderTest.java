package com.cg.web.dubbo;

/**
 * Created by Cheng Guang on 2016/9/20.
 */

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring/spring-dubbo-provider.xml" })
public class DubboProviderTest {

    @Test
    public void start() {
        System.out.println("Press any key to exit.");
        try {
            System.in.read();
            System.exit(0);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
