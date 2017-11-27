package com.cg.springstudy.tx;

import com.cg.springstudy.dao.mybatis.mapper.UserMapper;
import com.cg.springstudy.dao.mybatis.pojo.User;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.*;

/**
 * @author： Cheng Guang
 * @date： 2017/11/27.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring/applicationContext.xml" })
public class UserServiceImplTest {

    @Autowired
    private UserServiceImpl userService;

    private User jack = new User();
    private User rose = new User();

    @Before
    public void init(){
        jack.setName("jack");
        jack.setAge(26);
        jack.setSex("MALE");

        rose.setName("rose");
        rose.setAge(24);
        rose.setSex("FEMALE");
    }

    @Test
    public void saveError() throws Exception {
        userService.saveError(jack,rose);
    }

    @Test
    public void saveErrorFinal() throws Exception {
        userService.saveErrorFinal(jack,rose);
    }

    @Test
    public void saveErrorStatic() throws Exception {
    }

}