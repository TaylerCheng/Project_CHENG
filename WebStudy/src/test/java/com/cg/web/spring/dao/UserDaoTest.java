package com.cg.web.spring.dao;

import com.cg.web.pojo.User;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Created by Cheng Guang on 2016/9/19.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:spring/applicationContext.xml" })
public class UserDaoTest {

    @Autowired
    private UserDao userDao;

    @Test
    public void testAddUser() {
        User user = new User();
        user.setName("程广");
        user.setAge(25);
        user.setSex("男");
        userDao.addUser(user);
    }

    @Test
    public void testUpdateUser() {
        User user = new User();
        user.setId(2);
        user.setName("邵晨晨");
        user.setAge(23);
        user.setSex("女");
        userDao.updateUser(user);
    }
}
