package com.cg.web.spring.dao;

import com.cg.web.pojo.User;

/**
 * Created by Cheng Guang on 2016/9/19.
 */
public interface UserDao {

    void addUser(User user);

    void updateUser(User user);
}
