package com.cg.springstudy.dao.jdbc.impl;

import com.cg.springstudy.dao.mybatis.pojo.User;

/**
 * Created by Cheng Guang on 2016/9/19.
 */
public interface UserDao {

    void addUser(User user);

    void updateUser(User user);
}
