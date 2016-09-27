package com.cg.dubbox.service.impl;

import com.cg.dubbox.service.UserService;
import com.cg.dubbox.service.pojo.User;

/**
 * Created by Cheng Guang on 2016/9/23.
 */
public class UserServiceImpl implements UserService {

    public User getUser(Long id) {
        return new User(id,"cheng");
    }
}

