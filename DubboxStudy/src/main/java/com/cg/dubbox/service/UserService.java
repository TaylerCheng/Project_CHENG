package com.cg.dubbox.service;

import com.cg.dubbox.service.pojo.User;

/**
 * Created by Cheng Guang on 2016/9/23.
 */
public interface UserService {
    User getUser(Long id);
}