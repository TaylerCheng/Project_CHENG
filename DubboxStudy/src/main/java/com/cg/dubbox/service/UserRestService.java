package com.cg.dubbox.service;

import com.cg.dubbox.service.pojo.User;

import javax.validation.constraints.Min;

/**
 * Created by Cheng Guang on 2016/9/23.
 */
public interface UserRestService {

    User getUser(@Min(value = 1L, message = "User ID must be greater than 1") Long id);

}
