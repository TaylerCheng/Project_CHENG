package com.cg.web.dubbo.impl;

import com.cg.web.dubbo.CustomerService;

/**
 * Created by Cheng Guang on 2016/9/20.
 */
public class CustomerServiceImpl implements CustomerService {
    @Override
    public String getName(String name) {
        System.out.println("Hello, " + name);
        return "Hello, " + name;
    }
}