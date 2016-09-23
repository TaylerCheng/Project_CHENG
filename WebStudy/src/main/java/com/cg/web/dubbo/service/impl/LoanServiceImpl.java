package com.cg.web.dubbo.service.impl;

import com.cg.web.dubbo.service.CustomerService;
import com.cg.web.dubbo.service.LoanService;

/**
 * Created by Cheng Guang on 2016/9/20.
 */
public class LoanServiceImpl implements LoanService {

    @Override
    public String getApplCde() {
        return String.valueOf(System.currentTimeMillis());
    }
}