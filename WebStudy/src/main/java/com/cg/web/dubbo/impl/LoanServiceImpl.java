package com.cg.web.dubbo.impl;

import com.cg.web.dubbo.LoanService;

/**
 * Created by Cheng Guang on 2016/9/20.
 */
public class LoanServiceImpl implements LoanService {

    @Override
    public String getApplCde() {
        return String.valueOf(System.currentTimeMillis());
    }
}