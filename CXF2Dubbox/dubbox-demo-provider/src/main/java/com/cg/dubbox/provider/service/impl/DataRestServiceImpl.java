package com.cg.dubbox.provider.service.impl;

import com.alibaba.dubbo.config.annotation.Service;
import com.cg.dubbox.api.service.DataRestService;

/**
 * H5前端对用户进行信息采集时，提供Restful访问格式的数据服务
 * <p>
 * Created by Cheng Guang on 2016/10/14.
 */
@Service(protocol = "rest")
public class DataRestServiceImpl implements DataRestService {

    @Override
    public String getCitiesData() {
        return "abc";
    }

    @Override
    public String getAcademiesData(Integer cityCode) {
        return "abc";
    }
}
