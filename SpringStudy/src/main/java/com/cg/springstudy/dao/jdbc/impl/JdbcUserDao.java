package com.cg.springstudy.dao.jdbc.impl;

import com.cg.springstudy.dao.mybatis.pojo.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Cheng Guang on 2016/9/19.
 */
public class JdbcUserDao implements UserDao {

    //JDBC索引参数法
    private final static String SQL_INSERT = "insert into user(name,age,sex) "
            + "values(?,?,?);";
    //命名参数法
    private final static String SQL_UPDATE = "update user set name = :name ,age = :age ,sex = :sex  "
            + "where id =  :id";

    @Autowired
    private SimpleJdbcTemplate jdbcTemplate;

    @Override
    public void addUser(User user) {
        jdbcTemplate.update(SQL_INSERT, user.getName(), user.getAge(), user.getSex());
    }

    @Override
    public void updateUser(User user) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("id", user.getId());
        params.put("name", user.getName());
        params.put("age", user.getAge());
        params.put("sex", user.getSex());
        jdbcTemplate.update(SQL_UPDATE, params);
    }

}
