package com.cg.springstudy.tx;

import com.cg.springstudy.bean.service.IMyApiService;
import com.cg.springstudy.dao.mybatis.mapper.UserMapper;
import com.cg.springstudy.dao.mybatis.pojo.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * @author： Cheng Guang
 * @date： 2017/11/27.
 */
@Service
public class UserServiceImpl {

    @Autowired
    private UserMapper userMapper;
    @Autowired
    private IMyApiService myApiService;


    @Transactional
    public void saveError(User user1, User user2) {
        try {
            userMapper.insertSelective(user1);
            myApiService.doSomeError(user2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("------END---------");
    }

//    @Transactional
//    public void doSomeError() {
//        try {
//            userMapper.insertSelective(null);
//            System.out.println(10 / 0); // 引发异常
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    /**
     * 保存两个user对象. 添加了spring事务注解
     */
    @Transactional
    public final void saveErrorFinal(User user1, User user2) {
        UserMapper userMapper = getUserMapper(); // 此处需要使用getUserDao方法. 不能直接使用userMapper
        userMapper.insert(user1);
        System.out.println(10 / 0); // 引发异常
        userMapper.insert(user2);
    }

    /**
     * 静态方法. 添加了spring事务注解
     */
    @Transactional
    public static void saveErrorStatic(UserMapper userMapper, User user1, User user2) {
        userMapper.insert(user1);
        System.out.println(10 / 0); // 引发异常
        userMapper.insert(user2);
    }

    /**
     * 私有方法保存方法. 添加了spring事务注解
     */
    @Transactional
    private void saveErrorPrivate(User user1, User user2) {
        userMapper.insert(user1);
        System.out.println(10 / 0); // 引发异常
        userMapper.insert(user2);
    }

    public UserMapper getUserMapper() {
        return userMapper;
    }
}
