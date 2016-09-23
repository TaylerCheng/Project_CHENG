package com.cg.dubbox.service.impl;

import com.alibaba.dubbo.rpc.RpcContext;
import com.cg.dubbox.service.UserRestService;
import com.cg.dubbox.service.UserService;
import com.cg.dubbox.service.pojo.User;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;

/**
 * Created by Cheng Guang on 2016/9/23.
 */
public class UserRestServiceImpl implements UserRestService {

    private UserService userService;

    public void setUserService(UserService userService) {
        this.userService = userService;
    }

    @GET
    @Path("{id : \\d+}")
    public User getUser(@PathParam("id") Long id) {
        if (RpcContext.getContext().getRequest(HttpServletRequest.class) != null) {
            System.out.println("Client IP address from RpcContext: " + RpcContext.getContext().getRequest(HttpServletRequest.class).getRemoteAddr());
        }
        if (RpcContext.getContext().getResponse(HttpServletResponse.class) != null) {
            System.out.println("Response object from RpcContext: " + RpcContext.getContext().getResponse(HttpServletResponse.class));
        }
        return userService.getUser(id);
    }
}
