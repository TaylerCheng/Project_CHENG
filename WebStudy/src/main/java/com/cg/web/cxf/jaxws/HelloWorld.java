package com.cg.web.cxf.jaxws;

/**
 * Created by Cheng Guang on 2016/9/6.
 */

import com.cg.web.pojo.User;

import java.util.List;

import javax.jws.WebParam;
import javax.jws.WebService;

@WebService
public interface HelloWorld
{

	String sayHi( @WebParam(name = "text") String text );

	String sayHiToUser( User user );

	String[] SayHiToUserList( List<User> userList );
}


