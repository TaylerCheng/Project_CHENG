package com.cg.web.cxf.jaxws.impl;

/**
 * Created by Cheng Guang on 2016/9/6.
 */

import com.cg.web.cxf.jaxws.HelloWorld;
import com.cg.web.pojo.User;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.jws.WebService;

@WebService(endpointInterface = "com.cg.web.cxf.jaxws.HelloWorld", serviceName = "helloWorld")
public class HelloWorldImpl implements HelloWorld
{

	Map<Integer, User> users = new LinkedHashMap<Integer, User>( );

	public String sayHi( String text )
	{
		return "Hello " + text;
	}

	public String sayHiToUser( User user )
	{
		users.put( users.size( ) + 1, user );
		return "Hello " + user.getName( );
	}

	public String[] SayHiToUserList( List<User> userList )
	{
		String[] result = new String[userList.size( )];
		int i = 0;
		for ( User u : userList )
		{
			result[i] = "Hello " + u.getName( );
			i++;
		}
		return result;
	}
}
