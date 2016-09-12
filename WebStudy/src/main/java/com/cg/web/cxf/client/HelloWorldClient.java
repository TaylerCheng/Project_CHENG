package com.cg.web.cxf.client;

import com.cg.web.cxf.service.HelloWorld;
import com.cg.web.cxf.pojo.User;
import org.apache.cxf.jaxws.JaxWsProxyFactoryBean;

/**
 * Created by Cheng Guang on 2016/9/6.
 */
public class HelloWorldClient
{

	public static void main( String[] args )
	{
		JaxWsProxyFactoryBean svr = new JaxWsProxyFactoryBean( );
		svr.setServiceClass( HelloWorld.class );
		svr.setAddress( "http://localhost:8080/helloWorld" );
		HelloWorld hw = (HelloWorld) svr.create( );
		User user = new User( );
		user.setName( "Cheng" );
		user.setDescription( "test" );
		System.out.println( hw.sayHiToUser( user ) );
	}
}