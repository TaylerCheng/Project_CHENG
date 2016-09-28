package com.cg.web.cxf;

/**
 * Created by Cheng Guang on 2016/9/6.
 */

import com.cg.web.cxf.jaxws.impl.HelloWorldImpl;

import javax.xml.ws.Endpoint;

public class WebServiceApp
{

	public static void main( String[] args )
	{
		System.out.println( "Web jaxws start" );
		HelloWorldImpl implementor = new HelloWorldImpl( );
		String address = "http://localhost:8080/helloWorld";
		Endpoint.publish( address, implementor );
		System.out.println( "Web jaxws started" );
	}
}
