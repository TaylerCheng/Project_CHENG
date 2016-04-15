
package com.cg.javacore.cloneobject;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class CloneTest
{

	public static void main( String[] args )
	{
		// serializer();
		// reflecttest();
	}

	private static void reflecttest( ) throws Exception
	{
		Class c = Father.class;
		Method method = c.getDeclaredMethod( "sayYes" );
		if ( !method.isAccessible( ) )
		{
			method.setAccessible( true );
		}
		Father f = (Father) c.newInstance( );
		method.invoke( f );
	}

	private static void serializer( )
	{
		Father father = new Father( "cheng" );
		Student s = new Student( father );

		Student s2 = CloneUtils.cloneObject( s );
		System.out.println( s2 );
	}

}

class CloneUtils
{

	public static <T extends Serializable> T cloneObject( T obj )
	{
		T objClone = null;

		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream( );
			ObjectOutputStream oos = new ObjectOutputStream( baos );
			oos.writeObject( obj );
			oos.close( );

			ByteArrayInputStream bais = new ByteArrayInputStream(
					baos.toByteArray( ) );
			ObjectInputStream ois = new ObjectInputStream( bais );
			objClone = (T) ois.readObject( );
			ois.close( );
		}
		catch ( Exception e )
		{
			// TODO: handle exception
		}

		return objClone;

	}

}
