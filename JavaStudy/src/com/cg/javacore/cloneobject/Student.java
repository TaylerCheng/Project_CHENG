package com.cg.javacore.cloneobject;

import java.io.Serializable;
import java.util.LinkedHashMap;

public class Student implements Serializable
{

	public Father f;
	public String name;

	public Student( String name )
	{
		this.name = name;
	}

	public Student( Father father )
	{
		f = father;
	}

	public String getName( )
	{
		return name;
	}

	public void setName( String name )
	{
		this.name = name;
	}

	protected Object add( LinkedHashMap map )
	{
		System.out.println( "LinkedHashMap" );
		return null;
	}

	@Override
	public boolean equals( Object obj )
	{
		if ( !( obj instanceof Student ) )
		{
			return false;
		}
		Student student = (Student) obj;
		return name.equals( student.getName( ) );
	}

	@Override
	public String toString( )
	{
		return "�ҵĸ����ǣ�" + f.toString( );
	}
}
