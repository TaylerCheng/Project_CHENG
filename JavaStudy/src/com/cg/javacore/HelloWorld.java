
package com.cg.javacore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

public abstract class HelloWorld
{

	public static void main( String[] args )
	{
		// Map map = System.getenv( );
		// Iterator it = map.entrySet( ).iterator( );
		// while ( it.hasNext( ) )
		// {
		// Entry entry = (Entry) it.next( );
		// System.out.print( entry.getKey( ) + "=" );
		// System.out.println( entry.getValue( ) );
		// }
		System.out.println( "current system environment variable:" );
		Map<String, String> map = System.getenv( );
		Set<Map.Entry<String, String>> entries = map.entrySet( );
		for ( Map.Entry<String, String> entry : entries )
		{
			System.out.println( entry.getKey( ) + ":" + entry.getValue( ) );
		}

		System.out.println( "---------------" );
		System.out.println( "current system properties:" );
		Properties properties = System.getProperties( );
		Set<Map.Entry<Object, Object>> set = properties.entrySet( );
		for ( Map.Entry<Object, Object> objectObjectEntry : set )
		{
			System.out.println( objectObjectEntry.getKey( ) + ":"
					+ objectObjectEntry.getValue( ) );
		}

	}

	public static String[] localSplit( String str, int limit )
	{
		char ch = 'a';
		int off = 0;
		int next = 0;
		boolean limited = limit > 0;
		ArrayList<String> list = new ArrayList<>( );
		while ( ( next = str.indexOf( ch, off ) ) != -1 )
		{
			if ( !limited || list.size( ) < limit - 1 )
			{
				list.add( str.substring( off, next ) );
				off = next + 1;
			}
			else
			{ // last one
				// assert (list.size() == limit - 1);
				list.add( str.substring( off, str.length( ) ) );
				off = str.length( );
				break;
			}
		}
		// If no match was found, return this
		if ( off == 0 )
			return new String[]{str};

		// Add remaining segment
		if ( !limited || list.size( ) < limit )
			list.add( str.substring( off, str.length( ) ) );

		// Construct result
		int resultSize = list.size( );
		if ( limit == 0 )
			while ( resultSize > 0 && list.get( resultSize - 1 ).length( ) == 0 )
				resultSize--;
		String[] result = new String[resultSize];
		return list.subList( 0, resultSize ).toArray( result );
	}

}
