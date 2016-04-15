
package com.cg.mchtdata.servlet;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.cg.mchtdata.bean.Book;

public class BuyBook extends HttpServlet
{

	public static Map<String, String> nameMap = new HashMap<String, String>( );
	static
	{
		nameMap.put( "1", "java书" );
		nameMap.put( "2", "Time杂志" );
		nameMap.put( "3", "Ruby书" );
	}

	protected void doGet( HttpServletRequest request,
			HttpServletResponse response ) throws ServletException, IOException
	{
		String id = request.getParameter( "id" );
		Map<String, Book> books = (Map) request.getSession( ).getAttribute(
				"books" );
		if ( books == null )
		{
			books = new HashMap<String, Book>( );
			request.getSession( ).setAttribute( "books", books );
		}
		Book book = books.get( id );
		if ( book == null )
		{
			book = new Book( );
			book.setName( nameMap.get( id ) );
			books.put( id, book );
		}
		else
		{
			book.setNum( book.getNum( ) + 1 );
		}

		OutputStream out = response.getOutputStream( );
		PrintWriter pw = new PrintWriter( out );
		pw.println( "<h1>购物车</h1>" );

		Set<String> ids = books.keySet( );
		for ( String bookId : ids )
		{
			Book book2 = books.get( bookId );
			pw.println( book2.getName( ) + ": " + book2.getNum( ) +"<br/><br/>");
		}
		pw.println( "<a href = /MyProject/BookMall>返回网上书城</a>" );
		pw.close( );
	}

	protected void doPost( HttpServletRequest request,
			HttpServletResponse response ) throws ServletException, IOException
	{
		doGet( request, response );
	}

}
