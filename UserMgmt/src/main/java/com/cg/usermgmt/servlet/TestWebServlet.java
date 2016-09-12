package com.cg.usermgmt.servlet;

import com.cg.usermgmt.utils.CheckCodeUtil;

import javax.imageio.ImageIO;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Random;

/**
 * Created by cheng on 2016/5/29.
 */
public class TestWebServlet extends HttpServlet
{

	@Override
	protected void doGet( HttpServletRequest request,
			HttpServletResponse response ) throws ServletException, IOException
	{
		response.setHeader( "Content-Type", "text/html; charset=UTF-8" );
		try
		{
			Thread.currentThread().sleep(120000);
		}
		catch ( InterruptedException e )
		{
			e.printStackTrace( );
		}
		// 创建输出流
		OutputStream out = response.getOutputStream( );
		PrintWriter pw = new PrintWriter( out );
		pw.println( "你好，这只是一个测试" );
		// 关闭输出流
		pw.close( );
	}

	@Override
	protected void doPost( HttpServletRequest req, HttpServletResponse resp )
			throws ServletException, IOException
	{
		doGet( req, resp );
	}
}
