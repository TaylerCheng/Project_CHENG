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
import java.util.Random;

/**
 * Created by cheng on 2016/5/29.
 */
public class CreateCheckCodeServlet extends HttpServlet
{

	@Override
	protected void doGet( HttpServletRequest request,
			HttpServletResponse response ) throws ServletException, IOException
	{
		response.setHeader( "Pragma", "No-cache" );
		response.setHeader( "Cache-Control", "no-cache" );
		response.setDateHeader( "Expires", 0 );
		// 表明生成的响应是图片
		response.setContentType( "image/jpeg" );

		StringBuffer checkCode = new StringBuffer( );
		Random random = new Random( );
		for ( int i = 0; i < 4; i++ )
		{
			int itmp = random.nextInt( 26 ) + 65;
			checkCode.append( (char)itmp );
		}

		HttpSession session = request.getSession( true );
		session.setAttribute( "checkCode", checkCode.toString( ) );

		BufferedImage image = CheckCodeUtil
				.createCheckCode( checkCode.toString( ) );
		ImageIO.write( image, "JPEG", response.getOutputStream( ) );
	}

	@Override
	protected void doPost( HttpServletRequest req, HttpServletResponse resp )
			throws ServletException, IOException
	{
		doGet( req, resp );
	}
}
