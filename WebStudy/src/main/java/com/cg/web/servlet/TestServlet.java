package com.cg.web.servlet;

import com.alibaba.dubbo.common.utils.IOUtils;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by Cheng Guang on 2016/9/6.
 */
public class TestServlet extends HttpServlet
{

	@Override
	protected void doGet( HttpServletRequest req, HttpServletResponse resp )
			throws ServletException, IOException
	{
		ServletInputStream inputStream = req.getInputStream();
		System.out.println("----START-----");
		System.out.println("getContextPath: "+req.getRequestURI()+req.getServletPath());
		System.out.println("getQueryString: "+req.getQueryString());
		String data = IOUtils.read(new InputStreamReader(inputStream));
		System.out.println(data);
		System.out.println("----STOP-----");
	}

	@Override
	protected void doPost( HttpServletRequest req, HttpServletResponse resp )
			throws ServletException, IOException
	{
		ServletInputStream inputStream = req.getInputStream();
		System.out.println("----START-----");
		String data = IOUtils.read(new InputStreamReader(inputStream));
		System.out.println(data);
		System.out.println("----STOP-----");
	}
}
