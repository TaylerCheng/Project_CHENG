package com.cg.web.servlet;

import com.alibaba.dubbo.common.utils.IOUtils;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;

/**
 * Created by Cheng Guang on 2016/9/6.
 */
public class TestServlet extends HttpServlet
{

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		ServletInputStream inputStream = req.getInputStream();
		System.out.println("----START-----");
		System.out.println("getRequestURI: " + req.getRequestURI());
		System.out.println("getQueryString: " + req.getQueryString());
		String data = IOUtils.read(new InputStreamReader(inputStream));
		System.out.println("Request data: "+data);
		ServletOutputStream outputStream = resp.getOutputStream();
		PrintStream printStream = new PrintStream(outputStream);
		printStream.print("Hello,Cheng Guang!");
		printStream.close();
		System.out.println("----STOP-----");
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		System.out.println("----START-----");
		//		System.out.println(req.getParameter("a"));
		//		ServletOutputStream outputStream = resp.getOutputStream();
		//		PrintStream printStream = new PrintStream(outputStream);
		//		printStream.print("a = " + req.getParameter("a"));
		//		printStream.close();

		/**
		 * getParameter() 和 getInputStream()先到先得
		 */
		//		System.out.println("getContextPath: "+req.getRequestURI());
//		req.setCharacterEncoding("utf-8");
//		String b = req.getParameter("b");
//		System.out.println("req.getParameter:    " + b);
		//		System.out.println(new String(b.getBytes("ISO-8859-1"),"utf-8"));

		//		ServletInputStream inputStream = req.getInputStream();
		//		String data = IOUtils.read(new InputStreamReader(inputStream));
		//		System.out.println(data);
		this.doGet(req, resp);
		System.out.println("----STOP-----");
	}

}
