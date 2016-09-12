package com.cg.mchtdata.servlet;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ExcuteShellServlet extends HttpServlet {

	public String executeShell(String shellCommand) throws IOException {
		int success = 0;
		StringBuffer stringBuffer = new StringBuffer();
		BufferedReader input=null;
		try {			
			Process pid = Runtime.getRuntime().exec(shellCommand);
			if (pid != null) {				
				input = new BufferedReader(new InputStreamReader(pid.getInputStream()));
				pid.waitFor();
			} 
			String line = null;
			// 读取Shell的输出内容，并添加到stringBuffer中
			while (input != null
					&& (line = input.readLine()) != null) {
				stringBuffer.append(line).append("\r\n");
			}
			return stringBuffer.toString();
		} catch (Exception ioe) {
			stringBuffer.append("执行Shell命令时发生异常：\r\n").append(ioe.getMessage())
					.append("\r\n");
		} finally {
			input.close();					
			success = 1;
		}
		return stringBuffer.toString();
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		request.setCharacterEncoding("utf-8");
		String mcht_place = request.getParameter("mcht_place");
		String enter_web_month = request.getParameter("enter_web_month");
		String latest_6_avg_month_amount = request
				.getParameter("latest_6_avg_month_amount");
		String latest_6_avg_month_num = request
				.getParameter("latest_6_avg_month_num");

//		String command = "hive -e 'SELECT count(*) FROM  t_sta_result ts WHERE ts.mcht_place ='"
//				+ mcht_place
//				+ "' AND ts.enter_web_month >="
//				+ enter_web_month
//				+ " AND ts.latest_6_avg_month_amount >="
//				+ latest_6_avg_month_amount
//				+ " AND latest_6_avg"
//				+ "_month_num >="
//				+ latest_6_avg_month_num+"'";
		
		//String command = "hive -e \"SELECT * FROM  student \"  > '/home/hadoop/testdata/student'";
		
		String command = "hive -e \"show tables;\"";
		String result = executeShell(command);
		
		if (result != null) {
			request.setAttribute("message", result);
			request.getRequestDispatcher("/message.jsp").forward(request,
					response);
			return;
		}
	}

	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		doGet(request, response);
	}
}