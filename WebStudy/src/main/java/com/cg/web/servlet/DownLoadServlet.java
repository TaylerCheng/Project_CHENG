package com.cg.web.servlet;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by Cheng Guang on 2016/9/6.
 */
@WebServlet("/download/*")
public class DownLoadServlet extends HttpServlet
{

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
		ServletOutputStream outputStream = null;
		BufferedInputStream bi = null;
		try {
			outputStream = resp.getOutputStream();
			FileInputStream fileInputStream = new FileInputStream("F:\\sample_10w_201703_part1.rar");
			bi = new BufferedInputStream(fileInputStream);
			byte[] buffer = new byte[1024];
			int len = 0;
			while ((len = bi.read(buffer)) != -1) {
				outputStream.write(buffer, 0, len);
				System.out.println("正在下载...");
				Thread.currentThread().sleep(10);
			}
		} catch (Exception e) {
			System.out.println("网络异常");
		} finally {
			if (bi != null) {
				try {
					bi.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			if (outputStream != null) {
				try {
					outputStream.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

	}

}
