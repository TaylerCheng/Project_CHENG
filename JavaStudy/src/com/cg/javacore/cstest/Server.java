package com.cg.javacore.cstest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
* 哈哈
*/
public class Server {

	public ServerSocket serverSocket = null;
	public Socket s = null;
	public BufferedReader br = null;
	public BufferedReader br2 = null;

	public Server(int port) {
		try {
            //建立服务器
			serverSocket = new ServerSocket(port);
            //等待连接			
			//System.out.println("服务器开启,等待连接~~~");
			s = serverSocket.accept();
            //获得输入内容
			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			//返回内容
			PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
			//从控制台获取字符串
			br2 = new BufferedReader(new InputStreamReader(System.in));

			while (true) {
				String requset = br.readLine();
				System.out.println("程广:  " + requset);
				System.out.print("邵晨晨:");
				String response = br2.readLine();
				pw.println(response);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
				br2.close();
				s.close();
				serverSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) {
		Server server = new Server(9999);
	}

}
