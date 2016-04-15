package com.cg.javacore.cstest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Client {
	
	public Socket s =null;
	public BufferedReader br = null;
	public BufferedReader br2 = null;
	public OutputStream os = null;
	
	public Client(int port){
		try {			
			s =new Socket("localhost", 9999);
            System.out.println("已建立和邵晨晨的连接...");
            br2 = new BufferedReader(new InputStreamReader(System.in));
			PrintWriter pw = new PrintWriter(s.getOutputStream(),true);
			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			
			while (true) {
				//从控制台写入请求内容
				System.out.print("程广:\t");				
				String requset = br2.readLine();
				//生成请求内容
				pw.println(requset);
							
				//获得返回内容
				String response = br.readLine();
				System.out.println("邵晨晨:\t"+response);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				os.close();
				br.close();
				br2.close();
				s.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}	
	}
	
	public static void main(String[] args) {
		Client server = new Client(9999);
	}

}
