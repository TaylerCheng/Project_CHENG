package com.cg.javacore.cstest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class Server {

	public ServerSocket serverSocket = null;
	public Socket s = null;
	public BufferedReader br = null;
	public BufferedReader br2 = null;

	public Server(int port) {
		try {
            //����������
			serverSocket = new ServerSocket(port);
            //�ȴ�����			
			//System.out.println("����������,�ȴ�����~~~");
			s = serverSocket.accept();
            //�����������
			br = new BufferedReader(new InputStreamReader(s.getInputStream()));
			//��������
			PrintWriter pw = new PrintWriter(s.getOutputStream(), true);
			//�ӿ���̨��ȡ�ַ���
			br2 = new BufferedReader(new InputStreamReader(System.in));

			while (true) {
				String requset = br.readLine();
				System.out.println("�̹�:  " + requset);
				System.out.print("�۳���:");
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
