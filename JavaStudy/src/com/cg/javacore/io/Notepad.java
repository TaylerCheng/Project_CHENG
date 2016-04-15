package com.cg.javacore.io;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JTextArea;

import com.sun.jmx.mbeanserver.JmxMBeanServer;

public class Notepad extends JFrame implements ActionListener {
	// 主界面
	JTextArea jta;
	// 菜单条
	JMenuBar jmb;
	// 菜单
	JMenu jm;
	// 选项
	JMenuItem jmi1, jmi2;

	public static void main(String[] args) {
		Notepad notepad = new Notepad();
	}

	public Notepad() {
		jta = new JTextArea();
		jmb = new JMenuBar();

		jm = new JMenu("文件");
		jm.setMnemonic('F');
		jmi1 = new JMenuItem("打开(o)");
		jmi1.addActionListener(this);
		jmi1.setActionCommand("open");

		jmi2 = new JMenuItem("保存(s)");
		jmi2.setActionCommand("save");
		jmi2.addActionListener(this);

		jm.add(jmi1);
		jm.add(jmi2);

		jmb.add(jm);

		this.setJMenuBar(jmb);
		this.add(jta);

		this.setLocation(400, 300);
		this.setTitle("记事本");
		this.setSize(800, 600);
		this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		this.setVisible(true);
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		switch (e.getActionCommand()) {
		case "open":
			watchFile();
			break;
		case "save":
			saveFile();
			break;
		default:
			break;
		}
	}

	private void saveFile() {
		JFileChooser jfc1 = new JFileChooser();
		jfc1.setDialogTitle("另存为");
		jfc1.showSaveDialog(null);
		jfc1.setVisible(true);

		FileWriter fw = null;
		BufferedWriter bw = null;
		try {
			String allResult = this.jta.getText();
			File f = new File(jfc1.getCurrentDirectory().getAbsolutePath()+File.separator+jfc1.getSelectedFile().getName()+"txt");
			fw = new FileWriter(f);
			bw = new BufferedWriter(fw);
			if (f.isDirectory() || !f.exists()) {
				return;
			}else{		
				bw.write(allResult);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	private void watchFile() {
		JFileChooser jfc1 = new JFileChooser();
		jfc1.setDialogTitle("请选择文件");
		jfc1.showOpenDialog(null);
		jfc1.setVisible(true);
		File f = jfc1.getSelectedFile();
		if (f.isDirectory() || !f.exists()) {
			return;
		}
		FileReader fr = null;
		BufferedReader br = null;
		try {
			fr = new FileReader(f);
			br = new BufferedReader(fr);
			String allResult = "";
			String line;
			while ((line = br.readLine()) != null) {
				allResult += line + "\r\n";
			}
			jta.setText(allResult);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

}
