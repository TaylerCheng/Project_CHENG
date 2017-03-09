package com.cg.temp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

public class Dealtype {

	public void fuction1() throws Exception {
		InputStreamReader reader = new InputStreamReader(new FileInputStream(
				new File("D:/input.txt")));
		BufferedReader br = new BufferedReader(reader);
		String line;

		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(new File("D:/output.txt")));
		BufferedWriter bw = new BufferedWriter(writer);
		// _avg _mid _10pct _90pct _sd _max
		while ((line = br.readLine()) != null) {
			String[] words = line.split(" ");
			bw.write("   ");
			bw.write(words[3] + "_avg");
			bw.write(" DOUBLE,");
			bw.newLine();
			bw.write("   ");
			bw.write(words[3] + "_mid");
			bw.write(" DOUBLE,");
			bw.newLine();
			bw.write("   ");
			bw.write(words[3] + "_10pct");
			bw.write(" DOUBLE,");
			bw.newLine();
			bw.write("   ");
			bw.write(words[3] + "_90pct");
			bw.write(" DOUBLE,");
			bw.newLine();
			bw.write("   ");
			bw.write(words[3] + "_sd");
			bw.write(" DOUBLE,");
			bw.newLine();
			bw.write("   ");
			bw.write(words[3] + "_max");
			bw.write(" DOUBLE,");
			bw.newLine();
		}
		br.close();
		bw.close();

	}

	public void fuction2() throws Exception {
		InputStreamReader reader = new InputStreamReader(new FileInputStream(
				new File("D:/input.txt")));
		BufferedReader br = new BufferedReader(reader);
		String line;

		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(new File("D:/output.txt")));
		BufferedWriter bw = new BufferedWriter(writer);
		// avg(col),percentile(col, 0.5),percentile(col, 0.1),percentile(col,
		// 0.9),stddev_pop(col),max(col)
		while ((line = br.readLine()) != null) {
			String[] words = line.split(" ");
			bw.write("avg(" + words[3] + ")" + " as " + words[3] + "_avg,");
			if (words[4].equals("INT,") || words[4].equals("BIGINT,")) {
				bw.write("percentile(" + words[3] + ",0.5)" + " as " + words[3]
						+ "_mid,");
				bw.write("percentile(" + words[3] + ",0.1)" + " as " + words[3]
						+ "_10pct,");
				bw.write("percentile(" + words[3] + ",0.9)" + " as " + words[3]
						+ "_90pct,");
			} else if (words[4].equals("DOUBLE,")) {
				bw.write("percentile_approx(" + words[3] + ",0.5)" + " as "
						+ words[3] + "_mid,");
				bw.write("percentile_approx(" + words[3] + ",0.1)" + " as "
						+ words[3] + "_10pct,");
				bw.write("percentile_approx(" + words[3] + ",0.9)" + " as "
						+ words[3] + "_90pct,");
			} else {
				bw.write("percentile_approx(cast(" + words[3]
						+ " as DOUBLE),0.5)" + " as " + words[3] + "_mid,");
				bw.write("percentile_approx(cast(" + words[3]
						+ " as DOUBLE),0.1)" + " as " + words[3] + "_10pct,");
				bw.write("percentile_approx(cast(" + words[3]
						+ " as DOUBLE),0.9)" + " as " + words[3] + "_90pct,");
			}
			bw.write("stddev_pop(" + words[3] + ")" + " as " + words[3]
					+ "_sd,");
			bw.write("max(" + words[3] + ")" + " as " + words[3] + "_max,");
		}
		br.close();
		bw.close();
	}

	private void getMinValue() throws IOException {
		// 读取坡度数据
		File pd = new File("C:/Users/cheng/Desktop/pd.txt");
		InputStreamReader pd_isr = new InputStreamReader(
				new FileInputStream(pd));
		BufferedReader pd_br = new BufferedReader(pd_isr);
		int pd_dis[] = new int[63];
		float pd_i[] = new float[63];
		int stage = 0;
		String line;
		while ((line = pd_br.readLine()) != null && stage < 63) {
			String[] nums = line.split("\t");
			if (nums != null && nums.length == 2) {
				pd_dis[stage] = Integer.valueOf(nums[1]);
				pd_i[stage] = -Float.valueOf(nums[0]);// 取负数
			}
			stage++;
		}

		// 读取曲率数据
		File ql = new File("C:/Users/cheng/Desktop/ql.txt");
		InputStreamReader ql_isr = new InputStreamReader(
				new FileInputStream(ql));
		BufferedReader ql_br = new BufferedReader(ql_isr);
		int ql_dis[] = new int[76];
		float ql_r[] = new float[76];
		int stage2 = 0;
		String line2;
		while ((line2 = ql_br.readLine()) != null && stage2 < 76) {
			String[] nums = line2.split("\t");
			if (nums != null && nums.length == 2) {
				ql_dis[stage2] = Integer.valueOf(nums[1]);
				ql_r[stage2] = Float.valueOf(nums[0]);
			}
			stage2++;
		}
		pd_br.close();
		ql_br.close();

		int i = 0;
		int j = 0;
		float min_b = 10000;
		float b = 0;
		
		while (i <= 63 && j <76) {
			if (ql_r[j]==0) {
				b = pd_i[i];
			}else {
				b = pd_i[i] + 600 / ql_r[j];	
			}
			
			if (b < min_b) {
				min_b = b;
			}
			System.out.print("("+i+","+j+")");
			System.out.print("("+pd_dis[i]+","+ql_dis[j]+")");
			System.out.println("\t"+b);
			//System.out.print("("+pd_i[i]+","+ql_r[j]+")");
			//System.out.println("\t"+min_b);
			if (pd_dis[i] < ql_dis[j]) {
				i++;
			} else {
				j++;
			}			
		}
		if (i!=63) {
			b = pd_i[i] + 600 / ql_r[j-1];	
			if (b < min_b) {
				min_b = b;
			}
			i++;
		}
		if (j<76) {
			b = pd_i[i-1] + 600 / ql_r[j];	
			if (b < min_b) {
				min_b = b;
			}
			j++;
		}

		

	}

	public static void main(String[] args) throws Exception {
		Dealtype dt = new Dealtype();
		// dt.fuction2();
		dt.getMinValue();
	}
}
