package com.cg.temp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class GetMinValue {
	private static void getMinValue() throws IOException {
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

		int start = 0;
		int end = 0;
		while (i <= 63 && j < 76) {
			start=end;
			if (ql_r[j] == 0) {
				b = pd_i[i];
			} else {
				b = pd_i[i] + 600 / ql_r[j];
			}

			if (b < min_b) {
				min_b = b;
			}

			if (pd_dis[i] < ql_dis[j]) {
				end=pd_dis[i];
				i++;
			} else {
				end=ql_dis[j];
				j++;
			}			
			// System.out.print("(" + i + "," + j + ")");
			// System.out.print("(" + pd_dis[i] + "," + ql_dis[j] + ")");
			// System.out.println("\t" + b);
			 System.out.print(end+"->"+start);
			 System.out.print("\t"+b);
			 System.out.println("\t"+min_b);
		     
		}
		while (i < 63) {
			b = pd_i[i] + 600 / ql_r[j - 1];
			if (b < min_b) {
				min_b = b;
			}
			i++;
		}
		while (j < 76) {
			b = pd_i[i - 1] + 600 / ql_r[j];
			if (b < min_b) {
				min_b = b;
			}
			j++;
		}

	}

	public static void main(String[] args) throws Exception {
		getMinValue();
	}
}
