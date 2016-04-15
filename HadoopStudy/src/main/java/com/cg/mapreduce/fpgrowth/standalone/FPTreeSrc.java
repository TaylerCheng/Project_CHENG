package com.cg.mapreduce.fpgrowth.standalone;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

public class FPTreeSrc {

	private int minSuport;
	private static int count;
	private static String writePath;
	private static String h1writePath;
	private static FileOutputStream fos;
	private static ObjectOutputStream oos;
	private static FileWriter fw;
	private static BufferedWriter bw;
	private static boolean isFirst = true;

	public int getMinSuport() {
		return minSuport;
	}

	public void setMinSuport(int minSuport) {
		this.minSuport = minSuport;
	}

	// �����ɸ��ļ��ж���Transaction Record
	public List<List<String>> readTransRocords(String... filenames) {
		List<List<String>> transaction = null;
		if (filenames.length > 0) {
			transaction = new LinkedList<List<String>>();
			for (String filename : filenames) {
				try {
					BufferedReader br = new BufferedReader(
							new InputStreamReader(new FileInputStream(filename)));
					for (String line = br.readLine(); line != null; line = br
							.readLine()) {
						if (line.length() == 0 || "".equals(line))
							continue;
						String[] str = line.split(" ");
						LinkedList<String> litm = new LinkedList<String>();
						for (int i = 0; i < str.length; i++) {
							litm.add(str[i].trim());
						}
						transaction.add(litm);
					}

//					FileReader fr = new FileReader(filename);
//					BufferedReader br = new BufferedReader(fr);
//					try {
//						String line;
//						List<String> record;
//						while ((line = br.readLine()) != null) {
//							if (line.trim().length() > 0) {
//								String str[] = line.split(" ");
//								record = new LinkedList<String>();
//								for (String w : str)
//									record.add(w);
//								transaction.add(record);
//							}
//						}
//					} finally {
//						br.close();
//					}
				} catch (IOException ex) {
					System.out.println("Read transaction records failed."
							+ ex.getMessage());
					System.exit(1);
				}
			}
		}
		return transaction;
	}

	// FP-Growth�㷨
	public void FPGrowth(List<List<String>> transRecords,
			List<String> postPattern) {
		// ������ͷ��ͬʱҲ��Ƶ��1�
		ArrayList<TreeNode> HeaderTable = buildHeaderTable(transRecords);
		// ����FP-Tree
		TreeNode treeRoot = buildFPTreeSrc(transRecords, HeaderTable);
		// ���FP-TreeΪ���򷵻�
		if (treeRoot.getChildren() == null
				|| treeRoot.getChildren().size() == 0)
			return;
		// �����ͷ���ÿһ��+postPattern
		for (TreeNode header : HeaderTable) {
			try {
				bw.append(header.getCount() + "\t" + header.getName());
				if (postPattern != null) {
					for (String ele : postPattern)
						bw.append("\t" + ele);
				}
				bw.newLine();
				count++;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			/*
			 * System.out.print(header.getCount() + "\t" + header.getName()); if
			 * (postPattern != null) { for (String ele : postPattern)
			 * System.out.print("\t" + ele); } System.out.println(); count++;
			 */
		}
		// �ҵ���ͷ���ÿһ�������ģʽ��������ݹ����
		for (TreeNode header : HeaderTable) {
			// ��׺ģʽ����һ��
			List<String> newPostPattern = new LinkedList<String>();
			newPostPattern.add(header.getName());
			if (postPattern != null)
				newPostPattern.addAll(postPattern);
			// Ѱ��header������ģʽ��CPB������newTransRecords��
			List<List<String>> newTransRecords = new LinkedList<List<String>>();
			TreeNode backnode = header.getNextHomonym();
			while (backnode != null) {
				int counter = backnode.getCount();
				List<String> prenodes = new ArrayList<String>();
				TreeNode parent = backnode;
				// ����backnode�����Ƚڵ㣬�ŵ�prenodes��
				while ((parent = parent.getParent()).getName() != null) {
					prenodes.add(parent.getName());
				}
				while (counter-- > 0) {
					newTransRecords.add(prenodes);
				}
				backnode = backnode.getNextHomonym();
			}
			// �ݹ����
			FPGrowth(newTransRecords, newPostPattern);
		}
	}

	// ������ͷ��ͬʱҲ��Ƶ��1�
	public ArrayList<TreeNode> buildHeaderTable(List<List<String>> transRecords) {
		ArrayList<TreeNode> F1 = null;
		if (transRecords.size() > 0) {
			F1 = new ArrayList<TreeNode>();
			Map<String, TreeNode> map = new HashMap<String, TreeNode>();
			// �����������ݿ��и����֧�ֶ�
			for (List<String> record : transRecords) {
				for (String item : record) {
					if (!map.keySet().contains(item)) {
						TreeNode node = new TreeNode(item);
						node.setCount(1);
						map.put(item, node);
					} else {
						map.get(item).countIncrement(1);
					}
				}
			}
			if (isFirst) {
				try {
					oos.writeObject(map);
				} catch (IOException e) {
					e.printStackTrace();
				}
				isFirst = false;
			}
			// ��֧�ֶȴ��ڣ�����ڣ�minSup������뵽F1��
			Set<String> names = map.keySet();
			for (String name : names) {
				TreeNode tnode = map.get(name);
				if (tnode.getCount() >= minSuport) {
					F1.add(tnode);
				}
			}
			Collections.sort(F1);
			return F1;
		} else {
			return null;
		}
	}

	// ����FP-Tree
	public TreeNode buildFPTreeSrc(List<List<String>> transRecords,
			ArrayList<TreeNode> F1) {
		TreeNode root = new TreeNode(); // �������ĸ��ڵ�
		for (List<String> transRecord : transRecords) {
			LinkedList<String> record = sortByF1(transRecord, F1);
			TreeNode subTreeRoot = root;
			TreeNode tmpRoot = null;
			if (root.getChildren() != null) {
				while (!record.isEmpty()
						&& (tmpRoot = subTreeRoot.findChild(record.peek())) != null) {
					tmpRoot.countIncrement(1);
					subTreeRoot = tmpRoot;
					record.poll();
				}
			}
			addNodes(subTreeRoot, record, F1);
		}
		return root;
	}

	// �ѽ��׼�¼�����Ƶ������������
	public LinkedList<String> sortByF1(List<String> transRecord,
			ArrayList<TreeNode> F1) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		for (String item : transRecord) {
			// ����F1�Ѿ��ǰ��������еģ�
			for (int i = 0; i < F1.size(); i++) {
				TreeNode tnode = F1.get(i);
				if (tnode.getName().equals(item)) {
					map.put(item, i);
				}
			}
		}
		ArrayList<Entry<String, Integer>> al = new ArrayList<Entry<String, Integer>>(
				map.entrySet());
		Collections.sort(al, new Comparator<Map.Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> arg0,
					Entry<String, Integer> arg1) {
				// ��������
				return arg0.getValue() - arg1.getValue();
			}
		});
		LinkedList<String> rest = new LinkedList<String>();
		for (Entry<String, Integer> entry : al) {
			rest.add(entry.getKey());
		}
		return rest;
	}

	// ��record��Ϊancestor�ĺ����������
	public void addNodes(TreeNode ancestor, LinkedList<String> record,
			ArrayList<TreeNode> F1) {
		if (record.size() > 0) {
			while (record.size() > 0) {
				String item = record.poll();
				TreeNode leafnode = new TreeNode(item);
				leafnode.setCount(1);
				leafnode.setParent(ancestor);
				ancestor.addChild(leafnode);

				for (TreeNode f1 : F1) {
					if (f1.getName().equals(item)) {
						while (f1.getNextHomonym() != null) {
							f1 = f1.getNextHomonym();
						}
						f1.setNextHomonym(leafnode);
						break;
					}
				}

				addNodes(leafnode, record, F1);
			}
		}
	}

	public static void main(String[] args) throws IOException {
		long startTime = System.currentTimeMillis();
		FPTreeSrc FPTreeSrc = new FPTreeSrc();

		int total = 1;
		int minSuport = total * 500;

		FPTreeSrc.setMinSuport(minSuport);

		String[] path = new String[total];
		for (int i = 0; i < path.length; i++) {
			path[i] = "G:/result/datasrc/data" + (i + 1);
		}
		List<List<String>> transRecords = FPTreeSrc.readTransRocords(path);

		FileOutputStream fos0 = new FileOutputStream(
				"G:/result/transRecords/DB");
		ObjectOutputStream oos0 = new ObjectOutputStream(fos0);
		oos0.writeObject(transRecords);
		oos0.close();

		fos = new FileOutputStream("G:/result/head/data_h"+total);
		oos = new ObjectOutputStream(fos);

		writePath = "G:/result/frequent/DB_fre"+total;
		fw = new FileWriter(writePath);
		bw = new BufferedWriter(fw);

		FPTreeSrc.FPGrowth(transRecords, null);
		oos.close();
		bw.close();
		System.out.println("������" + count);
		long endTime = System.currentTimeMillis();
		System.out.println("����ʱ�� " + (endTime - startTime) + "ms");
	}
}
