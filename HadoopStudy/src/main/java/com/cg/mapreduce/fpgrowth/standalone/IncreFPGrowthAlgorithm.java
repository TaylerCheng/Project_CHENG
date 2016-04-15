package com.cg.mapreduce.fpgrowth.standalone;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
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

public class IncreFPGrowthAlgorithm {

	private static final String BASE_DIR = "C:/Users/cheng/Desktop/fp";

	private static String writePath;
	private static String old_hl;
	private static String new_hl;
	private static FileWriter fw;
	private static BufferedWriter bw;
	private static int minSuport;
	private static int dbMinSuport;
	private static int count;
	private static List<List<String>> DB;
	private static HashMap<List<String>, ItemsRecord> preResult;
	private static HashMap<List<String>, Integer> candidateSet = new HashMap<List<String>, Integer>();;

	public int getMinSuport() {
		return minSuport;
	}

	public void setMinSuport(int minSuport, int dbMinSuport) {
		this.minSuport = minSuport;
		this.dbMinSuport = dbMinSuport;
	}

	// �����ɸ��ļ��ж���Transaction Record
	public List<List<String>> readTransRocords(String... filenames) {
		List<List<String>> transaction = null;
		if (filenames.length > 0) {
			transaction = new LinkedList<List<String>>();
			for (String filename : filenames) {
				try {
					FileReader fr = new FileReader(filename);
					BufferedReader br = new BufferedReader(fr);
					try {
						String line;
						List<String> record;
						while ((line = br.readLine()) != null) {
							if (line.trim().length() > 0) {
								String str[] = line.split(" ");
								record = new LinkedList<String>();
								for (String w : str)
									record.add(w);
								transaction.add(record);
							}
						}
					} finally {
						br.close();
					}
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
		ArrayList<TreeNode> HeaderTable = buildHeaderTable(transRecords,
				postPattern);
		// ����FP-Tree
		TreeNode treeRoot = buildFPTree(transRecords, HeaderTable);
		// ���FP-TreeΪ���򷵻�
		if (treeRoot.getChildren() == null
				|| treeRoot.getChildren().size() == 0)
			return;

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
	public ArrayList<TreeNode> buildHeaderTable(
			List<List<String>> transRecords, List<String> postPattern) {
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

			// ��֧�ֶȴ��ڣ�����ڣ�minSup������뵽F1��
			if (postPattern != null) {
				Set<String> names = map.keySet();
				for (String name : names) {
					TreeNode tnode = map.get(name);

					if (isFrequentItems(tnode, postPattern)) {
						F1.add(tnode);
					}
				}
			} else {
				F1 = mergeHeaderTable(map);
			}
			Collections.sort(F1);

			return F1;
		} else {
			return null;
		}

	}

	private ArrayList<TreeNode> mergeHeaderTable(Map<String, TreeNode> map) {
		int sum;
		Set<String> names = map.keySet();
		ArrayList<TreeNode> FI1 = new ArrayList<TreeNode>();
		Map<String, TreeNode> F_1DB;
		try {
			FileInputStream fis = new FileInputStream(old_hl);
			ObjectInputStream ois = new ObjectInputStream(fis);
			try {
				F_1DB = (Map) ois.readObject();
				ois.close();
				for (String name : names) {
					TreeNode tnode = map.get(name);
					if (F_1DB.keySet().contains(name)) {
						sum = F_1DB.get(name).getCount() + tnode.getCount();
						F_1DB.get(name).setCount(sum);
					} else {
						F_1DB.put(name, tnode);
					}
				}
				// ���µ�ͷ����
				FileOutputStream fos0 = new FileOutputStream(new_hl);
				ObjectOutputStream oos0 = new ObjectOutputStream(fos0);
				oos0.writeObject(F_1DB);
				oos0.close();

				names = F_1DB.keySet();
				for (String name : names) {
					TreeNode tnode = F_1DB.get(name);
					if (tnode.getCount() >= minSuport) {
//						bw.append(tnode.getCount() + "\t" + tnode.getName());
//						bw.newLine();
						FI1.add(tnode);
					}
				}
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return FI1;
	}

	private boolean isFrequentItems(TreeNode tnode, List<String> postPattern) {
		List<String> items = new ArrayList<String>();
		// candidateSet = new HashMap<List<String>, Integer>();
		items.add(tnode.getName());
		for (String ele : postPattern) {
			items.add(ele);
		}
		Collections.sort(items);
		int total = tnode.getCount();
		if (preResult.containsKey(items)) {
			total += preResult.get(items).count;
			preResult.get(items).used();
			if (total >= minSuport) {
				tnode.setCount(total);
				// �����ͷ���ÿһ��+postPattern
				try {
					if (postPattern != null) {
						bw.append(tnode.getCount() + "\t" + tnode.getName());
						for (String ele : postPattern)
							bw.append("\t" + ele);
						bw.newLine();
						count++;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				// System.out.print(header.getCount() + "\t" +
				// header.getName());
				// if (postPattern != null) {
				// for (String ele : postPattern)
				// System.out.print("\t" + ele);
				// }
				// System.out.println();
				// count++;

				return true;
			}
		} else {
			if (total > dbMinSuport) {
				// for (List<String> record : transRecordsDB) {
				// if (record.containsAll(items)) {
				// total++;
				// }
				// }
				// if (total >= minSuport) {
				// tnode.setCount(total);
				// return true;
				// }
				candidateSet.put(items, total);
				return true;
			}
		}
		return false;
	}

	// ����FP-Tree
	public TreeNode buildFPTree(List<List<String>> transRecords,
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

	// ����1-�
	public Map<String, TreeNode> countItems(List<List<String>> transRecords) {
		ArrayList<TreeNode> F1 = null;
		Map<String, TreeNode> map = null;
		if (transRecords.size() > 0) {
			F1 = new ArrayList<TreeNode>();
			map = new HashMap<String, TreeNode>();
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
		}
		return map;
	}

	private HashMap<List<String>, ItemsRecord> readPreResult(String filename) {
		HashMap<List<String>, ItemsRecord> result = new HashMap<List<String>, ItemsRecord>();
		try {
			FileReader fr = new FileReader(filename);
			BufferedReader br = new BufferedReader(fr);
			try {
				String line;
				int i;
				List<String> record;
				while ((line = br.readLine()) != null) {
					if (line.trim().length() > 0) {
						String str[] = line.split("\t");
						record = new LinkedList<String>();
						for (i = 1; i < str.length; i++)
							record.add(str[i]);
						Collections.sort(record);
						result.put(
								record,
								new ItemsRecord(Integer.parseInt(str[0]), false));
					}
				}
			} finally {
				br.close();
			}
		} catch (IOException ex) {
			System.out.println("Read transaction records failed."
					+ ex.getMessage());
			System.exit(1);
		}
		return result;
	}

	private static void SearchDB(List<List<String>> transRecordsDB,
			HashMap<List<String>, Integer> candidateSet) {
		Set<List<String>> candidateItems = candidateSet.keySet();
		for (List<String> items : candidateItems) {
			for (List<String> record : transRecordsDB) {
				if (record.containsAll(items)) {
					candidateSet.put(items, candidateSet.get(items) + 1);
				}
			}
			if (candidateSet.get(items) >= minSuport) {
				try {
					
					bw.append(candidateSet.get(items)+"");
					for (String ele : items)
						bw.append("\t" + ele);
					bw.newLine();
					count++;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
	}

	private static void writeUnusedResult(HashMap<List<String>, ItemsRecord> map) {
		Set<List<String>> records = map.keySet();
		for (List<String> record : records) {
			ItemsRecord it = map.get(record);
			if (!it.isUsed && it.count >= minSuport) {
				if (record.size() > 1) {
					try {
						bw.append(it.count + "");
						for (String ele : record)
							bw.append("\t" + ele);
						bw.newLine();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					count++;
				}

			}
		}

	}

	private void init(List<List<String>> transRecords) throws Exception {
		// ��������
		int total = 2;
		// ���л���������
		FileOutputStream fos = new FileOutputStream(BASE_DIR
				+ "/records/data2.ser");
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(transRecords);

		// ����Ƶ�����·��
		writePath = BASE_DIR + "/result/fre_" + total;
		bw = new BufferedWriter(new FileWriter(writePath));

		// �ϴε�ͷ��
		old_hl = BASE_DIR + "/headlist/head_" + (total - 1);
		// �µ�ͷ��
		new_hl = BASE_DIR + "/headlist/head_" + total;

		// ��ȡ�ϴε�Ƶ����
		preResult = readPreResult(BASE_DIR + "/result/fre_" + (total - 1));
		oos.close();
	}

	private static List<List<String>> getDB() throws Exception {
		// ��ȡ֮ǰ��DB
		FileInputStream fis0 = new FileInputStream(BASE_DIR
				+ "/records/data1.ser");
		ObjectInputStream ois0 = new ObjectInputStream(fis0);
		DB = (List<List<String>>) ois0.readObject();
		ois0.close();
		return DB;
	}

	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		IncreFPGrowthAlgorithm fptree = new IncreFPGrowthAlgorithm();
		// ����֧�ֶ�
		fptree.setMinSuport(6, 2);
		// ��������·��
		List<List<String>> db = fptree
				.readTransRocords(BASE_DIR + "/data2.txt");
		fptree.init(db);
		// ��ʼ�ھ�
		fptree.FPGrowth(db, null);

		writeUnusedResult(preResult);
		
		DB = getDB();
		// ɨ���ѡ�
		SearchDB(DB, candidateSet);

		// �洢�µĵ�DB
		// FileOutputStream fos_DB = new FileOutputStream(
		// "G:/result/transRecords/DB");
		// ObjectOutputStream oos_DB = new ObjectOutputStream(fos0);
		// transRecordsDB.addAll(transRecords);
		// oos_DB.writeObject(transRecordsDB);
		// oos_DB.close();

		bw.close();

		System.out.println("��Ŀ����" + count);
		long endTime = System.currentTimeMillis();
		System.out.println("����ʱ�� " + (endTime - startTime) + "ms");
	}
}