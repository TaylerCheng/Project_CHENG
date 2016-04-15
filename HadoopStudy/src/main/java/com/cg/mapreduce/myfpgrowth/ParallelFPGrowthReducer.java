/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cg.mapreduce.myfpgrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.list.LongArrayList;

import com.cg.mapreduce.myfpgrowth.fpgrowth.FpGrow;
import com.cg.mapreduce.myfpgrowth.fpgrowth.TreeNode;
import com.google.common.collect.Lists;
import com.ibm.icu.text.MessagePatternUtil.Node;

/**
 * takes each group of transactions and runs Vanilla FPGrowth on it and outputs
 * the the Top K frequent Patterns for each group.
 * 
 */

public final class ParallelFPGrowthReducer extends
		Reducer<IntWritable, ArrayList<String>, Text, LongWritable> {

	public static int minSupport;

	private final ArrayList<TreeNode> fList = new ArrayList<TreeNode>();
	private int maxPerGroup;

	@Override
	protected void reduce(IntWritable key, Iterable<ArrayList<String>> values,
			Context context) throws IOException {
		System.out.println("------Group ID : " + key);
		ArrayList<TreeNode> localFList = generateLocalList(key, fList);
		getFrequentItems(localFList, values);

	}

	private ArrayList<TreeNode> generateLocalList(IntWritable key,
			List<TreeNode> fList) {
		ArrayList<TreeNode> localList = new ArrayList<TreeNode>();
		int fListLen = fList.size();
		int gid = key.get();
		int startIndex = gid * maxPerGroup;
		int lastaIndex = (gid + 1) * maxPerGroup < fListLen ? (gid + 1)
				* maxPerGroup : fListLen;
		for (int i = startIndex; i < lastaIndex; i++) {
			localList.add(fList.get(i));
		}
		Collections.sort(localList);
		return localList;
	}

	private void getFrequentItems(ArrayList<TreeNode> localFList,
			Iterable<ArrayList<String>> values) {
		List<List<String>> trans = new LinkedList<List<String>>();
		for (ArrayList<String> record : values) {
			trans.add(record);
		}
		// ����FP-Tree
		TreeNode treeRoot = FpGrow.buildFPTree(trans, fList);
		// �ҵ���ͷ���ÿһ�������ģʽ��������ݹ����
		for (TreeNode header : localFList) {
			// ��׺ģʽ����һ��
			List<String> postPattern = new LinkedList<String>();
			postPattern.add(header.getName());
			// Ѱ��header������ģʽ��CPB������newTransRecords��
			List<List<String>> transRecords = new LinkedList<List<String>>();
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
					transRecords.add(prenodes);
				}
				backnode = backnode.getNextHomonym();
			}
			FpGrow.setMinSuport(minSupport);
			// �ݹ����
			FpGrow.fpgrowth(transRecords, postPattern);
		}

		// // ��ʼ������ͷ����ÿһ�������ģʽ��
		// HashMap<String, LinkedList<List<String>>> condiTransMap = new
		// HashMap<String, LinkedList<List<String>>>();
		// for (TreeNode node : localFList) {
		// condiTransMap.put(node.getName(), new LinkedList<List<String>>());
		// }
		//
		// // ��ÿ����¼���뵽��Ӧ������ģʽ����
		// for (List<String> record : values) {
		// System.out.println(record);
		// String postNode = record.get(record.size() - 1);
		// List<List<String>> conditionTrans = condiTransMap.get(postNode);
		// if (conditionTrans != null) {
		// record.remove(postNode);
		// conditionTrans.add(record);
		// } else {
		// System.out.println("-------------");
		// }
		// }
		//
		// // ����ÿ��ģʽ������FP-tree
		// Iterator iter = condiTransMap.entrySet().iterator();
		// while (iter.hasNext()) {
		// Map.Entry entry = (Map.Entry) iter.next();
		// List<String> postPattern = new LinkedList<String>();
		// postPattern.add((String) entry.getKey());
		// LinkedList<List<String>> conditionTrans = (LinkedList<List<String>>)
		// entry
		// .getValue();
		// FpGrow.FPGrowth(conditionTrans, postPattern);
		// }

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		super.setup(context);
		Parameters params = new Parameters(context.getConfiguration().get(
				PFPGrowth.PFP_PARAMETERS, ""));
		minSupport = Integer.valueOf(params.get(PFPGrowth.MIN_SUPPORT, "3"));
		maxPerGroup = params.getInt(PFPGrowth.MAX_PER_GROUP, 0);

		for (Pair<String, Long> e : readFList(context.getConfiguration())) {
			fList.add(new TreeNode(e.getFirst(), e.getSecond().intValue()));
		}
	}

	/**
	 * Generates the fList from the serialized string representation
	 * 
	 * @return Deserialized Feature Frequency List
	 */
	public List<Pair<String, Long>> readFList(Configuration conf)
			throws IOException {
		List<Pair<String, Long>> list = Lists.newArrayList();

		Path[] files = HadoopUtil.getCachedFiles(conf);
		if (files.length != 1) {
			throw new IOException(
					"Cannot read Frequency list from Distributed Cache ("
							+ files.length + ')');
		}

		for (Pair<Text, LongWritable> record : new SequenceFileIterable<Text, LongWritable>(
				files[0], true, conf)) {
			list.add(new Pair<String, Long>(record.getFirst().toString(),
					record.getSecond().get()));
		}
		return list;
	}

}
