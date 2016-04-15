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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.Parameters;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenObjectIntHashMap;
import org.apache.mahout.math.set.OpenIntHashSet;

import com.cg.mapreduce.myfpgrowth.fpgrowth.FpGrow;
import com.cg.mapreduce.myfpgrowth.fpgrowth.TreeNode;
import com.google.common.collect.Lists;

/**
 * maps each transaction to all unique items groups in the transaction. mapper
 * outputs the group id as key and the transaction as value
 * 
 */
public class ParallelFPGrowthMapper extends
		Mapper<LongWritable, Text, IntWritable, ArrayList<String>> {

	private final OpenObjectIntHashMap<String> fMap = new OpenObjectIntHashMap<String>();
	private final ArrayList<TreeNode> fList = new ArrayList<TreeNode>();
	private Pattern splitter;
	private int maxPerGroup;
	private final IntWritable wGroupID = new IntWritable();

	@Override
	protected void map(LongWritable offset, Text input, Context context)
			throws IOException, InterruptedException {

		String[] items = splitter.split(input.toString());
		List<String> record = FpGrow.sortByF1(Arrays.asList(items), fList);
		OpenIntHashSet groups = new OpenIntHashSet();
		for (int j = record.size() - 1; j >= 0; j--) {
			// generate group dependent shards
			String item = record.get(j);
			int groupID = fMap.get(item) / maxPerGroup;
			if (!groups.contains(groupID)) {
				ArrayList<String> tempItems = Lists.newArrayList();
				for (int i = 0; i <= j; i++) {
					tempItems.add(record.get(i));
				}
				wGroupID.set(groupID);
				if (tempItems.size() > 1) {
					// System.out.println(groupID+"   "+tempItems);
					context.write(wGroupID, tempItems);
				}
			}
			groups.add(groupID);
		}

	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);

		int i = 0;
		for (Pair<String, Long> e : readFList(context.getConfiguration())) {
			fList.add(new TreeNode(e.getFirst(), e.getSecond().intValue()));
			fMap.put(e.getFirst(), i++);
		}

		Collections.sort(fList);

		Parameters params = new Parameters(context.getConfiguration().get(
				PFPGrowth.PFP_PARAMETERS, ""));

		splitter = Pattern.compile(params.get(PFPGrowth.SPLIT_PATTERN,
				PFPGrowth.SPLITTER.toString()));

		maxPerGroup = params.getInt(PFPGrowth.MAX_PER_GROUP, 0);
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
