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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cg.mapreduce.myfpgrowth.fpgrowth.FpGrow;

@Deprecated
public final class FPGrowthDriver extends AbstractJob {

	private static final Logger log = LoggerFactory
			.getLogger(FPGrowthDriver.class);

	private FPGrowthDriver() {
	}

	public static void main(String[] args) throws Exception {
//		long startTime = System.currentTimeMillis();
		ToolRunner.run(new Configuration(), new FPGrowthDriver(), args);
//
//		long endTime = System.currentTimeMillis();
//		System.out.println("共用时： " + (endTime - startTime) + "ms");
		System.out.println(FpGrow.count);
	}

	/**
	 * Run TopK FPGrowth given the input file,
	 */
	@Override
	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();

		addOption("minSupport", "s",
				"(Optional) The minimum number of times a co-occurrence must be present."
						+ " Default Value: 3", "3");
		addOption(
				"maxHeapSize",
				"k",
				"(Optional) Maximum Heap Size k, to denote the requirement to mine top K items."
						+ " Default value: 50", "50");
		addOption(
				"numGroups",
				"g",
				"(Optional) Number of groups the features should be divided in the map-reduce version."
						+ " Doesn't work in sequential version Default Value:"
						+ PFPGrowth.NUM_GROUPS_DEFAULT,
				Integer.toString(PFPGrowth.NUM_GROUPS_DEFAULT));
		addOption(
				"splitterPattern",
				"regex",
				"Regular Expression pattern used to split given string transaction into"
						+ " itemsets. Default value splits comma separated itemsets.  Default Value:"
						+ " \"[ ,\\t]*[,|\\t][ ,\\t]*\" ",
				"[ ,\t]*[,|\t][ ,\t]*");
		addOption(
				"numTreeCacheEntries",
				"tc",
				"(Optional) Number of entries in the tree cache to prevent duplicate"
						+ " tree building. (Warning) a first level conditional FP-Tree might consume a lot of memory, "
						+ "so keep this value small, but big enough to prevent duplicate tree building. "
						+ "Default Value:5 Recommended Values: [5-10]", "5");
		addOption("method", "method",
				"Method of processing: sequential|mapreduce", "sequential");
		addOption("encoding", "e",
				"(Optional) The file encoding.  Default value: UTF-8", "UTF-8");
		addFlag("useFPG2", "2", "Use an alternate FPG implementation");

		if (parseArguments(args) == null) {
			return -1;
		}

		Parameters params = new Parameters();

		if (hasOption("minSupport")) {
			String minSupportString = getOption("minSupport");
			params.set("minSupport", minSupportString);
		}
		if (hasOption("maxHeapSize")) {
			String maxHeapSizeString = getOption("maxHeapSize");
			params.set("maxHeapSize", maxHeapSizeString);
		}
		if (hasOption("numGroups")) {
			String numGroupsString = getOption("numGroups");
			params.set("numGroups", numGroupsString);
		}

		if (hasOption("numTreeCacheEntries")) {
			String numTreeCacheString = getOption("numTreeCacheEntries");
			params.set("treeCacheSize", numTreeCacheString);
		}

		if (hasOption("splitterPattern")) {
			String patternString = getOption("splitterPattern");
			params.set("splitPattern", patternString);
		}

		String encoding = "UTF-8";
		if (hasOption("encoding")) {
			encoding = getOption("encoding");
		}
		params.set("encoding", encoding);

		if (hasOption("useFPG2")) {
			params.set(PFPGrowth.USE_FPG2, "true");
		}

		Path inputDir = getInputPath();
		Path outputDir = getOutputPath();

		params.set("input", inputDir.toString());
		params.set("output", outputDir.toString());

		Configuration conf = new Configuration();
		HadoopUtil.delete(conf, outputDir);
		PFPGrowth.runPFPGrowth(params);

		return 0;
	}

}
