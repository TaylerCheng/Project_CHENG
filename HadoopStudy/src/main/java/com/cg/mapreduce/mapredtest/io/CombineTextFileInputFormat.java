package com.cg.mapreduce.mapredtest.io;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class CombineTextFileInputFormat extends CombineFileInputFormat {

	@Override
	public RecordReader createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		CombineFileSplit combineFileSplit = (CombineFileSplit) split;
		CombineFileRecordReader recordReader = new CombineFileRecordReader(
				combineFileSplit, context, CombineSmallfileRecordReader.class);
		try {
			recordReader.initialize(combineFileSplit, context);
		} catch (InterruptedException e) {
			new RuntimeException(
					"Error to initialize CombineSmallfileRecordReader.");
		}
		return recordReader;
	}

}
