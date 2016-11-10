package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/*
 * Mapper 今回は特に何もしていない　ただ流しているだけ
 * ホントは処理に必要な行だけ抜き出すべき
 */


public class CleansingMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
	@Override
	protected void map(LongWritable key, Text line, Context context)  throws IOException, InterruptedException {
				context.write(NullWritable.get(), line);
	}
}
