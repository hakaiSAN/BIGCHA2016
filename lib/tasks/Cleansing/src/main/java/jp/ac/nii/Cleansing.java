package jp.ac.nii;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FileSystem;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
import java.util.Scanner;

/*
 * 表記ブレのあるcsvデータに対して
 * 辞書.txtおよび，スペース，円，カンマ等の統一を行うMapReduce
 */

public class Cleansing {

	public static void main(String[] args) throws Exception{
		if (args.length < 2) {
			System.out.println("error");
			return;
		}
		Configuration conf = new Configuration();
		Job initjob = Job.getInstance(conf, "Cleansing");
		initjob.setJarByClass(Cleansing.class);
		FileSystem fileSystem = FileSystem.get(conf);
    
		// 中間データのKeyとValueの型
		initjob.setMapOutputKeyClass(NullWritable.class);
		initjob.setMapOutputValueClass(Text.class);
		initjob.setOutputKeyClass(NullWritable.class);
		initjob.setOutputValueClass(Text.class);	

    // Mapper classとReducer classのセット
		initjob.setMapperClass(CleansingMapper.class);
		initjob.setReducerClass(CleansingReducer.class);
		
		initjob.setInputFormatClass(TextInputFormat.class);
		initjob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(initjob, new Path(args[0]));
		Path outpath = new Path(args[1]);
		FileOutputFormat.setOutputPath(initjob, outpath);

		DistributedCache.addCacheFile(new URI("yourpath/parts_brand.csv"), initjob.getConfiguration());	

		initjob.waitForCompletion(true);
		Path finalmergepath = new Path(args[2]);
		FileUtil.copyMerge(fileSystem, outpath, fileSystem, finalmergepath, false, conf, null);
		System.exit(0);
	}
}
