package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Paths;
import java.util.Scanner;


public class Avedev {

	public static void main(String[] args) throws Exception{
		if (args.length < 2) {
			System.out.println("hadoop jar Avedev-0.0.1-SNAPSHOT-jar-with-dependencies.jar jp.ac.nii.Avedev <input_hdfs_file> <output_hdfs_dir>");
			return;
		}
//		Configuration conf = createConfiguration("part-brand.txt");
		Configuration conf = createConfigurationpb((args[2]));
		Job averagejob = Job.getInstance(conf, "Average");
		averagejob.setJarByClass(Avedev.class);
    //
			// 中間データのKeyとValueの型
		averagejob.setMapOutputKeyClass(IntWritable.class);
		averagejob.setMapOutputValueClass(IntWritable.class);
		averagejob.setOutputKeyClass(DoubleWritable.class);
		averagejob.setOutputValueClass(DoubleWritable.class);	

    // Mapper classとReducer classのセット
		averagejob.setMapperClass(AverageCalculationMapper.class);
		averagejob.setReducerClass(AverageCalculationReducerTest.class);
		
		averagejob.setInputFormatClass(TextInputFormat.class);
		averagejob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(averagejob, new Path(args[0]));
		FileOutputFormat.setOutputPath(averagejob, new Path(args[1]));
		
		System.exit(averagejob.waitForCompletion(true) ? 0 : 1);
	}

//	private static Configuration createConfiguration(String entry) throws FileNotFoundException {
	private static Configuration createConfigurationpb(String arg) throws FileNotFoundException {
		Configuration conf = new Configuration();
		/*
		FileInputStream inputStream;
		Scanner scanner;
		try {
			inputStream = new FileInputStream(path);
			scanner = new Scanner(inputStream, "UTF-8");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		String keyValueparts = scanner.nextLine(); // 1行目は部位の情報が詰まってる（ex. Tシャツ,Tshirt）
		conf.set("parts", keyValueparts);
		String keyValuebrands = scanner.nextLine(); //2行目はブランドの情報（ex. NIKE,ナイキ）
		conf.set("brand", keyValuebrands);
		scanner.close();
		*/
		// 引数format ex.:Tシャツ,NIKE
		String[] values = arg.split(","); //0:部位 1:ブランド名
		conf.set("parts", values[0]);
		conf.set("brand", values[1]);
		return conf;
	}

}
