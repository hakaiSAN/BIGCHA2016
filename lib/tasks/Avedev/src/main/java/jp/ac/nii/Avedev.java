package jp.ac.nii;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Avedev {

	public static void main(String[] args) throws Exception{
		if (args.length < 2) {
			System.out.println("Please read Readme.");
			return;
		}
		//		Configuration conf = createConfiguration("part-brand.txt");

		Configuration conf = createConfigurationpb((args[2]));
		Job averagejob = Job.getInstance(conf, "Average");
		averagejob.setJarByClass(Avedev.class);


		averagejob.setMapOutputKeyClass(IntWritable.class);
		averagejob.setMapOutputValueClass(IntWritable.class);
		averagejob.setOutputKeyClass(IntWritable.class);
		averagejob.setOutputValueClass(DoubleWritable.class);

		// Mapper classとReducer classのセット
		averagejob.setMapperClass(AverageCalculationMapper.class);
		averagejob.setReducerClass(AverageCalculationReducerTest.class);

		averagejob.setInputFormatClass(TextInputFormat.class);
		averagejob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(averagejob, new Path(args[0]));
		FileOutputFormat.setOutputPath(averagejob, new Path(args[1]));
		averagejob.waitForCompletion(true);
		Runtime r = Runtime.getRuntime();
		try {
			Process	process = r.exec("hdfs dfs -get "+args[1]+"/part-r-00000 part-r-000001");
			process.waitFor();
		} catch (IOException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}

		// 中間データのKeyとValueの
		conf = createConfigurationpb2((args[2]));

		Job sbjob = Job.getInstance(conf);
		sbjob.setJarByClass(Avedev.class);
		sbjob.setMapOutputKeyClass(IntWritable.class);
		sbjob.setMapOutputValueClass(IntWritable.class);
		sbjob.setOutputKeyClass(IntWritable.class);
		sbjob.setOutputValueClass(DoubleWritable.class);

		// Mapper classとReducer classのセット
		sbjob.setMapperClass(StandardDeviationCalculationMapper.class);
		sbjob.setReducerClass(StandardDeviationCalculationReducer.class);

		sbjob.setInputFormatClass(TextInputFormat.class);
		sbjob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(sbjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(sbjob, new Path(args[1]+"2"));
		r = Runtime.getRuntime();

		sbjob.waitForCompletion(true);
		try {
			Process process = r.exec("hdfs dfs -get "+args[1]+"2/part-r-00000 part-r-000002");
			process.waitFor();
			//			process = r.exec("hdfs dfs -rm  -r hadoop/output2");
		} catch (IOException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}
		String under=args[2].replace(",","_").replace(" ", "");
		convcsv("part-r-000001","average",under);
		convcsv("part-r-000002","deviation",under);
		Process process = r.exec("rm -f part-r-000001");
		process = r.exec("rm -f part-r-000002");
		process = r.exec("hdfs dfs -rm -r "+args[1]);
		process = r.exec("hdfs dfs -rm -r "+args[1]+"2");
		process = r.exec("hdfs dfs -get -mkdir test");
		process.waitFor();
	}
	private static void convcsv(String before,String after,String part){
		Scanner scanner;
		FileInputStream inputStream;
		try {

			inputStream = new FileInputStream(before);
			scanner = new Scanner(inputStream, "UTF-8");
			String keyValue1 = scanner.nextLine().replace("	",",");
			String keyValue2 = scanner.nextLine().replace("	",",");
			System.out.println(keyValue1);
			File file = new File((part.replace(",", "_").replace(" ", ""))+"_"+after+".csv");
			FileWriter filewriter = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(filewriter);
			PrintWriter pw = new PrintWriter(bw);
			pw.println(keyValue1);
			pw.println(keyValue2);
			scanner.close();
			pw.close();
		} catch (FileNotFoundException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		} catch (IOException e) {
			// TODO 自動生成された catch ブロック
			e.printStackTrace();
		}


	}

	//	private static Configuration createConfiguration(String entry) throws FileNotFoundException {
	private static Configuration createConfigurationpb(String arg) throws FileNotFoundException {
		Configuration conf = new Configuration();

		String[] values = arg.split(",");
		conf.set("part", values[0]);
		conf.set("brand", values[1]);
		return conf;
	}
	private static Configuration createConfigurationpb2(String arg) throws FileNotFoundException {
		FileInputStream inputStream;
		Scanner scanner;
		Configuration conf = new Configuration();




		inputStream = new FileInputStream("part-r-000001");
		scanner = new Scanner(inputStream, "UTF-8");
		String keyValue[] = scanner.nextLine().split("	");


		conf.set(keyValue[0], keyValue[1]);
		if(!keyValue[1].equals("0.0")){
			keyValue = scanner.nextLine().split("	");
			conf.set(keyValue[0], keyValue[1]);
		}
		scanner.close();


		String[] values = arg.split(",");
		conf.set("part", values[0]);
		conf.set("brand", values[1]);

		return conf;
	}

}
