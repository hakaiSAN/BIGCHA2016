package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class AverageCalculationMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
  private static final IntWritable zero = new IntWritable(0);
  private static final IntWritable one  = new IntWritable(1);
  private IntWritable valueOut = new IntWritable();

	@Override
	protected void map(LongWritable key, Text line, Context context)  throws IOException, InterruptedException {
		// format: "サイト名","商品タイトル","落札価格","終了日","入札数","サムネイル画像URL","商品ＩＤ","実サイト商品ペ ージのURL","詳細ページのURL","開始価格","出品者ID"
//		String[] parts = context.getConfiguration().get("parts").split(","); // configuration読み込み
//		String[] brands = context.getConfiguration().get("brand").split(",");

		String part = context.getConfiguration().get("parts"); // configuration読み込み
		String brand = context.getConfiguration().get("brand");
		String[] values = line.toString().split(","); //formatを落とし込み
		values[2] = values[2].substring(1, values[2].length()-1); //ダブルクオーテーション削除
		valueOut.set(Integer.parseInt(values[2]));

		if(isMatch(values[1], part)){
			if(isMatch(values[1], brand)){
				context.write(one, valueOut);
				return;
			}
		context.write(zero, valueOut);
		}
		/*
		for(String part : parts){
			if(isMatch(values[1], part)){
				for(String brand : brands) {
					if(isMatch(values[1], brand)){
						System.out.println(values[2]);
						context.write(one, valueOut);
						return;
					}
				}
				context.write(zero, valueOut);
			}
		}
		*/
	}
	public boolean isMatch(String str1, String str2) {
	    if(str1.matches(".*" + str2 + ".*")) {
	        return true;
	    }
	    else {
	        return false;
	    }
	}
}
