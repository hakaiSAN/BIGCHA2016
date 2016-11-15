package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StandardDeviationCalculationMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private static final IntWritable zero = new IntWritable(0);
	private static final IntWritable one  = new IntWritable(1);
	private IntWritable valueOut = new IntWritable();
	/** 後ろに偶数個の「"」が現れる「,」にマッチする正規表現 * * /

/** 最初と最後の「"」にマッチする正規表現*/
	static final String REGEX_SURROUND_DOUBLEQUATATION = "^\"|\"$";
	/** 「""」にマッチする正規表現 */
	static final String REGEX_DOUBLEQUOATATION = "\"\"";
	private static final String REGEX_CSV_COMMA = ",(?=(([^\"]*\"){2})*[^\"]*$)";
	@Override
	protected void map(LongWritable key, Text line, Context context)  throws IOException, InterruptedException {
		String part = context.getConfiguration().get("parts"); // configuration読み込み
		String brand = context.getConfiguration().get("brand");
		String[] values = (line.toString().split(",")); //formatを落とし込み
		//	System.out.println(values[x]); //for debug
		valueOut.set(Integer.parseInt(values[x]));

		if(isMatch(values[y], part)){
			if(isMatch(values[y], brand)){
				context.write(one, valueOut);
				context.write(zero, valueOut);
				return;
			}
			context.write(zero, valueOut);
		}
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



