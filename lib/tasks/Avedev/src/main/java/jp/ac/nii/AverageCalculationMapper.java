package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageCalculationMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
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

		String part = context.getConfiguration().get("1");
		String brand = context.getConfiguration().get("2");
		String[] values = (line.toString().split(","));


		if(isMatch(values[a], part)){
			if(isMatch(values[a], brand)){
				valueOut.set(Integer.parseInt(values[x]));
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



