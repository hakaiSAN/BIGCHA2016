package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
//分散を計算して返す
public class StandardDeviationCalculationReducer extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {
	private DoubleWritable keyOut = new DoubleWritable();
	private DoubleWritable valueOut = new DoubleWritable();

	@Override
	protected void reduce(IntWritable keyIn, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
		double sum=0;
		double element=0;
		int key = (int) keyIn.get();
		double ave=Double.valueOf(context.getConfiguration().get(String.valueOf(key)));

		for(IntWritable val : values){
			double vals = (double) val.get();
			sum+=(ave-vals)*(ave-vals);
			element++;
		}
		if(element==0){
			valueOut.set((double) 0);
			context.write(keyIn, valueOut);
			return;
		}
		ave=Math.sqrt(sum/element);
		valueOut.set((double) ave);
		keyOut.set(key);
		context.write(keyIn, valueOut);
	}
}