package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Test用 Reducer 中身はMapperの要素をそのまま返すだけ
 */
public class AverageCalculationReducerTest extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {
	private DoubleWritable valueOut = new DoubleWritable();

	@Override
	protected void reduce(IntWritable keyIn, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {
		double sum=0;
		double element=0;
		double ave=0;
		for(IntWritable val : values){
			double vals = (double) val.get();
			sum=sum+vals;
			element++;
		}
		if(element==0){
			valueOut.set((double) ave);
			context.write(keyIn, valueOut);
			return;
		}
		ave=sum/element;
		valueOut.set((double) ave);
		context.write(keyIn, valueOut);

	}
}
