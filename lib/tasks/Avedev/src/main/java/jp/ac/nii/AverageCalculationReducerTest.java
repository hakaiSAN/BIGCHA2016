package jp.ac.nii;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Test用 Reducer 中身はMapperの要素をそのまま返すだけ
 */
public class AverageCalculationReducerTest extends Reducer<IntWritable, IntWritable, DoubleWritable, DoubleWritable> {
  private DoubleWritable keyOut = new DoubleWritable();
  private DoubleWritable valueOut = new DoubleWritable();
  private int cast_tmp; // cast用tmp

	@Override
	protected void reduce(IntWritable keyIn, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException {

    Double key = (double) keyIn.get();
    for(IntWritable val : values){
    	cast_tmp = val.get();
    	keyOut.set(key);
    	valueOut.set((double) cast_tmp);
		context.write(keyOut, valueOut);
		}
	}
}
