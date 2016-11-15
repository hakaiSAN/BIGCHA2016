package jp.ac.nii;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.HashMap;
import java.util.AbstractMap;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;


/*
 * Reducer スペース，円, 表記ブレなどの処理
 * 配列のa, b, x, y等の位置は各自のデータに合わせて調整してください
 */


public class CleansingReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
  private HashMap<Integer, String> cachedData = new HashMap<Integer, String>();
  private int index;
//  public static int INDEX = 1; //Test
  private static int INDEX = ; //ブランド名の辞書のINDEX数
  private Text valueOut = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
	index = 0;
	super.setup(context);

	// 分散キャッシュファイルのPathを取得
	FileStatus stat;
	try {
		stat = DistributedCache.getFileStatus(context.getConfiguration(), new URI("yourpath/parts_brand.csv"));
	} catch (URISyntaxException e) {
		throw new IOException(e);
	}
	Path path = stat.getPath();

	FileSystem fs = path.getFileSystem(context.getConfiguration());
	FSDataInputStream is = fs.open(path);
	BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
	try {
		for (;;) {
			String s = br.readLine();
			if (s == null) {
				break;
			}
			cachedData.put(index, s);
			index++;
		}
	} finally {
		br.close();
	}
  }

	@Override
	protected void reduce(NullWritable keyIn, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
		int reduce_index = 0;
		StringBuilder buf = new StringBuilder();
		// format:カラムに合わせて調整
		for(Text val : values){
			String[] cols = val.toString().split(",");
			cols[a] = deleteSpace(cols[a]);
		
			//文字列置換　ブランド名．商品名表記統一
			for(reduce_index=0; reduce_index < INDEX; reduce_index++){
				String[] pbs = cachedData.get(reduce_index).split(",");
				String def = deleteSpace(pbs[0]);
				for(int i = 1; i < pbs.length; i++){ //スペースなし統一
					pbs[i] = deleteSpace(pbs[i]);
					cols[b] = replaceName(cols[b], pbs[i], def);
				}
			}
			
			
			// 価格名の表記ブレ処理
			cols[x] = deleteSpace(cols[x]);
			cols[x] = deleteYen(cols[x]); //開始価格の処理
			cols[y] = deleteSpace(cols[y]);
			cols[y] = deleteYen(cols[y]); //落札価格の処理

			buf.setLength(0);
			for(int i=0; i<cols.length; i++){ 
				buf.append(cols[i]);
				buf.append(","); //split用文字
			}

			String value = buf.toString();
			valueOut.set(value.substring(0, value.length()-1)); // 末端のカンマを削除
			context.write(NullWritable.get(), valueOut);
		}
	}

	private String deleteYen(String str){
		String result;
		result = replaceName(str,"¥", "");
		result = replaceName(result,"￥", "");
		result = replaceName(result,"円", ""); //価格の処理
		return result;
	}
	
	private String deleteSpace(String str){
		String result;
		result = replaceName(str, " ", "");
//		result = replaceName(result, &nbsp, "");
		result = replaceName(result, "　", ""); //スペースの削除 
		return result;
	}

	private String replaceName(String str, String delete, String replace){ 
		//strは対象文字列　deleteは削除したい文字列 replaceは置換文字列
	Pattern p = Pattern.compile(delete);

	Matcher m = p.matcher(str);
	String result = m.replaceAll(replace);
	return result;
	}
}