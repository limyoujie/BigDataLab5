import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class StockCount{

    public static class StockCountMapper extends
	Mapper <Object, Text, Text, StockTuple>{
	    private Text date = new Text();
	    private StockTuple outTuple = new StockTuple();

	    public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException{
		    String line = value.toString();
		    String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
		    date.set(fields[1]);
		    float curPrice = Float.parseFloat(fields[2].trim());
		    float price50 = Float.parseFloat(fields[3].trim());
		    float price100 = Float.parseFloat(fields[5].trim());
		    float price200 = Float.parseFloat(fields[7].trim());
		    if (Float.compare(price50,curPrice)>0) {
			outTuple.setMa50(1);
		    }else{
			outTuple.setMa50(0);
			}
		    if (Float.compare(price100,curPrice)>0) {
			outTuple.setMa100(1);
		    }else{
			outTuple.setMa100(0);
			}
		    if (Float.compare(price200,curPrice)>0) {
			outTuple.setMa200(1);
		    }else{
			outTuple.setMa200(0);
			}
		    context.write(date,outTuple);
		}
	}

    public static class StockCountReducer extends
	Reducer<Text, StockTuple, Text, StockTuple>{
	    private StockTuple result=new StockTuple();
	    public void reduce (Text key, Iterable<StockTuple> values, Context context) throws IOException, InterruptedException {
		int ma50count=0;
		int ma100count=0;
		int ma200count=0;
		int totalcount=0;
		for (StockTuple val : values){
		    ma50count+=val.getMa50();	
		    ma100count+=val.getMa100();	
		    ma200count+=val.getMa200();	
		    totalcount+=val.getCount();
		}
		result.setMa50(ma50count);
		result.setMa100(ma100count);
		result.setMa200(ma200count);
		result.setCount(totalcount);
		context.write(key, result);
	    }
	}
    public static void main (String [] args) throws Exception {
	Configuration conf = new Configuration();
	Job job = new Job (conf, "StockCount");
	job.setJarByClass(StockCount.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(StockTuple.class);

	job.setMapperClass(StockCountMapper.class);
	job.setReducerClass(StockCountReducer.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.waitForCompletion(true);
    }
}


