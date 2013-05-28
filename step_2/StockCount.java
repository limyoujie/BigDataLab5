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
		    double curPrice = fields[2];
		    if (Double.parseDouble(fields[3])>curPrice) outTuple.setMa50(1);
		    context.write(airlineID,outTuple);
		}
	}

    public static class StockCountReducer extends
	Reducer<Text, StockCountTuple, Text, StockCountTuple>{
	    private StockCountTuple result=new StockCountTuple();
	    public void reduce (Text key, Iterable<StockCountTuple> values, Context context) throws IOException, InterruptedException {
		result.setMin(null);
		result.setMax(null);
		result.setCount(0);
		int sum=0;


