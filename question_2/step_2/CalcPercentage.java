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

public class CalcPercentage {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
	private Text word = new Text();
	private final static DoubleWritable change= new DoubleWritable(0.0);
	public void map(LongWritable key, Text value, Context context)
	throws IOException, InterruptedException {
	    String line = value.toString();
	    String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	    word.set(fields[0]);
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    double today = Double.parseDouble(fields[2]);
	    double yesterday = Double.parseDouble(fields[3]);
	    double calcper = today/yesterday -1.0;
	    if(yesterday!=0) {
		change.set(calcper); 
	    }
	    Text changetext = new Text();
	    changetext.set(String.valueOf(change));
	    context.write(word, changetext);

	}
    } 

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
		int[] histogram = new int[22];
	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException {
		Text out_key = new Text();	
		Text out_value = new Text();	
		for (Text val : values) {
		    String checktext = val.toString();
		    float check = Float.parseFloat(checktext);
		    if (check < -1.0 ) histogram[0] = histogram[0] +1;
		    if (check >= -1.0 && check<-0.9) histogram[1] = histogram[1] +1;
		    if (check >= -0.9 && check<-0.8) histogram[2] = histogram[2] +1;
		    if (check >= -0.8 && check<-0.7) histogram[3] = histogram[3] +1;
		    if (check >= -0.7 && check<-0.6) histogram[4] = histogram[4] +1;
		    if (check >= -0.6 && check<-0.5) histogram[5] = histogram[5] +1;
		    if (check >= -0.5 && check<-0.4) histogram[6] = histogram[6] +1;
		    if (check >= -0.4 && check<-0.3) histogram[7] = histogram[7] +1;
		    if (check >= -0.3 && check<-0.2) histogram[8] = histogram[8] +1;
		    if (check >= -0.2 && check<-0.1) histogram[9] = histogram[9] +1;
		    if (check >= -0.1 && check<0.0) histogram[10] = histogram[10] +1;
		    if (check >= 0.0 && check<0.1) histogram[11] = histogram[11] +1;
		    if (check >= 0.1 && check<0.2) histogram[12] = histogram[12] +1;
		    if (check >= 0.2 && check<0.3) histogram[13] = histogram[13] +1;
		    if (check >= 0.3 && check<0.4) histogram[14] = histogram[14] +1;
		    if (check >= 0.4 && check<0.5) histogram[15] = histogram[15] +1;
		    if (check >= 0.5 && check<0.6) histogram[16] = histogram[16] +1;
		    if (check >= 0.6 && check<0.7) histogram[17] = histogram[17] +1;
		    if (check >= 0.7 && check<0.8) histogram[18] = histogram[18] +1;
		    if (check >= 0.8 && check<0.9) histogram[19] = histogram[19] +1;
		    if (check >= 0.9 && check<1.0) histogram[20] = histogram[20] +1;
		    if (check >= 1.0 ) histogram[21] = histogram[21] +1;
		}
	    out_key.set("Final Histogram");
		    out_value.set(Arrays.toString(histogram));
		    context.write(out_key,out_value);
	    }
    }

    public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();

	Job job = new Job(conf, "CalcPercentage");
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);




	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);



	job.setJarByClass(CalcPercentage.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);

	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));

	job.waitForCompletion(true);
    }

}
