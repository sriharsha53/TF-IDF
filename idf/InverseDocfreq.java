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
        
public class InverseDocfreq
{
    //public static int docnumbers=0;
    public static Set<Integer> set = new HashSet<Integer>();
    
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
	private final static IntWritable one = new IntWritable(1);
	private Text outKey = new Text();
        
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
	    String inputLine = value.toString(); //input is coming from the output file from tf
	    String temp[] = inputLine.split("\t"); //spliting input string to get pair of word,document name and frequency
	    context.write(new Text(temp[0]),new Text(temp[1]));
	    
	    //loop is not required in this mapper as we know that the input string will only have 3 parts
	}
    } 
        
    public static class Reduce extends Reducer<Text,Text, Text, Text>
    {

	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException
	    {
		int sum = 0;
		List<String> lis = new ArrayList<String>();		
		for (Text val : values) 
		{      lis.add(val.toString());
		       sum += 1;
		}
		 
		for (String valu: lis){
		String te[]=valu.split(",");
		float tf_idf = (float)(12/sum);
		double fin_tfidf =Float.parseFloat(te[1])*(Math.log(tf_idf)/Math.log(2));
		context.write(key, new Text(valu+",\t"+String.valueOf(fin_tfidf)));
	    }
	   
    }
    }
        
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
        
        Job job = new Job(conf, "InverseDocfreq");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	//job.setMapOutputKeyClass(Text.class);
	//job.setMapOutputValueClass(Text.class);
	job.setJarByClass(InverseDocfreq.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
        
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
	job.waitForCompletion(true);
    }
       
}
