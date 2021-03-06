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
        
public class TermFrequency 
{
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
	//private final static IntWritable one = new IntWritable(1);
	private Text outKey = new Text();
        
	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException 
	{   
	    String inputLine = value.toString();
	    String temp1[] = inputLine.split("\t"); 
	  	String keyf = temp1[0];
	    
	    String temp[] = temp1[1].split(","); //spliting input string to get pair of word,document name and frequency
	    float wordCntr = Float.parseFloat(temp[1]);
	    float docCntr= Float.parseFloat(temp[2]);
	    String word = temp[0];//getting word frequency
	    //seperating document name and word
	    //getting the document number or the document name
	    outKey.set(keyf);
	    float freq= wordCntr/docCntr;
	    String outval = word+","+freq;
	    context.write(outKey,new Text(outval));
	   
	    //String word = docPart[0];//getting the input word
	    //String tempStr=""; //temp string to construct the key part
	    
	    //loop is not required in this mapper as we know that the input string will only have 3 parts
	}
    } 
        
    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {

	public void reduce(Text key, Iterable<Text> values, Context context) 
	    throws IOException, InterruptedException
	    {
		
		ArrayList<Float> valuelist=new ArrayList<Float>();
    ArrayList<String> valuelist2 = new ArrayList<String>();
for (Text text : values) {
    String tem[] = text.toString().split(",");
    valuelist.add(Float.parseFloat(tem[1]));
    valuelist2.add(text.toString());
    }
    float maxi = Collections.max(valuelist);
for (String text : valuelist2) {
    String temp2[]= text.split(",");
    float norm = Float.parseFloat(temp2[1])/maxi;
    String outvalue = key.toString()+","+norm;
    context.write(new Text(temp2[0]), new Text(outvalue));
}
	//context.write(key, new Text(values.toString()+","+sum));
    }
    }
        
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
        
        Job job = new Job(conf, "TermFrequency");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	
	job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(Text.class);
  job.setJarByClass(TermFrequency.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
        
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
	job.waitForCompletion(true);
    }
        
}
