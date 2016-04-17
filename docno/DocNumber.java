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
        
public class DocNumber 
{
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
	//private final static IntWritable one = new IntWritable(1);
	private Text outKey = new Text();
        
	public void map(LongWritable key,Text value, Context context) throws IOException, InterruptedException 
	{
	    String inputLine = value.toString();
	    String temp[] = inputLine.split("\t"); //spliting input string to get pair of word,document name and frequency
	    
	    String docPart[]=temp[0].split(",");//seperating document name and word
	    String docName = docPart[1]; //getting the document number or the document name
	    outKey.set("docnumber");
	    context.write(outKey,new Text(docName));
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
	
		Set<String> valuelist=new HashSet<String>();
    
for (Text text : values) {
    valuelist.add(text.toString());
   }
    String docsize = "" +valuelist.size();
context.write(new Text("No. of docs"), new Text(docsize));
    }
    }
        
    public static void main(String[] args) throws Exception
    {
	Configuration conf = new Configuration();
        
        Job job = new Job(conf, "DocNumber");
    
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setJarByClass(DocNumber.class);

	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
        
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);
        
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
	job.waitForCompletion(true);
    }
        
}
