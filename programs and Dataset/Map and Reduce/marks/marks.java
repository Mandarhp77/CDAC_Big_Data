import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;
import java.util.*;
import java.lang.*;

public class marks {
	
    public static class AgeMapper extends Mapper<Object,Text,Text,IntWritable>{
    
    boolean isNumber(String s){ 
         for(int i=0; i<s.length();i++){
         char ch = s.charAt(i);
         if( ch < '0' || ch > '9')
             return false;
       }
         return true;
       }
     
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
              
             String line = value.toString(); 
             StringTokenizer st = new StringTokenizer(line);
             
             while(st.hasMoreTokens()){
             
                String word = st.nextToken(",");
                if (isNumber(word)){
                
                context.write(new Text("age"),new IntWritable(Integer.parseInt(word)));
                
                }
             
             }
          }
       }
  
    
    public static class AgeReducer extends Reducer<Text,IntWritable,Text,FloatWritable>
    {
         public void reduce(Text key,
                            Iterable<IntWritable> values,
                            Context context) throws IOException,InterruptedException{ 
                               
             FloatWritable avg = new FloatWritable();
             int sum = 0,total=0;
                         
             for(IntWritable num : values){
                 if (num.get() >= 60) {
                 sum = sum+num.get();
                 total++;
                 }
             }
             
             avg.set(sum/total);
             context.write(new Text("Average is:"), avg);
         }
    }
    
    public static void main(String args[]) throws Exception{
    
    Configuration conf = new Configuration();
    
    Job job = new Job(conf,"Ages");
    
    job.setMapOutputKeyClass(Text.class);
    
    job.setMapOutputValueClass(IntWritable.class);
    
    job.setOutputKeyClass(Text.class);
    
    job.setOutputValueClass(FloatWritable.class);
    
    job.setOutputFormatClass(TextOutputFormat.class);
   
    job.setInputFormatClass(TextInputFormat.class);
    
    job.setMapperClass(AgeMapper.class);
    
    job.setReducerClass(AgeReducer.class);
    
    FileInputFormat.addInputPath(job,new Path(args[0]));
    
    FileOutputFormat.setOutputPath(job,new Path(args[1]));
    
    job.waitForCompletion(true);
    
    }
    
    }
