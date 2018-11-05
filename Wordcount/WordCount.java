import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import java.util.HashSet;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class WordCount {
    
    static enum UserCounter { PAIRS }
    
    public static class IntArrayWritable extends ArrayWritable
    {
        
        public IntArrayWritable(){
            super(IntWritable.class);
        }
        
        public IntArrayWritable(IntWritable[] values) {
            super(IntWritable.class, values);
        }
        
        public IntWritable[] get() {
            Writable[] temp = super.get();
            if (temp != null) {
                int n = temp.length;
                IntWritable[] items = new IntWritable[n];
                for (int i = 0; i < temp.length; i++) {
                    items[i] = (IntWritable)temp[i];
                }
                return items;
            } else {
                return null;
            }
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("[");
            String[] ss = super.toStrings();
            for(int x = 0; x < ss.length; x++){
                if (x == 0){
                    sb.append(ss[x]);
                }else{
                    sb.append(" ").append(ss[x]);
                }
            }
            sb.append("]");
            return sb.toString();
        }
    }
    
    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntArrayWritable>{
           
           private String sBuffer(Integer string){
               if(string < 10){
                   return "   ";
               }else if(string < 100){
                   return "  ";
               }else if(string < 1000){
                   return " ";
               }
               return "";
           }
           
           public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
               
               String firstLetter = "";
               ArrayList<String> friends = new ArrayList<String>();
               StringTokenizer iterator = new StringTokenizer(value.toString());
               int index = 0;
               
               while(iterator.hasMoreTokens()){
                   String token = iterator.nextToken();
                   if(index == 0){
                       firstLetter = token;
                   }else{
                       friends.add(token);
                   }
                   index++;
               }
               
               String friendString = String.join(" ", friends);
               IntWritable[] temp = new IntWritable[friends.size()];
               for(int x = 0; x < friends.size(); x++){
                   temp[x] = new IntWritable(Integer.parseInt(friends.get(x)));
               }
               IntArrayWritable output = new IntArrayWritable(temp);
               
               for(int x = 0; x < friends.size(); x++){
                   String friend = friends.get(x);
                   int starting = Integer.parseInt(firstLetter);
                   int ending = Integer.parseInt(friend);
                   String keyy = "";
                   if (starting > ending) {
                       keyy = ending + sBuffer(ending) + " " + starting + sBuffer(starting);
                   }else if (starting < ending) {
                       keyy = starting + sBuffer(starting) + " " + ending + sBuffer(ending);
                   }
                   
                   if (!keyy.equals("")){
                       context.write(new Text(keyy), output);
                   }
               }
            }
       }
    
    public static class Combiner extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {
        
        public void reduce(Text key, Iterable<IntArrayWritable> values, Context context ) throws IOException, InterruptedException {
            HashSet<Integer> h = new HashSet<Integer>();
            int index = 0;
            ArrayList<Integer> list = new ArrayList<Integer>();
            for (IntArrayWritable val : values) {
                IntWritable[] arr = val.get();
                if(index == 0){
                    for(int x = 0; x < arr.length; x++)
                    {
                        h.add(arr[x].get());
                    }
                }else{
                    for(int x = 0; x < arr.length; x++)
                    {
                        int num = arr[x].get();
                        if (h.contains(num))
                        {
                            list.add(num);
                        }
                    }
                }
                index++;
            }
            
            IntWritable[] temp = new IntWritable[list.size()];
            for(int x = 0; x < list.size(); x++){
                temp[x] = new IntWritable(list.get(x));
            }
            IntArrayWritable output = new IntArrayWritable(temp);
            
            if(list.size() != 0){
                context.getCounter(UserCounter.PAIRS).increment(1);
                context.write(key, output);
            }
            
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(Combiner.class);
    //job.setReducerClass(Combiner.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntArrayWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
