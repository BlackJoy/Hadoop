package hadoop_master.hadoop.mr;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class MyWordCount {
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        private final static IntWritable  one =new IntWritable(1);
        private Text word =new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
              String lineValue = value.toString();
              StringTokenizer stringTokenizer =new StringTokenizer(lineValue);
              while(stringTokenizer.hasMoreTokens()){
            	  String wordValue =stringTokenizer.nextToken();
            	  word.set(wordValue);
            	  context.write(word, one);
              }
		}

	}
	static class MyRducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result =new IntWritable ();
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
		throws IOException, InterruptedException {
			int sum=0;
			for (IntWritable value :values){
				sum=sum+value.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}
	public static void main (String[] args)throws Exception{
		args=new String[]{
				"hdfs://hadoop-master:9000/opt/data/input",
				"hdfs://hadoop-master:9000/opt/data/outout1"
		};
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration,args).getRemainingArgs();
        if(otherArgs.length !=2){
        	System.err.println("Usage: wordcount <in> <out>");
        	System.exit(2);
        }
        Job job = new Job(configuration,"wc");
        job.setJarByClass(MyWordCount.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyRducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        boolean isSuccess =job.waitForCompletion(true);
        System.exit(isSuccess ? 0: 1);
	}

}
