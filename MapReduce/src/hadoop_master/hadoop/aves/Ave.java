package hadoop_master.hadoop.aves;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Ave {
	static class AveMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text mapOutKey = new Text();
		private IntWritable mapOutValue = new IntWritable();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException
		{
			String lineValue = value.toString();
			String strs[] = lineValue.split(" ");
			int tempValue = Integer.valueOf(strs[1]);
			mapOutKey.set(strs[0]);
			mapOutValue.set(tempValue);
			context.write(mapOutKey, mapOutValue);
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}
	}
	static class AveReducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
		}
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException
		{
			int sum=0,i=0;
			float ave=0;
			for (IntWritable value : values)
			{   
				i++;
				sum += value.get();
			}
			ave=sum/i;
			context.write(new Text(key), new FloatWritable(ave));
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}
	}
	public int run(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, Ave.class.getSimpleName());
		job.setJarByClass(Ave.class);
		Path inPutDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPutDir);
		job.setMapperClass(AveMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(AveReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		Path outPutDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPutDir);
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String args[]) throws Exception
	{
		args = new String[] { "hdfs://hadoop-master:9000/opt/data/input3",
				"hdfs://hadoop-master:9000/opt/data/output6" };
		int status = new Ave().run(args);
		System.exit(status);
	}
}
