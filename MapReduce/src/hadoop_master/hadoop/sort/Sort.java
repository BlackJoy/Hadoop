package hadoop_master.hadoop.sort;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Sort {
	static class SortMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
		private LongWritable mapOutKey = new LongWritable();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException
		{
			// TODO Auto-generated method stub
			String lineValue = value.toString();
			mapOutKey.set(Long.parseLong(lineValue));
			context.write(mapOutKey, NullWritable.get());
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
	}
	static class SortReducer extends Reducer<LongWritable, NullWritable, IntWritable, LongWritable> {
		private IntWritable lineNum = new IntWritable(1);
		@Override
		protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException
		{
			for (NullWritable val : values)
			{
				context.write(lineNum, key);
				lineNum = new IntWritable(lineNum.get() + 1);
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
		}
	}
	public int run(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, Sort.class.getSimpleName());
		job.setJarByClass(Sort.class);
		Path inPutDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPutDir);
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(LongWritable.class);
		Path outPutDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPutDir);
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String args[]) throws Exception
	{
		args = new String[] { "hdfs://hadoop-master:9000/opt/data/input2",
				"hdfs://hadoop-master:9000/opt/data/output5" };
		int status = new Sort().run(args);
		System.exit(status);
	}
}
