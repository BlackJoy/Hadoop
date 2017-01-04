package hadoop_master.hadoop.delrep;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class DelRep {
	static class DelRepMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
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
			context.write(value, NullWritable.get());
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
	}
	static class DelRepReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		protected void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException
		{
			context.write(key, NullWritable.get());
		}
	}
	public int run(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, DelRep.class.getSimpleName());
		job.setJarByClass(DelRep.class);
		Path inPutDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPutDir);
		job.setMapperClass(DelRepMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setReducerClass(DelRepReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		Path outPutDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPutDir);
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String args[]) throws Exception
	{
		args = new String[] { "hdfs://hadoop-master:9000/opt/data/input1",
				"hdfs://hadoop-master:9000/opt/data/output4" };
		int status = new DelRep().run(args);
		System.exit(status);
	}
}
