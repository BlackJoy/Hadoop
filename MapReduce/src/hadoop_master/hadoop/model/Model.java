package hadoop_master.hadoop.model;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Model {
	static class TopKeyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text mapOutKey = new Text();
		private LongWritable mapOutValue = new LongWritable();
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException,
				InterruptedException
		{
			mapOutKey.set("");
			mapOutValue.set(1);
			context.write(mapOutKey, mapOutValue);
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			super.cleanup(context);
		}
	}
	static class TopKeyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException
		{
			super.setup(context);
		}
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException
		{
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
		Job job = new Job(conf, Model.class.getSimpleName());
		job.setJarByClass(Model.class);
		Path inPutDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPutDir);
		job.setMapperClass(TopKeyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(TopKeyReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		Path outPutDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPutDir);
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String args[]) throws Exception
	{
		args = new String[] { "hdfs://hadoop-master:9000/opt/data/input",
				"hdfs://hadoop-master:9000/opt/data/output3" };
		int status = new Model().run(args);
		System.exit(status);
	}
}
