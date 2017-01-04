package hadoop_master.hadoop.topkey;
import java.io.IOException;
import java.util.TreeSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TopKey {
	static class TopKeyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		public static final int KEY = 3;
		private LongWritable mapOutValue = new LongWritable();
		private Text mapOutKey = new Text();
		TreeSet<TopWritable> topSet = new TreeSet<TopWritable>();
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
			String strs[] = lineValue.split("\t");
			long tempValue = Long.valueOf(strs[1]);
			mapOutKey.set(strs[0]);
			mapOutValue.set(tempValue);
			context.write(mapOutKey, mapOutValue);
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			super.cleanup(context);
		}
	}
	static class TopKeyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		private static final int KEY = 3;
		TreeSet<TopWritable> topSet = new TreeSet<TopWritable>();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException
		{
			long count = 0;
			for (LongWritable value : values)
			{
				count += value.get();
			}
			topSet.add(new TopWritable(key.toString(), count));
			if (topSet.size() > KEY)
			{
				topSet.remove(topSet.last());
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException
		{
			// TODO Auto-generated method stub
			for (TopWritable top : topSet)
			{
				context.write(new Text(top.getWord()), new LongWritable(top.getCount()));
			}
		}
	}
	public int run(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, TopKey.class.getSimpleName());
		job.setJarByClass(TopKey.class);
		Path inPutDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPutDir);
		job.setMapperClass(TopKeyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setReducerClass(TopKeyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		Path outPutDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPutDir);
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String args[]) throws Exception
	{
		
		int status = new TopKey().run(args);
		System.exit(status);
	}
}
