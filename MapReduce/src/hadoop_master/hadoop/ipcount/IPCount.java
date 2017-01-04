package hadoop_master.hadoop.ipcount;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
public class IPCount {
	static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text mapOutKey = new Text();
		private final static IntWritable one = new IntWritable(1);
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
		}
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			String strs[] = lineValue.split(" ");
			mapOutKey.set(strs[0]);
			context.write(mapOutKey, one);
		}
	}

	static class MyRducer extends Reducer<Text, IntWritable, Text, FloatWritable> {
		private FloatWritable result = new FloatWritable();
		private MultipleOutputs<Text, FloatWritable> mos;  
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			mos = new MultipleOutputs<Text, FloatWritable>(context);  
			//super.setup(context);
		}		
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			float sum = 0;
			for (IntWritable value : values) {
				sum = sum + value.get();
			}
			result.set(sum);
			//context.write(key, result);
			mos.write(key,result,key.toString());
		}
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			mos.close(); 
		}

	}
	public int run(String args[]) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, IPCount.class.getSimpleName());
		job.setJarByClass(IPCount.class);
		Path inPutDir = new Path(args[0]);
		FileInputFormat.addInputPath(job, inPutDir);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setReducerClass(MyRducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		Path outPutDir = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outPutDir);
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	public static void main(String args[]) throws Exception
	{
		args = new String[] { "hdfs://hadoop-master:9000/opt/data/input5",
				"hdfs://hadoop-master:9000/opt/data/output8" };
		int status = new IPCount().run(args);
		System.exit(status);
	}
}

