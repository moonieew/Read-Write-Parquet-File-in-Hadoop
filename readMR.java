import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleInputFormat;

public class ParquetFileRead extends Configured implements Tool{

  public static void main(String[] args)  throws Exception{
    int exitFlag = ToolRunner.run(new ParquetFileRead(), args);
    System.exit(exitFlag);
  }
  // Map function
  public static class ParquetMapper1 extends Mapper<LongWritable, Group, NullWritable, Text> {
    public static final Log log = LogFactory.getLog(ParquetMapper1.class);
    public void map(LongWritable key, Group value, Context context) 
        throws IOException, InterruptedException {
      NullWritable outKey = NullWritable.get();
      String line = value.toString();
      String[] fields = line.split("\n");
      String[] record = fields[1].split(": ");
      context.write(outKey, new Text(record[1]));           
    }		
  }
	
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "parquet1");
    job.setJarByClass(getClass());
    job.setMapperClass(ParquetMapper1.class);    
    job.setNumReduceTasks(0);
    
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
  
    job.setInputFormatClass(ExampleInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
}