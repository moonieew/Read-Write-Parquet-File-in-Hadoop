import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

public class ParquetFile extends Configured implements Tool{
  public static void main(String[] args)  throws Exception{	
    int exitFlag = ToolRunner.run(new ParquetFile(), args);
    System.exit(exitFlag);
  }
  /// Schema
  private	static final Schema AVRO_SCHEMA = new	Schema.Parser().parse(
    "{\n" +
    "	\"type\":	\"record\",\n" +				
    "	\"name\":	\"testFile\",\n" +
    "	\"doc\":	\"test records\",\n" +
    "	\"fields\":\n" + 
    "	[\n" + 
    "			{\"name\": \"byteofffset\",	\"type\":	\"long\"},\n"+ 
    "			{\"name\":	\"line\", \"type\":	\"string\"}\n"+
    "	]\n"+
    "}\n");
	
  // Map function
  public static class ParquetMapper extends Mapper<LongWritable, Text, Void, GenericRecord> {
    
    private	GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
    public void map(LongWritable key, Text value, Context context) 
        throws IOException, InterruptedException {
      record.put("byteofffset", key.get());
      record.put("line", value.toString());
      context.write(null, record); 
    }		
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "parquet");
    job.setJarByClass(ParquetFile.class);
    job.setMapperClass(ParquetMapper.class);    
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Void.class);
    job.setOutputValueClass(Group.class);
    job.setOutputFormatClass(AvroParquetOutputFormat.class);
    // setting schema to be used
    AvroParquetOutputFormat.setSchema(job, AVRO_SCHEMA);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }
}