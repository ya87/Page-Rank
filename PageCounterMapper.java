package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class PageCounterMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, NullWritable> {

    public void map(LongWritable key, Text value, OutputCollector<LongWritable, NullWritable> output, Reporter reporter) throws IOException {

        final LongWritable one = new LongWritable(1);

        //<k,v> = <1, null>
        output.collect(one, NullWritable.get());
    }
}
