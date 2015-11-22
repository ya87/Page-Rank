package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class PageCounterReducer extends MapReduceBase implements Reducer<LongWritable, NullWritable, Text, NullWritable> {

    public void reduce(LongWritable key, Iterator<NullWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {

        long count = 0;

        while(values.hasNext()){
            values.next();
            count++;
        }
        
        //<k,v> = <N=count, null>
        output.collect(new Text("N=" + Long.toString(count)), NullWritable.get());
    }
}