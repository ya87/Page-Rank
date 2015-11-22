package PageRank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class PageRankCalculatorMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public static String special_char;

    public void configure(JobConf jobConf){
        special_char = jobConf.get("special_char");
    }
    
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        //line = title \t pageRank \t links
        String line = value.toString();
        String[] elements = line.split("\t");

        String title = elements[0];
        String rank = elements[1];

        String outLinks = null;
        int numOutlinks = 0;
        double pageVote = 0.0;

        //check for pages with no out-links
        if(elements.length > 2) {
            int titleEndIndex = line.indexOf("\t");
            int rankEndIndex = line.indexOf("\t", titleEndIndex + 1);

            outLinks = line.substring(rankEndIndex + 1);
            numOutlinks = elements.length - 2;
            pageVote = Double.parseDouble(rank) / numOutlinks;
        }

        //if page has outlinks then <k,v> = <page tile, special_char \t outlinks>
        //else <k,v> = <page tile, special_char>
        //special_char has been prefixed to outlinks to aid the reducer process
        if (outLinks == null) {
            //sink page
            output.collect(new Text(title), new Text(special_char));
        } else {
            output.collect(new Text(title), new Text(special_char + "\t" + outLinks));
        }

        //Output <k,v> = <outlink, pageVote>
        for(int i=2; i < elements.length; i++) {
            output.collect(new Text(elements[i]), new Text(Double.toString(pageVote)));
        }
    }
}