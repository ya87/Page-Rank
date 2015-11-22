package PageRank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PageRankCalculatorReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    private static final double D = 0.85;
	public static String count;
    public static String special_char;

    public void configure(JobConf jobConf){
        count  = jobConf.get("count");
        special_char = jobConf.get("special_char");
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        int n = Integer.parseInt(count);
        String title = key.toString();
        
        double rank = 0.0;
        double votes = 0.0;
        String outlinks = "";
        boolean pageWithoutOutlinks = false;

        while(values.hasNext()) {
            String str = values.next().toString();

            if(str.indexOf("\t") != -1) {
                String[] outLink = str.split("\t");

                if(outLink[0].equals(special_char)) {
                    for(int i= 1; i < outLink.length; i++){
                        outlinks += "\t" + outLink[i];
                    }
                }
            } else {
                if(str.equals(special_char)) {
                    pageWithoutOutlinks = true;
                } else {
                    votes += Double.parseDouble(str);
                }
            }
        }

        rank = ((1 - D) / n) + D * votes;

        //if sink page then <k,v> = <title, pageRank>
        //else <k,v> = <title, pageRank \t outlinks>
        if (pageWithoutOutlinks) {
        	output.collect(new Text(title), new Text(Double.toString(rank)));
        } else { //else print the title, rand and outlinks
        	output.collect(new Text(title), new Text(Double.toString(rank) + outlinks));
        }
    }
}