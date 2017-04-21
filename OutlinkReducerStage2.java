package wikilinksandred;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class OutlinkReducerStage2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        StringBuilder resultStr = new StringBuilder();

            while(values.hasNext())
            {
                resultStr.append(values.next().toString() + "\t");
            }
            if (resultStr.length() > 0)
                resultStr.deleteCharAt(resultStr.length() - 1);
            if (!resultStr.toString().equals("NULL"))
                output.collect(key, new Text(resultStr.toString()));
            else
                output.collect(key, new Text("\t"));
    }
}
