package wikilinksandred;



import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class OutlinkReducerStage1 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        Set<String> set = new HashSet<String>();

        while ( values.hasNext()){
            set.add(values.next().toString());
        }

            if (set.contains("#")){
                output.collect(key,new Text(" "));
            	set.remove("#");

                	Iterator i = set.iterator();
                	boolean flag=true;
                    while ( i.hasNext()){
                    	flag=false;
                        String val = (String) i.next();
                        if ( !val.equals("NULL"))
                            output.collect(new Text(val), key);
                        else
                        	output.collect(key, new Text(" "));
                    }


            }
    }
}
