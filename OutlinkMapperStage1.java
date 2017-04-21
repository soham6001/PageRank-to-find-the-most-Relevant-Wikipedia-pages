package wikilinksandred;



import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;



public class OutlinkMapperStage1 extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private static final Pattern wikiLinksPattern = Pattern.compile("\\[.+?\\]");

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

    	StringTokenizer itr = new StringTokenizer(value.toString());
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilderRed = dbFactory.newDocumentBuilder();
            Document doc = dBuilderRed.parse(new ByteArrayInputStream(("<xmlns>"+value+"</xmlns>").getBytes()));
            NodeList nList = doc.getElementsByTagName("page");
            for (int temp = 0; temp < nList.getLength(); temp++) {
                Node nNode = nList.item(temp);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                   Element eElem = (Element) nNode;
                   String title = eElem.getElementsByTagName("title").item(0).getTextContent().trim().replaceAll(" ", "_");
                   String textString = eElem.getElementsByTagName("text").item(0).getTextContent();
              	 Pattern pattern = Pattern.compile("\\[\\[(.*?)(\\||\\]\\])");
              	 Matcher m = pattern.matcher(textString);
              	 String links="";
              	 Text textTitle = new Text(title);
              	 Text dummyText = new Text("#");
              	 output.collect(textTitle, dummyText);
              	 boolean flag=false;
              	 while (m.find()) {
              		 flag=true;
              	 String link = m.group(1).trim().replaceAll(" ", "_");
              	 Text linkText = new Text(link);
              	 output.collect(linkText,textTitle);
              	 }
              	 if(!flag){
              		output.collect(textTitle,new Text("NULL"));
              	 }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
