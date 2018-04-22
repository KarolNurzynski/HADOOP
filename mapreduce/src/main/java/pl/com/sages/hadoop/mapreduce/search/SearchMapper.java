package pl.com.sages.hadoop.mapreduce.search;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by knurzyns on 22.04.18.
 */
public class SearchMapper extends Mapper<Object,Text,Text,Text> {

    private static final String DELIMITERS = " \t\n\r\f,.:;![]()'*\"„”";

    private Text word = new Text();


    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String fileName = inputSplit.getPath().getName();

        StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().trim(), DELIMITERS);
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, new Text (fileName));
        }



    }
}
