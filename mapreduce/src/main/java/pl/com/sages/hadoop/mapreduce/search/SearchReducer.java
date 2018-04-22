package pl.com.sages.hadoop.mapreduce.search;

import com.google.common.base.Joiner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by knurzyns on 22.04.18.
 */
public class SearchReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //Rozwiazanie wykladowcy

        Set<String> files = new HashSet<>();
        for (Text val:values) {
            files.add(val.toString());
        }

        String filesAsString = Joiner.on(", ").join(files);
        result.set(filesAsString);
        context.write(key,result);


//       ///// Moje rozwiazanie:
//
//        String files ="";
//        for (Text val : values) {
//            files+=val.toString()+", ";
//        }
//        result.set(files);
//        context.write(key, result);
    }
}
