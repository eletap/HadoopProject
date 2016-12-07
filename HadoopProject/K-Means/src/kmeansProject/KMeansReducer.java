package kmeansProject;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/**
 *KmeansReducer class
 *assign new center to each datapoint
 *@see reduce(Text old_centroid, Iterable<Text> values, Context context)
 *@see ~reduce(Text old_centroid, Iterable<Text> values, Context context)
 */
public class KMeansReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    /**
     *Calculate the new center for each data point.
     *Assign the center whose distance is the min from the datapoint.
     */
    protected void reduce(Text old_centroid, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Double mx = 0.0;
        Double my = 0.0;
        int counter = 0;

        for (Text value: values) {
            String[] temp = value.toString().split(";");
            if ((temp[0])!=null && temp[0].length()>0){
            	mx += Double.parseDouble(temp[0]);
            }

            if ((temp[1])!=null && temp[1].length()>0){
            	my += Double.parseDouble(temp[1]);
            }
            counter ++;
        }

        mx = mx/counter;
        my = my/counter;
        String new_centroid = mx + ";" + my;
        context.write(old_centroid, new Text(new_centroid));
    }

}
