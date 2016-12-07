package kmeansProject;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;


public class KMeansMapper extends Mapper<Object, Text, Text, Text> {
	/**
   *The centroids file is read at the beginning of each map task by overriding the setup method.
   */
    public static List<Double[]> centroids;


  /**
   *Setup method
   *@param context is isEuclideanRythm
   *throws IOException, InterruptedException
   */
     @Override
    protected void setup(Context context) throws IOException, InterruptedException {
    	super.setup(context);
        URI[] cacheFiles = context.getCacheFiles();
        Path filepath = new Path(cacheFiles[0]);
        centroids = new ArrayList<>();
        centroids = Utils.readCentroids(filepath.toString());

    }

    @Override
    /**
     *map method
     *@param key is of type Object is the datapoint
     *@param value is the center closest to the datapoint
     *throws IOException, InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	    	double x=0.0;
	    	double y=0.0;
	    	double cx = 0.0;
	    	double cy = 0.0;
	        String[] xy = value.toString().split(";");
	        if ((xy[0])!=null && xy[0].length()>0){
	        	 x = Double.parseDouble(xy[0]);
	        }
	        if ((xy[1])!=null && xy[1].length()>0){
	        	y = Double.parseDouble(xy[1]);
	        }
	        String centroid="";
	        double minDistance = Double.MAX_VALUE;
	        if (!centroids.isEmpty() && centroids!=null){
		        for (int j = 0; j < centroids.size(); j++) {
		        	Double[] cxy = new Double[2];
		        	if (centroids.get(j)!=null){
		        		cxy=centroids.get(j);
		        		if (cxy.length>1 && cxy != null){

			        		if ((cxy[0])!=null){
			        			 cx=cxy[0];
			    	        }
			    	        if ((cxy[1])!=null){
				        		cy=cxy[1];
			    	        }
		        		}
			        	double distance = Utils.euclideanDistance(cx, cy, x, y);
 			            if (distance < minDistance) {
			                centroid = cx +";"+cy;
			                minDistance = distance;
			            }
		        	}
		        }
	        }

	        context.write(new Text(centroid), value);
    }


}
