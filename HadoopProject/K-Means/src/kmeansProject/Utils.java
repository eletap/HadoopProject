package kmeansProject;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils class: contains general functions for the processing of the points and centers
 */
public class Utils {
	/**
	 *Read the centroids from the file and store them in a List. Called at the beginning of each map task.
	 */
	public static List<Double[]> readCentroids(String filename) throws IOException {

        FileInputStream fileInputStream = new FileInputStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
        String line;
        List<Double[]> centroids = new ArrayList<>();
        try {
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(";");
                Double[] centroid = new Double[2];
                for (int j=0; j<centroid.length; j++){
                	if (values[j]!=null && values[j].length()>0){
                    	centroid[j] = Double.parseDouble(values[j]);
                    }
                }

                centroids.add(centroid);
            }
        }
        finally {
            reader.close();
        }
        return centroids;

    }
		/**
		 *create a formatted string of the 3 centers
		 *@param centroids, a list (pair) of points
		 *@return a formatted string
		 */
    public static String getFormattedCentroids(List<Double[]> centroids) {

        StringBuilder centroidsBuilder = new StringBuilder();
        for (Double[] centroid : centroids) {
            centroidsBuilder.append(centroid[0].toString());
            centroidsBuilder.append(";");
            centroidsBuilder.append(centroid[1].toString());
            centroidsBuilder.append("\n");
        }

        return centroidsBuilder.toString();
    }

		/**
		 * write the neew centroids in the centroids file in the correct formattedCentroids
		 * using the formatted string @see getFormattedCentroids(List<Double[]> centroids)
		 * @see getFormattedCentroids(List<Double[]> centroids)
		 */
    public static void writeCentroids(Configuration configuration, String formattedCentroids) throws IOException {

        FileSystem fs = FileSystem.get(configuration);
        FSDataOutputStream fin = fs.create(new Path(KMeansDriver.CENTROIDS_FILE));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fin));
        bw.append(formattedCentroids);
        bw.close();
    }

    /**
		 *Write the final data in a file where each line represents one point of the original dataset and the center of the cluster it belongs to.
     */
		public static void writeFinalData(Configuration configuration, String centroids) throws IOException {

        FileSystem fs = FileSystem.get(configuration);

        FSDataOutputStream dataOutputStream = fs.create(new Path(configuration.get(KMeansDriver.OUTPUT_FILE_ARG) + "/final-data"));
        FSDataInputStream dataInputStream = new FSDataInputStream(fs.open(new Path(configuration.get(KMeansDriver.INPUT_FILE_ARG) + KMeansDriver.DATA_FILE_NAME)));
        /**
				 *the reader
				 */
        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
				/**
				 *the writer
				 */
				BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(dataOutputStream));

        try {

            String line;
            while ((line = reader.readLine()) != null) {

                String[] values = line.split(";");
                double x = Double.parseDouble(values[0]);
                double y = Double.parseDouble(values[1]);

                String REGEX="((\\d+.\\d+);(\\d+.\\d+))";
                Pattern p = Pattern.compile(REGEX);
                Matcher m = p.matcher(centroids);

    	        String centroid="";
                double minDistance = Double.MAX_VALUE;
                double cx=0.0;
                double cy=0.0;

                while (m.find()){
                	String point[] = (m.group(1)).split(";");
                	if (point[0]!=null){
                        cx = Double.parseDouble(point[0]);
                	}
                	if (point[1]!=null){
                        cy = Double.parseDouble(point[1]);
                	}

                    double distance = euclideanDistance(cx, cy, x, y);
                    if (distance < minDistance) {
                        minDistance = distance;
                        centroid= cx +", "+cy;
                    }

                }

                writer.write("Point: ("+x + ", " + y + ")				Cluster-Center: (" + centroid + ")\n");
            }
        }
        finally {

            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
        }
    }

    /**
		 *Create the first List of random centroids, by choosing existing points of the dataset.
		 */
    public static List<Double[]> createRandomCentroids(int centroidsNumber, Configuration configuration) throws IOException, NullPointerException{

        List<Double[]> centroids = new ArrayList<>();
        FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream dataInputStream = new FSDataInputStream(fs.open(new Path(configuration.get(KMeansDriver.INPUT_FILE_ARG) + KMeansDriver.DATA_FILE_NAME)));
        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
        ArrayList<Integer> line_nums = new ArrayList<Integer>(centroidsNumber);
        String line=reader.readLine();

        int counter_lines=0;
        int min=1;
        int max=KMeansDriver.DATASET_SIZE/KMeansDriver.NUM_OF_CLUSTERS;
        for (int j = 0; j < centroidsNumber; j++) {
        	Random rn = new Random();
        	int lnNum=0;
            do{
                lnNum=rn.nextInt((max-min)+1)+min;
            }while(line_nums.contains(lnNum));
            line_nums.add(lnNum);

            line=reader.readLine();
            while (line !=null && counter_lines != lnNum){
            	counter_lines++;
            	line=reader.readLine();
            }

        	String[] temp=line.split(";");
            Double[] centroid = new Double[2];
            for (int c=0; c<temp.length;c++){
            	if (temp[c]!=null && temp[c].length()>0){
                	centroid[c] = Double.parseDouble(temp[c]);
                }
            }
            min=max;
            max=max+(max/(j+1));
            centroids.add(centroid);
        }
        reader.close();
        return centroids;
    }


    public static double euclideanDistance(double x1, double y1, double x2, double y2) {
        return Math.sqrt(Math.pow(x1 - x2, 2) + Math.pow(y1 - y2, 2));
    }

    /**
		 *Check if the difference between the old and new centroids is less than 0.1.
		 */
    public static boolean compareReducerCentroids(Configuration configuration) throws IOException {
    	boolean convergence=false;
    	FileSystem fs = FileSystem.get(configuration);
        FSDataInputStream dataInputStream = new FSDataInputStream(fs.open(new Path(configuration.get(KMeansDriver.OUTPUT_FILE_ARG) + "/part-r-00000")));
        BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));

        String line;

        while((line = reader.readLine()) != null) {

            List<Double[]> centroids = new ArrayList<>();
            String REGEX="((\\d+.\\d+);(\\d+.\\d+))";
            Pattern p = Pattern.compile(REGEX);
            Matcher m = p.matcher(line);
            while (m.find()){
            	String point[] = (m.group(1)).split(";");
            	Double[] centroid = new Double[2];

                for (int j=0; j<centroid.length; j++){
                	if (point[j]!=null && point[j].length()>0){
                    	centroid[j] = Double.parseDouble(point[j]);
                    }
                }
            	centroids.add(centroid);
            }


            Double oldx = centroids.get(0)[0];
            Double oldy = centroids.get(0)[1];
            Double newx = centroids.get(1)[0];
            Double newy = centroids.get(1)[1];

            if (Math.abs(oldx-newx)<0.1 && Math.abs(oldy-newy)<0.1){
            	convergence=true;
            }else{
            	convergence=false;
            	break;
            }


        }
        reader.close();
        return convergence;

    }

    /**
		 *Returns a formatted string of the new centroids.
		 */
    public static String readReducerOutput(Configuration configuration) throws IOException {
    	 FileSystem fs = FileSystem.get(configuration);
         FSDataInputStream dataInputStream = new FSDataInputStream(fs.open(new Path(configuration.get(KMeansDriver.OUTPUT_FILE_ARG) + "/part-r-00000")));
         BufferedReader reader = new BufferedReader(new InputStreamReader(dataInputStream));
         StringBuilder content = new StringBuilder();
         String line;
         while((line = reader.readLine()) != null) {

             String REGEX="((\\d+.\\d+);(\\d+.\\d+))";
             Pattern p = Pattern.compile(REGEX);
             Matcher m = p.matcher(line);
             int counter=0;
             while (m.find()){
            	 counter++;
            	 if (counter>1){
            		 content.append(m.group(1));
            	 }
             }
         	content.append("\n");
         }
         reader.close();
         return content.toString();
    }
}
