package kmeansProject;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.List;

/**
 *The kMeansDriver class
 *The driver for the kmeans map reduce project.
 *@see launchJob(Configuration configuration)
 */
public class KMeansDriver{
	public static final String INPUT_FILE_ARG = "input_file";
	public static final String OUTPUT_FILE_ARG = "output_file";
	public static final int DATASET_SIZE = 1050000;
	public static final int NUM_OF_CLUSTERS = 3;
	public static String CENTROIDS_FILE = "centroids.txt";
	public static String DATA_FILE_NAME = "/dataset.txt";
	public static String JOB_NAME = "KMeans";

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: KMeans <in> <out>");
            System.exit(2);
        }


        configuration.set(INPUT_FILE_ARG, otherArgs[0]);
				/**
         * Creates random centroids for the first try of the K-Means algorithm.
				 */
        List<Double[]> centroids = Utils.createRandomCentroids(NUM_OF_CLUSTERS, configuration);
        String centroidsFile = Utils.getFormattedCentroids(centroids);
				/**
				 *Writes centroids on distributed cache (broadcasting).
				 */
        Utils.writeCentroids(configuration, centroidsFile);
        boolean hasConverged = false;

        int iteration = 0;

        do {

            configuration.set(OUTPUT_FILE_ARG, otherArgs[1] + "-" + iteration);

						/**
						 *Executes a hadoop job
						 */
            if (!launchJob(configuration)) {

                /**
								 * If an error has occurred stops iteration and terminates
								 */
                System.exit(1);
            }

            /**Check the difference between the new and the old centroids.
             *If it has not converged, write a new 'centroids.txt' file using the last reducer output and repeat the map-reduce job.
      			 */
						hasConverged=Utils.compareReducerCentroids(configuration);
           if(!hasConverged){
        	   String newCentroids = Utils.readReducerOutput(configuration);
               Utils.writeCentroids(configuration, newCentroids);
               centroidsFile = newCentroids;
           }


            iteration ++;

        } while (!hasConverged);

        /**
				 *Once we have computed the final centroids
				 */
       Utils.writeFinalData(configuration, centroidsFile);
    }
		/**
		 *The method to launch the map reduce job.
		 *@param configuration an argument of hadoop-t Configuration
		 */
    private static boolean launchJob(Configuration configuration) throws Exception {

        Job job = Job.getInstance(configuration);
        job.setJobName(JOB_NAME);
        job.setJarByClass(KMeansDriver.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        job.addCacheFile(new Path(CENTROIDS_FILE).toUri());

        FileInputFormat.addInputPath(job, new Path(configuration.get(INPUT_FILE_ARG)));
        FileOutputFormat.setOutputPath(job, new Path(configuration.get(OUTPUT_FILE_ARG)));

        return job.waitForCompletion(true);
    }



}
