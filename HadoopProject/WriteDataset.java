import java.io.*;
import java.util.*;
/**
 *WriteDataset class. Detailed description:
 *Produce a dataset of 1 million points, all centered around 3 given points (dimensions given by the user).
 *@see getRandom(double bias)
 *@see ~getRandom(double bias)
 */
public class WriteDataset {

	   public static void main(String args[])throws IOException{

	      File file = new File("dataset.txt");
	      file.createNewFile();
	      FileWriter writer = new FileWriter(file);
	      Scanner input = new Scanner(System.in);
	      for (int i=0; i<3; i++){
	    	  System.out.print("Enter the x center for cluster "+ (i+1) + ": ");
		      double x = input.nextDouble();

		      System.out.print("Enter the y center for cluster "+ (i+1) + ": ");
		      double y = input.nextDouble();
		      for (int j=0; j<350000; j++){
			      writer.write(getRandom(x)+";"+ getRandom(y)+ "\n");
		      }
	      }

	      writer.flush();
	      writer.close();
	      input.close();

	   }

		 /**
		  *getRandom method. Detailed description:
		  *A method to get a biased random number. Close to the given centers.
			*min=bias-4, max=bias+4
			*@param bias is a double argument
			*@return the random value calculated by the formula
		  */
	   public static double getRandom(double bias){
		   double min = bias-4;
		   double max = bias+4;
		   double influence = 0.6;
		   Random r = new Random();
		   double rnd = r.nextDouble() * (max - min) + min;
		   double mix = r.nextDouble() * influence;
		   double value = rnd * (1 - mix) + bias * mix;
		   return value;
	   }

}
