package cloudproject.transcoding;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.core.CloudSim;

public class VideoStreams implements Callable<String>{
	
	private String command;
	private static volatile boolean volatile_flag = false;
	
	//New arrival queue size
	private final int NEW_ARRIVAL_QUEUE_SIZE = 4;
	
    //broker Id, every instance has its own userId
	private int userId;
	
	//video Id
	private int videoId;
	//The number of cloudlets are gonna be created, every instance has its own cloudlet number
	private int cloudlets;
	//Every instance has its own cloudletList
	private List<VideoSegment> cloudletList;
	//All the instance share cloudletNewArrivalQueue and cloudletBatchqueue, both of them are synchronized list
	private static List<VideoSegment> cloudletNewArrivalQueue = Collections.synchronizedList(new ArrayList<VideoSegment>());
	private static List<VideoSegment> cloudletBatchQueue = Collections.synchronizedList(new ArrayList<VideoSegment>());
	private static List<VideoSegment> cloudletNewList = new ArrayList<VideoSegment>();

	//private static ArrayList<Integer> newcloudlets = new ArrayList<Integer> ();

    private static int testCount = 0;
    private static int fileCount = 0;
    
	private String inputdataFolderURL;
	public boolean startupqueue;
	public int seedShift = 0;
	public String estimatedGopLength = null;

    
	
	public VideoStreams(){
		
	}
	
	public VideoStreams(String s, String inputdataFolderURL, boolean startupqueue, String estimatedGopLength, int seedShift, int userId, int videoId, int cloudlets){
	    this.command = s;
	    this.inputdataFolderURL = inputdataFolderURL;
	    this.startupqueue = startupqueue;
	    this.userId = userId;
	    this.videoId = videoId;
	    this.cloudlets = cloudlets;
	    this.seedShift = seedShift;
	    this.estimatedGopLength = estimatedGopLength;
	}

    public String call() {
    	// do stuff and return some String
         //currentTime = System.nanoTime()/(double)1000000;
    	
         //System.out.println(Thread.currentThread().getName()+" Start. Video Stream_"+ command);

         processCommand();
        // System.out.println(Thread.currentThread().getName()+" End.");         
            
         return Thread.currentThread().getName();
     }
    
     public boolean accept(File file) {
        return !file.isHidden();
     }
    
     private void processCommand() {
    	 //Read the files in the datafile folder	
		//File folder = new File("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudsim-impl/resources/inputdatafile"); 
		File folder = new File(inputdataFolderURL);

		File[] listOfFiles = folder.listFiles();
		
		/*Random randomSeed = new Random(seedShift);
		int shift = randomSeed.nextInt(1000)+1000;*/
		
		Random random = new Random(fileCount + seedShift*100);
		
		int fileIndex = random.nextInt(1000)%listOfFiles.length;
		
		if(fileIndex < listOfFiles.length){
			File file = listOfFiles[fileIndex];
			//if the file is hidden file, it won't be accepted and move to next one
			while(!accept(file)){
			  fileCount++;
			  fileIndex++;
			  file = listOfFiles[fileIndex];
			  
			}
			
		     
		    if (file.isFile() && file.getName().endsWith(".txt")) {
			   fileCount++;
		       cloudletList = createCloudlet(userId, videoId, file);
			   
		    }
		    	
			// cloudletList.clear();
			 System.out.println("\n**************************Video Stream_"+ command + " just arrived**************************\n"); 

			 System.out.println(Thread.currentThread().getName() +" Video Stream_"+ command + ": Created " + cloudletList.size() + " Cloudlets\n"); 
				// cloudletBatchQueue.addAll(cloudletList);
			 ArrayList<Integer> newcloudlets = new ArrayList<Integer> ();
         
		 
			 synchronized (this) {
				     if(startupqueue){
				    	 /**
		    		      * with new arrival queue
		    		      *   
		    		      */
		    		     if(cloudletList.size() > NEW_ARRIVAL_QUEUE_SIZE){
				    		 cloudletNewArrivalQueue.clear();

		    		    	 for(int i=0; i < NEW_ARRIVAL_QUEUE_SIZE; i++) {
				    			 cloudletNewArrivalQueue.add(cloudletList.get(i));
				    		 }
				    		 //The rest of the cloudlet list are sent to batch queue
				    		 for(int j=NEW_ARRIVAL_QUEUE_SIZE; j<cloudletList.size(); j++ ){
				    			 cloudletBatchQueue.add(cloudletList.get(j));
				    		 }
		    		     }else{
		    		    	 cloudletNewArrivalQueue.clear();
		    		    	 for(int i=0; i < cloudletList.size(); i++) {
				    			 cloudletNewArrivalQueue.add(cloudletList.get(i));
				    			
				    		 }
		    		    	 
		    		     }
				     }else{
				    	 /**
		    		      * without new arrival queue
		    		      *   
		    		      */
		    		  
		        		 for(int j=0; j<cloudletList.size(); j++ ){
			    			 cloudletBatchQueue.add(cloudletList.get(j));
			    		 }

				    	 
				    	 
				     }
				     
				    
			    	 System.out.println(Thread.currentThread().getName() + "*****New arrival queue Video ID_" + videoId + ": " + cloudletNewList + " **********");
				// }
			   }
			 
		 
		     /*System.out.println(Thread.currentThread().getName() +" Video Stream_"+ command + 
		    		 " Thread is going to sleep " + sleepTime + "ms."); 
		     Thread.sleep(sleepTime);*/
		 }
     }
     
     private List<VideoSegment> createCloudlet(int userId, int videoId, File file){
 		// Creates a container to store Cloudlets
 		LinkedList<VideoSegment> list = new LinkedList<VideoSegment>();
 		
 	  

        ParseData pd = new ParseData();
        pd.parseData(file);
        
      //cloudlet parameters
 		long length;
 		double deadline;
 		long fileSize = 0;
 		long outputSize = 0;
 		int cloudletNum = pd.getGopIdList().size(); 
 		//int cloudletNum = cloudlets;
 		int pesNumber = 1;
 		
 		long worstLength;
 		long bestLength;
 		long avglength;
 		int std;
 		UtilizationModel utilizationModel = new UtilizationModelFull();

 		VideoSegment[] cloudlet = new VideoSegment[cloudletNum];
 		
 		Random random = new Random();

 		for(int i=0;i<cloudletNum;i++){
 			
 			//length is the execution time mutiply by mips =10000, so it's 10 per ms
 			//It was runing on Mac whose Mips is 50000, covert it to cloud mips
 		//	length = pd.getGopTranscodingTimeList().get(i)*10000*(10000/10000);
 		    if(estimatedGopLength.equals("BEST")){
 			    length = (pd.getGopTranscodingTimeList().get(i) - pd.getStdList().get(i))*8000;

 		    }else if(estimatedGopLength.equalsIgnoreCase("AVERAGE")){
 			    length = pd.getGopTranscodingTimeList().get(i)*8000;

 		    }else {
 			    length = pd.getDeviationGopTranscodingTimeList().get(i)*8000;

 		    }
 			deadline = pd.getGopPtsList().get(i);
 			fileSize = pd.getGopInputSizeList().get(i);
 			outputSize = pd.getGopOutputSizeList().get(i);
 			
 			avglength = pd.getGopTranscodingTimeList().get(i)*8000;
 			std = pd.getStdList().get(i)*8000;
 			
 			/*double val = random.nextGaussian()*std + avglength;
 			length = (long) Math.round(val);*/
 			
 			
 			//length = getRandomNumber(1000, 10000, random);
 			//deadline = getRandomNumber(1, 100, random);
 			double arrivalTime = CloudSim.clock();//FIX HERE
 			//System.out.println(arrivalTime);
			double deadlineBeforePlay = Double.MAX_VALUE - cloudletNum + i;
			double deadlineAfterPlay = deadline;
			
			
 		    cloudlet[i] = new VideoSegment(i, videoId, length, avglength, std, arrivalTime, deadlineBeforePlay, deadlineAfterPlay, pesNumber, fileSize, outputSize, utilizationModel, utilizationModel, utilizationModel);
 			
 		    // setting the owner of these Cloudlets		
 		    cloudlet[i].setUserId(userId);
 			list.add(cloudlet[i]);
 			
 		}
        
 		volatile_flag = true;
 		
 		return list;
 		  
 	}
 	//Generate random number for random length cloudlets
 	
 	private static long getRandomNumber(int aStart, int aEnd, Random aRandom){
 	    if (aStart > aEnd) {
 	      throw new IllegalArgumentException("Start cannot exceed End.");
 	    }
 	    //get the range, casting to long to avoid overflow problems
 	    long range = (long)aEnd - (long)aStart + 1;
 	    // compute a fraction of the range, 0 <= frac < range
 	    long fraction = (long)(range * aRandom.nextDouble());
 	    long randomNumber =  (long)(fraction + aStart); 
 	    
 	    return randomNumber;
 	}
 	
 	public List<VideoSegment> getBatchQueue() {
 		return cloudletBatchQueue;
 	}
 	public List<VideoSegment> getNewArrivalList(){
 		return cloudletNewList;
 	}
 	
 	
 	public void setBatchQueue(List<VideoSegment> cloudletBatchQueue){
 		this.cloudletBatchQueue = cloudletBatchQueue;
 	}
 	
 	public List<VideoSegment> getNewArrivalQueue(){
 		return cloudletNewArrivalQueue;
 	}
 	
 	public void setNewArrivalQueue(List<VideoSegment> cloudletNewArrivalQueue){
 		this.cloudletNewArrivalQueue = cloudletNewArrivalQueue;
 	}

}

