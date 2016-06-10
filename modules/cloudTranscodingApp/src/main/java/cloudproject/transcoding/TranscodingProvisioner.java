package cloudproject.transcoding;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.lists.VmList;

import cloudproject.transcoding.TranscodingBroker;

public class TranscodingProvisioner {
	
	private double deadlineMissRate;
	private static double previousDeadlineMissRate;
	private double deadlineMissRateVarance = 0.0;
	private double arrivalRate;
	private double transcodingRate;
	
	private int frequency = 0;
	private double DEADLINE_MISS_RATE_UPTH;
	private double DEADLINE_MISS_RATE_LOWTH;
	private final static double ARRIVAL_RATE_TH = 0.5;
	private final static double TRNSCODING_RATE_TH = 0.5;
	private final static double DEADLINE_MISS_RATE_VARANCE_TH = 0.05;
	
	private static List<VideoSegment> cloudletReceivedList = new ArrayList<VideoSegment>();
	private static List<VideoSegment> previousCloudletReceivedList = new ArrayList<VideoSegment>();
	
	private static List<VideoSegment> cloudletArrivaledList = new ArrayList<VideoSegment>();
	private static List<VideoSegment> previousCloudletArrivaledList = new ArrayList<VideoSegment>();
	
	private static List<Cloudlet> cloudletNewList = new ArrayList<Cloudlet>();
	
	
	//private List<VideoSegment> startupQueueList = new ArrayList<VideoSegment>();
	//private List<VideoSegment> batchQueueList = new ArrayList<VideoSegment>();
	//private Map<Integer, Double> vmCompletionTimeMap = new HashMap<Integer, Double>();

    
	

    
	TranscodingBroker tb;
	
	
	public TranscodingProvisioner(TranscodingBroker transcodingBroker, String propertiesFileURL) throws IOException {
		this.tb = transcodingBroker;
		
		Properties prop = new Properties();
		//InputStream input = new FileInputStream("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudsim-impl/config.properties");
		InputStream input = new FileInputStream(propertiesFileURL);
		prop.load(input);
		
		String upTh = prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.2");
		String lowTh = prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.05");
		String frequency = prop.getProperty("periodicDelay", "25000");
		
		this.frequency = Integer.valueOf(frequency);
		this.DEADLINE_MISS_RATE_UPTH = Double.valueOf(upTh);
		this.DEADLINE_MISS_RATE_LOWTH = Double.valueOf(lowTh);
		
 	}
	
	/**
	 * Predict the deadline miss rate of next period of time
	 */
	
	public double getEstimatedDeadlineMissRate(){
		
		List<VideoSegment> startupQueueList = Collections.synchronizedList(new ArrayList<VideoSegment>());	
	    List<VideoSegment> batchQueueList = Collections.synchronizedList(new ArrayList<VideoSegment>());	
		
	    Map<Integer, Double> vmCompletionTimeMap = new HashMap<Integer, Double>();
		Map.Entry<Integer, Double> minCompletionTime_vm =  null;
	    Map<Integer, Double> displayStartupTimeMap = new HashMap<Integer, Double>();
		VideoSegment cloudlet;
		VideoSegment cloudlet_new;
		VideoSegment cloudlet_batch;
		Vm vm;

		int vmIndex;
		int switch_flag = 0;
		double gopCount = 0.0;
		double gopMissDeadlineCount = 0.0;
		double dmr;
		
		double gopCompletionTime;
		double gopDeadline;
		double estimated_completionTime;
		double timePeriod = 0.0;
		
		int testCount;
	    
		vmCompletionTimeMap.putAll(tb.totalCompletionTime_vmMap);
		startupQueueList.addAll(tb.cloudletNewList);
		batchQueueList.addAll(tb.cloudletList);
	    
		while(timePeriod < frequency){
			/**
			 * 1. Find the minimum completion time VM
			 */
			
			
			
			for (Map.Entry<Integer, Double> entry : vmCompletionTimeMap.entrySet()) {
			    if (minCompletionTime_vm == null || minCompletionTime_vm.getValue() > entry.getValue()) {
			        minCompletionTime_vm = entry;
			    }
			}
			if(minCompletionTime_vm == null){
				vmIndex = 0;
			}else {
		        vmIndex = minCompletionTime_vm.getKey();
			}
			
			
			/**
			 * 2. check the first GOP (either from startup queue or batch queue) will miss its deadline or not
			 */
			
			//get the first cloudlet in the new arrival queue
			if(startupQueueList.size() > 0) {
				cloudlet_new = (VideoSegment) startupQueueList.get(0);
	
				
					//get the first cloudlet in the batch queue
					if(batchQueueList.size() > 0){
					    cloudlet_batch = (VideoSegment) batchQueueList.get(0);
					    //check if cloudlet in new arrial and batch are the same video stream
					    //If they are the same video, always send the new arrival queue first
		 
					   // calculate the expected time for cloudlet completion
						if (cloudlet_new.getVmId() == -1) {
							//vm = getVmsCreatedList().get(vmIndex);
							vm = (TranscodingVm) tb.getVmList().get(vmIndex);
	
						} else { // submit to the specific vm
							vm = VmList.getById(tb.getVmList(), cloudlet_new.getVmId());
							if (vm == null) { 
								// vm was not created
								/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
										+ cloudlet.getCloudletId() + ": bount VM not available");*/
								Log.printLine(tb.getName() + ": Postponing execution of cloudlet "
										+ cloudlet_new.getCloudletId() + ": bount VM not available");
								continue;
								
							}
						}	
						//Set Each VM CloudletWaitingList to 2, so if a VM's waitingList is beyound 2, current cloudlet 
						//won't be sent to specific vm.
						VideoSchedulerSpaceShared vcschTemp = (VideoSchedulerSpaceShared) vm.getCloudletScheduler();
						double capacity = 0.0;
						int cpus = 0;
						for (Double mips : vcschTemp.getCurrentMipsShare()) {
							capacity += mips;
							if (mips > 0) {
								cpus++;
							}
						}
				
						int currentCpus = cpus;
						capacity /= cpus;
						
						
						//reset each cloudlet's deadline after the first cloudlet begins to display
					    if(cloudlet_new.getCloudletVideoId() == cloudlet_batch.getCloudletVideoId()) {
						    
					    	cloudlet =(VideoSegment) startupQueueList.get(0);
					      //  getCloudletNewList().remove(0);
					      //  vstream.getNewArrivalQueue().remove(0);
	
					    	switch_flag = 1; 
	                        
					    	/**
					    	 * Calculate random smaple from normal distribution
					    	 */
					    	Random r = new Random();
					    	double val = r.nextGaussian()*cloudlet_new.getCloudletStd() + cloudlet_new.getAvgCloudletLength();
					    	long sampleLength = (long) Math.round(val);
					    	cloudlet_new.setCloudletLength(sampleLength);
					    	
					    	
						    estimated_completionTime = cloudlet_new.getCloudletLength() / capacity + minCompletionTime_vm.getValue();
	                        
						    if(!displayStartupTimeMap.containsKey(cloudlet.getCloudletVideoId()) || cloudlet.getCloudletId() == 0){	
						         displayStartupTimeMap.put(cloudlet.getCloudletVideoId(), estimated_completionTime + CloudSim.clock());
								// cloudlet.setCloudletDeadline(minToCompleteTime);
							
								 for(VideoSegment vs: startupQueueList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }
								 for(VideoSegment vs:batchQueueList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }	 
							}
					    	
					    }else{//if they are not the same video, then continue algorithm
					
							
					
						
			/*			    *//** 
					        * Now what it should do next:
					        * 1. find a calculate each vm's estimated completion time
					        * 2. find the smallest completion time vm, and send it cloudlet. 
					        */
					    	
					    	/**
					    	 * Calculate random smaple from normal distribution
					    	 */
					    	Random r = new Random();
					    	double val = r.nextGaussian()*cloudlet_new.getCloudletStd() + cloudlet_new.getAvgCloudletLength();
					    	long sampleLength = (long) Math.round(val);
					    	cloudlet_new.setCloudletLength(sampleLength);
							
						    estimated_completionTime = cloudlet_new.getCloudletLength() / capacity + minCompletionTime_vm.getValue();
						    
						    //After calclulate the estimated min completion time of inserted new cloudlet, replace that vm's completion time
						    //Create a new map to find the minimum completion time Vm
						    
						    
						    Map<Integer, Double> totalCompletionTime_vmMap_New = new HashMap<Integer, Double>();
			                
						    totalCompletionTime_vmMap_New.putAll(vmCompletionTimeMap);
						    
						    for(Integer key:totalCompletionTime_vmMap_New.keySet()){
								if(key == vmIndex) {
									totalCompletionTime_vmMap_New.put(key,estimated_completionTime);
			
								}
							}
						    //compare this min completion time with other vm's completion time again in the totalCompletionTime_vmMap
			                //Find the min completion time VM afater insert new arrival cloudlet
						    
						    
						    Map.Entry<Integer, Double> minCompletionTime_vm_new = null;
							for (Map.Entry<Integer, Double> entry : totalCompletionTime_vmMap_New.entrySet()) {
							    if (minCompletionTime_vm_new == null || minCompletionTime_vm_new.getValue() > entry.getValue()) {
							        minCompletionTime_vm_new = entry;
							    }
							}
						    
						    //double minToCompleteTime =  minCompletionTime_vm_new.getValue() + cloudlet_batch.getCloudletLength() / capacity + CloudSim.clock();
						    double minToCompleteTime =  minCompletionTime_vm_new.getValue() + cloudlet_batch.getCloudletLength() / capacity + CloudSim.clock();
	
						   // double cloudletDeadlineAbs = cloudlet_batch.getCloudletDeadline() + cloudlet_batch.getSubmissionTime(getId());
						    double cloudletDeadlineAbs = cloudlet_batch.getCloudletDeadline();
	
						    
						    //for test
							/**
							 * reset each cloudlet's deadline after the first cloudlet begins to display
							 * It's based on estimated finish time
							 */
	
					    	if(cloudletDeadlineAbs < minToCompleteTime){
						    	cloudlet = cloudlet_batch;
									switch_flag = 0;
								}else {
									cloudlet = cloudlet_new;
									
							       // getCloudletNewList().remove(0);
							      //  vstream.getNewArrivalQueue().remove(0);
	
									switch_flag = 1;
									
									
									if(!displayStartupTimeMap.containsKey(cloudlet.getCloudletVideoId()) || cloudlet.getCloudletId() == 0){	
								         displayStartupTimeMap.put(cloudlet.getCloudletVideoId(), estimated_completionTime + CloudSim.clock());
										// cloudlet.setCloudletDeadline(minToCompleteTime);
									
	
										 for(VideoSegment vs:startupQueueList){
											 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
											    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
											 }
										 }
										 for(VideoSegment vs:batchQueueList){
											 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
											    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
											 }
										 }	 
									}
									
									
						     }	
						    
						    
			           }   
					    
					    
					    
				//If batch queue is empty, but new arrival queue is not, send the new arrival cloudlet	
					}else {
					       cloudlet =(VideoSegment) startupQueueList.get(0);
					       switch_flag = 1;
					       
					       /**
					        * reset cloudlet deadline
					        */
					       // calculate the expected time for cloudlet completion
							if (cloudlet_new.getVmId() == -1) {
								//vm = getVmsCreatedList().get(vmIndex);
								vm = (TranscodingVm) tb.getVmList().get(vmIndex);
	
							} else { // submit to the specific vm
								vm = VmList.getById(tb.getVmList(), cloudlet_new.getVmId());
								if (vm == null) { 
									// vm was not created
									/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
											+ cloudlet.getCloudletId() + ": bount VM not available");*/
									Log.printLine(tb.getName() + ": Postponing execution of cloudlet "
											+ cloudlet_new.getCloudletId() + ": bount VM not available");
									continue;
								}
							}	
							//Set Each VM CloudletWaitingList to 2, so if a VM's waitingList is beyound 2, current cloudlet 
							//won't be sent to specific vm.
							VideoSchedulerSpaceShared vcschTemp = (VideoSchedulerSpaceShared) vm.getCloudletScheduler();
							double capacity = 0.0;
							int cpus = 0;
							for (Double mips : vcschTemp.getCurrentMipsShare()) {
								capacity += mips;
								if (mips > 0) {
									cpus++;
								}
							}
					
							int currentCpus = cpus;
							capacity /= cpus;
					        cloudlet =(VideoSegment) startupQueueList.get(0);
					      //  getCloudletNewList().remove(0);
					      //  vstream.getNewArrivalQueue().remove(0);
					        
					    	switch_flag = 1; 
					    	
					    	/**
					    	 * Calculate random smaple from normal distribution
					    	 */
					    	Random r = new Random();
					    	double val = r.nextGaussian()*cloudlet_new.getCloudletStd() + cloudlet_new.getAvgCloudletLength();
					    	long sampleLength = (long) Math.round(val);
					    	cloudlet_new.setCloudletLength(sampleLength);
		
						    estimated_completionTime = cloudlet_new.getCloudletLength() / capacity + minCompletionTime_vm.getValue();
		                   
						    if(!displayStartupTimeMap.containsKey(cloudlet.getCloudletVideoId()) || cloudlet.getCloudletId() == 0){	
						         displayStartupTimeMap.put(cloudlet.getCloudletVideoId(), estimated_completionTime + CloudSim.clock());
								// cloudlet.setCloudletDeadline(minToCompleteTime);
							
								 for(VideoSegment vs:startupQueueList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }
								 for(VideoSegment vs:batchQueueList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }	 
							}
					       
					       
					       
					}
	        //If new arrival queue is empty, checek batch queue
	          }else{	
		        	if(batchQueueList.size() > 0){
		    	        
				       cloudlet =(VideoSegment) batchQueueList.get(0);
				       switch_flag = 0;
				       
				       /**
				    	 * Calculate random smaple from normal distribution
				    	 */
				    	Random r = new Random();
				    	double val = r.nextGaussian()*cloudlet.getCloudletStd() + cloudlet.getAvgCloudletLength();
				    	long sampleLength = (long) Math.round(val);
				    	cloudlet.setCloudletLength(sampleLength);
				       
				    //if both new arrival and batch queue are empty, return and stop sending cloudlets
		        	}else {
		        		//return;
		        		break;
		        	}
		      }
			
			
			/**
			 * 3. Calculate the number of GOP and those who missed deadline
			 */
			
			
			// calculate the expected time for cloudlet completion
			if (cloudlet.getVmId() == -1) {
				//vm = getVmsCreatedList().get(vmIndex);
				vm = (TranscodingVm) tb.getVmList().get(vmIndex);
	
			} else { // submit to the specific vm
				vm = VmList.getById(tb.getVmList(), cloudlet.getVmId());
				if (vm == null) { 
					// vm was not created
					/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
							+ cloudlet.getCloudletId() + ": bount VM not available");*/
					Log.printLine(tb.getName() + ": Postponing execution of cloudlet "
							+ cloudlet.getCloudletId() + ": bount VM not available");
					continue;
				}
			}	
		
			//Set Each VM CloudletWaitingList to 2, so if a VM's waitingList is beyound 2, current cloudlet 
			//won't be sent to specific vm.
			VideoSchedulerSpaceShared vcschTemp = (VideoSchedulerSpaceShared) vm.getCloudletScheduler();
			double capacity = 0.0;
			int cpus = 0;
			for (Double mips : vcschTemp.getCurrentMipsShare()) {
				capacity += mips;
				if (mips > 0) {
					cpus++;
				}
			}
	
			int currentCpus = cpus;
			capacity /= cpus;
			
		    double gop_estimated_completionTime = cloudlet.getCloudletLength() / capacity + minCompletionTime_vm.getValue();
		  //  System.out.println("VideoID: " + cloudlet.getCloudletVideoId() + "   GOP#: " + cloudlet.getCloudletId() + "    DMR: " + cloudlet.getCloudletDeadline() + "     Completion Time:" + (gop_estimated_completionTime + CloudSim.clock()));
	        if(cloudlet.getCloudletDeadline() < (gop_estimated_completionTime + CloudSim.clock())){
	        	gopMissDeadlineCount ++;
	        	gopCount++;
	        }else{
	        	gopCount++;
	        }
	        
	        
	        /**
	         * 4. update each VM completion time and startup queue and batch queue list
	         *     
	         */
	        
	        if(switch_flag == 1){
	        	startupQueueList.remove(cloudlet);
	        }else{
	        	batchQueueList.remove(cloudlet);
	        }
	        
			vmCompletionTimeMap.put(vmIndex, gop_estimated_completionTime);
	
			/**
			 * 5. Add up completion time until it equals to period of time.   
			 */
			Map.Entry<Integer, Double> maxEntry = null;

			for (Map.Entry<Integer, Double> entry : vmCompletionTimeMap.entrySet())
			{
			    if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0)
			    {
			        maxEntry = entry;
			    }
			}
			
			timePeriod = maxEntry.getValue();		
			
		} 	
		   /**
		    * 5. Calculate the deadline miss rate in the next period of time
		    */
		    
		    System.out.println("\nThere are " + gopCount + " GOPs will be transcoded in the next period of time");
		    System.out.println("There are " + gopMissDeadlineCount + " GOPs will miss their deadline in the next period of time\n");
		    
		    if(gopCount !=0 ){
		         dmr = gopMissDeadlineCount/gopCount;
		    }else{
		    	 dmr = 0.0;
		    }
		    System.out.println("\n********The deadline miss rate of next time period is: " + dmr + "*************\n");
		
		return dmr;
		
		
	}
	
	
	/**
	 * Calculate a period of time deadline miss rate
	 * @return
	 * @throws Exception 
	 */
	public double getDeadlineMissRate() {
        
		int deadlineMissCount = 0;
	   // double dmrVarance = 0.0;

		
		cloudletReceivedList = tb.getCloudletReceivedList();
		
		//calculte this period of time received cloudlets by minusing the former recieved cloudlets.
		cloudletReceivedList.removeAll(previousCloudletReceivedList);
		
		for(VideoSegment cl:cloudletReceivedList){
			if(cl.getCloudletDeadline() < cl.getFinishTime()){
				deadlineMissCount++;
			}
		}
		
		previousCloudletReceivedList.addAll(cloudletReceivedList);
		if(cloudletReceivedList.size() != 0) {
		 deadlineMissRate = (double) deadlineMissCount/ (double)cloudletReceivedList.size();
		}else{
		   deadlineMissRate = 0;
		}
		
		System.out.println("\n******The deadline miss rate during this period of time is: " + deadlineMissRate + " *************************\n");
		System.out.println("\n******The previous deadline miss rate is: " + previousDeadlineMissRate + " *************************\n");
        
		deadlineMissRateVarance = deadlineMissRate - previousDeadlineMissRate;
		
		previousDeadlineMissRate = deadlineMissRate;
        
		if(deadlineMissRateVarance >= 0){
		     System.out.println("\n******The deadline miss rate has increased: " + deadlineMissRateVarance + " *************************\n");
		}else{
		     System.out.println("\n******The deadline miss rate has decreased: " + deadlineMissRateVarance + " *************************\n");
		}
		
		
		return deadlineMissRate;
	}
	
    public int getStartupQueueLength(){
    	//int startupQueueLength = 0;
    	int videoNum = 0;
    	
    	List<Integer> videoIdList = new ArrayList<Integer>();
    	cloudletNewList.addAll(tb.getCloudletNewList());
    	
    	for(Cloudlet cl:cloudletNewList){
    		VideoSegment vs = (VideoSegment)cl;
    		if(!videoIdList.contains(vs.getVmId())){
    			videoNum ++;
    			videoIdList.add(vs.getVmId());
    			
    		}else{
    			continue;
    		}
    		
    	} 	
    	
    	return videoNum;
    	
    }
	
	/**
	 * Calculate a period of time video streams arrival rate
	 * @return
	 */
	public double getArrivalRate(){
		
		cloudletArrivaledList = tb.getCloudletSubmittedList();
		cloudletArrivaledList.remove(previousCloudletArrivaledList);
		previousCloudletArrivaledList.addAll(cloudletArrivaledList);
		
		int cloudletNum = cloudletArrivaledList.size();
		
		arrivalRate = cloudletNum * 1000.00 / frequency;

		return arrivalRate;
	}
	
	/**
	 * Calculate a period of time vms transcoding rate
	 * @return
	 */
	public double getTranscodingTrate(){
		return transcodingRate;
	}
	
	/**
	 * Allocate or deallocate vms based deadline miss rate, new streams arrival rate and vms' transcoding rate.
	 * @param deadlineMissRate
	 * @param arrivalRate
	 * @param transcodingRate
	 */
	public int allocateDeallocateVm(){
	   deadlineMissRate = getDeadlineMissRate();
	   
	   
	   if(deadlineMissRateVarance >= 0 && deadlineMissRate > DEADLINE_MISS_RATE_UPTH){
		   
				if(deadlineMissRate < 2*DEADLINE_MISS_RATE_UPTH){
			       return 1;
			    
				}else{
				   return 3;
				}
		   
	   }else if(deadlineMissRateVarance <= 0 && deadlineMissRate < DEADLINE_MISS_RATE_LOWTH){
				return 2;
	   }else{
				return 0;
	        }
     }
	  
    	

}
