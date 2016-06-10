package cloudproject.transcoding;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.lists.VmList;




//SortingCloudletsBroker is inherited to DatacenterBroker
public class TranscodingBroker extends DatacenterBroker{
	
	/** The cloudlet new arrival list. */
	protected List<VideoSegment> cloudletNewList = new ArrayList<VideoSegment>();
	protected List<VideoSegment> cloudletList = new ArrayList<VideoSegment>();

	
   //create sending cloudlet event
	public final static int CLOUDLET_SUBMIT_RESUME = 125;
   //exchange completion time between datacenter and broker	
	public final static int ESTIMATED_COMPLETION_TIME = 126;
   //create period event
	public final static int PERIODIC_EVENT = 127;
   //video Id
	public static int videoId = 1;
	
    double periodicDelay = 5; //contains the delay to the next periodic event
    boolean generatePeriodicEvent = true; //true if new internal events have to be generated
	
	//All the instance share cloudletNewArrivalQueue and cloudletBatchqueue, both of them are synchronized list
	private static List<VideoSegment> cloudletNewArrivalQueue = Collections.synchronizedList(new ArrayList<VideoSegment>());
	private static List<VideoSegment> cloudletBatchQueue = Collections.synchronizedList(new ArrayList<VideoSegment>());	
	
	public List<TranscodingVm> vmDestroyedList = new ArrayList<TranscodingVm>();

	//set size of local vm queue
	//private final static int waitinglist_max = 2;
	private int waitinglist_max;
	
	public Map<Integer, Double> totalCompletionTime_vmMap = new HashMap<Integer, Double>();
	public static Map<Integer, Double> totalCompletionTime_vmMap_Min = new HashMap<Integer, Double>();
	
	//Track the disply start up time
	private Map<Integer, Double> displayStartupTimeMap = new HashMap<Integer, Double>();
	private Map<Integer, Double> displayStartupTimeRealMap = new HashMap<Integer, Double>();
	
	private Map<Integer, Double> videoStartupTimeMap = new HashMap<Integer, Double>();
 
	private static int waitingListSize = 0;

    
	
    int vmIndex;
    int temp_key = 0;
    
    
    int cloudletSubmittedCount;
    boolean broker_vm_deallocation_flag =false;
    
	static int testBrokerCount=0;

	
	//vm Cost
    private static double vmCost = 0;
    
    //set up DatacenterCharacteristics;
	public DatacenterCharacteristics characteristics;
	public boolean startupqueue;
	public String sortalgorithm;
	
    //flag = 0, cloudlet is from batch queue
    //flag = 1, cloudlet is from new arrival queue
    private int switch_flag = 0;

	public TranscodingBroker(String name, DatacenterCharacteristics characteristics, String propertiesFileURL) throws Exception {
         super(name);
         
         Properties prop = new Properties();
		 //InputStream input = new FileInputStream("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudsim-impl/config.properties");
         InputStream input = new FileInputStream(propertiesFileURL);

         prop.load(input);
		
		 String waitinglist = prop.getProperty("waitinglist_max", "2");
		 String startupqueue = prop.getProperty("startupqueue", "true");
		 
		 this.sortalgorithm = prop.getProperty("sortalgorithm", "SDF");		 
		 this.waitinglist_max = Integer.valueOf(waitinglist);
		 this.startupqueue = Boolean.valueOf(startupqueue);
		 this.characteristics = characteristics;
		 
		 
		 
      // TODO Auto-generated constructor stub
    }
	
	

	/**
	 * Processes events available for this Broker.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	@Override
	public void processEvent(SimEvent ev) {
		switch (ev.getTag()) {
		// Resource characteristics request
			case CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
				processResourceCharacteristicsRequest(ev);
				break;
			// Resource characteristics answer
			case CloudSimTags.RESOURCE_CHARACTERISTICS:
				processResourceCharacteristics(ev);
				break;
			// VM Creation answer
			case CloudSimTags.VM_CREATE_ACK:
				processVmCreate(ev);
				break;
			// A finished cloudlet returned
			case CloudSimTags.CLOUDLET_RETURN:
				processCloudletReturn(ev);
				break;
			// if the simulation finishes
			case CloudSimTags.END_OF_SIMULATION:
				shutdownEntity();
				break;
			/**
			* @override
			* add a new tag CLOUDLET_SUBMIT_RESUME to broker
			* updating cloudletwaitinglist in VideoSegmentScheduler, whenever a vm's waitinglist is smaller
			* than 2, it will add a event in the broker, so that broker can keep send this vm the rest of 
			* cloudlet in its batch queue
			**/	
		    case CLOUDLET_SUBMIT_RESUME: 
		    	resumeSubmitCloudlets(ev); 
		    	break;
		    case ESTIMATED_COMPLETION_TIME:
		    	setVmCompletionTime(ev);
		    	//submitCloudlets();
		    	break;

			// other unknown tags are processed by this method
			default:
				processOtherEvent(ev);
				break;
		}
	}

    /**
     * Calculate Vm cost 
     * @param ev
     */
	public void setVmCost(TranscodingVm vm){
		vmCost += characteristics.getCostPerSecond()*(vm.getVmFinishTime() - vm.getStartTime())/1000.0;
	}
	
	/**
	 * get vm Cost;
	 * @param ev
	 */
	public double getVmCost(){
		return vmCost;
	}
	
    private void resumeSubmitCloudlets(SimEvent ev) {
	   submitCloudlets();
	 }
    
    private void setVmCompletionTime (SimEvent ev) {
        if (ev.getData() instanceof Map) {
        	totalCompletionTime_vmMap =(Map) ev.getData();
        }
    }
    
    private Map<Integer, Double> getVmCompletionTime(){
    	return totalCompletionTime_vmMap;
    }
    
    /**
	 * Process the ack received due to a request for VM creation.
	 * 
	 * @param ev a SimEvent object
	 * @pre ev != null
	 * @post $none
	 */
	protected void processVmCreate(SimEvent ev) {
		int[] data = (int[]) ev.getData();
		int datacenterId = data[0];
		int vmId = data[1];
		int result = data[2];

		if (result == CloudSimTags.TRUE) {
			getVmsToDatacentersMap().put(vmId, datacenterId);
			getVmsCreatedList().add(VmList.getById(getVmList(), vmId));
			Log.printLine(CloudSim.clock() + ": " + getName() + ": VM #" + vmId
					+ " has been created in Datacenter #" + datacenterId + ", Host #"
					+ VmList.getById(getVmsCreatedList(), vmId).getHost().getId() + "\n");
		} else {
			Log.printLine(CloudSim.clock() + ": " + getName() + ": Creation of VM #" + vmId
					+ " failed in Datacenter #" + datacenterId);
		}

		incrementVmsAcks();
		
		
		

		// all the requested VMs have been created
		//if (getVmsCreatedList().size() == getVmList().size() - getVmsDestroyed()) {
		
		List<Vm> vmTempList = new ArrayList<Vm>();
		vmTempList.addAll(getVmsCreatedList());
		vmTempList.removeAll(vmDestroyedList);
		if(vmTempList.size() > 0){
		  Map<Integer, Double> totalCompletionTime_vmMap_temp = new HashMap<Integer, Double>();
		  totalCompletionTime_vmMap_temp = totalCompletionTime_vmMap;
			//Initial all vm completion time as 0.
			 for(Vm vm: vmTempList){
				    if(totalCompletionTime_vmMap_temp.containsKey(vm.getId())){
				    	totalCompletionTime_vmMap.put(vm.getId(), totalCompletionTime_vmMap_temp.get(vm.getId()));
				    }else{
				    	System.out.println("\ninitial vmcompletiontimemap test");
		        	    totalCompletionTime_vmMap.put(vm.getId(), 0.0);
				    }
		     }
			submitCloudlets();
		} else {
			// all the acks received, but some VMs were not created
			if (getVmsRequested() == getVmsAcks()) {
				// find id of the next datacenter that has not been tried
				for (int nextDatacenterId : getDatacenterIdsList()) {
					if (!getDatacenterRequestedIdsList().contains(nextDatacenterId)) {
						createVmsInDatacenter(nextDatacenterId);
						return;
					}
				}

				// all datacenters already queried
				if (getVmsCreatedList().size() > 0) { // if some vm were created
					submitCloudlets();
				} else { // no vms created. abort
					Log.printLine(CloudSim.clock() + ": " + getName()
							+ ": none of the required VMs could be created. Aborting");
					finishExecution();
				}
			}
		}
	}
	
	protected void submitCloudlets() {
		
		
		if(sortalgorithm.equals("FCFS")){
		    FCFS();
		}else if(sortalgorithm.equals("SJF")){
			SortedbySJF();	
		}else{
		    SortedbyDeadline();
		}
       /* if(CloudSim.clock() >= 120000){
        	System.out.println("Test broker..");
        }*/
		/** 
        * Now what it should do next:
        * 1. find a calculate each vm's estimated completion time
        * 2. find the smallest completion time vm, and send it cloudlet. 
        */
		Map.Entry<Integer, Double> minCompletionTime_vm;
		TranscodingVm vmToSend;
		while(true){ 
			
			
			
			//
			//totalCompletionTime_vmMap_Min.putAll(totalCompletionTime_vmMap);
			
			minCompletionTime_vm = null;
			for (Map.Entry<Integer, Double> entry : totalCompletionTime_vmMap.entrySet()) {
			    if (minCompletionTime_vm == null || minCompletionTime_vm.getValue() >= entry.getValue()) {
			        minCompletionTime_vm = entry;
			    }
			}
			if(minCompletionTime_vm == null){
				vmIndex = 0;
			}else {
		        vmIndex = minCompletionTime_vm.getKey();
			}
			
           
			
			
			
			//find the minimal completion time vmId
		   // vmToSend = (TranscodingVm) getVmsCreatedList().get(vmIndex);
			vmToSend = (TranscodingVm) getVmList().get(vmIndex);
			
		    
		    //check if this vm is about to be destroyed or not, if yes, find another one
			//check vm's remaining time
		    if(vmToSend.getDeallocationFlag() && vmToSend.getRemainingTime() <= 0 ) {
		    	//set this vm's completion time as its remaining time
		    	//vmToSend.setRemainingTime(minCompletionTime_vm.getValue());
		    	//remove this vm from vm map
		    	/*System.out.println(CloudSim.clock() + "The mini completion time is: " + minCompletionTime_vm.getValue());

		    	System.out.println(CloudSim.clock() + "VM#: " + vmToSend.getId() + "'s renting time until " + vmToSend.getRentingTime() + 
						 "...The remaining time is: " + vmToSend.getRemainingTime());	*/
		    	
		    	//broker_vm_deallocation_flag = true;
		    	//getVmList().remove(vmToSend);
		    	
				VideoSchedulerSpaceShared vmcsch = (VideoSchedulerSpaceShared) vmToSend.getCloudletScheduler();

		    	
		    	if(vmcsch.getCloudletExecList() == null && vmcsch.getCloudletWaitingList() == null && vmcsch.getCloudletPausedList() == null){
			    	//System.out.println(CloudSim.clock() + "\n********************VM_" + vmToSend.getId() + "'s renting time is over and to be destroyed***********************" );
		    		sendNow(getVmsToDatacentersMap().get(vmToSend.getId()), CloudSimTags.VM_DESTROY, vmToSend);
	                vmDestroyedList.add(vmToSend);
	                
	                //set Vm's finish time.
	                vmToSend.setVmFinishTime(CloudSim.clock());
	    	      
	                //Calculate vm cost based on the time it last.
	                setVmCost(vmToSend);
		    	}
		    	
		    	totalCompletionTime_vmMap.remove(vmIndex);
		    	
		    	continue;
		    }else if(vmToSend.getDeallocationFlag() && vmToSend.getRemainingTime() > 0 ){
		    	/*System.out.println("\nThe VM#: " + vmToSend.getId() +" has been set to destroy, but the remaing time is: " + vmToSend.getRemainingTime() + " So "
		    			+ "keep sending cloudlets to this vm");*/
		    	
		    	break;
		    }else{
		    	break;
		    }
		}  

		
		/**
		 * Check if we can insert the first cloudlet in the new arrival queue to the front of the batch queue. 
		 * 1. calculate the minimum completion time when cloudlet_new insert in the front of batch queue
		 * 2. compare this minimum completion time to the deadline of the cloudlet in the front of batch queue
		 * 
		 */
		
		
		//cloudlet will be sent to vm
		VideoSegment cloudlet;
		VideoSegment cloudlet_new;
		VideoSegment cloudlet_batch;
		//VideoStreams vstream = new VideoStreams();
		Vm vm;

	
		double estimated_completionTime;
		
		/* 1. calculate the minimum completion time when cloudlet_new insert in the front of batch queue */

		if(startupqueue){
			/**
			 * With new arrival queue
			 */
			//get the first cloudlet in the new arrival queue
			if(getCloudletNewList().size() > 0) {
				cloudlet_new = (VideoSegment) getCloudletNewList().get(0);

				
					//get the first cloudlet in the batch queue
					if(getCloudletList().size() > 0){
					    cloudlet_batch = (VideoSegment) getCloudletList().get(0);
					    //check if cloudlet in new arrial and batch are the same video stream
					    //If they are the same video, always send the new arrival queue first
					    
					    
					  //for debug
					     if(cloudlet_batch.getCloudletVideoId() == 5 && cloudlet_batch.getCloudletId() >= 5){
					 		System.out.println("Test for broker...");
					     }
					    
					    
					    
					   // calculate the expected time for cloudlet completion
						if (cloudlet_new.getVmId() == -1) {
							//vm = getVmsCreatedList().get(vmIndex);
							vm = (TranscodingVm) getVmList().get(vmIndex);
	
						} else { // submit to the specific vm
							vm = VmList.getById(getVmList(), cloudlet_new.getVmId());
							if (vm == null) { 
								// vm was not created
								/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
										+ cloudlet.getCloudletId() + ": bount VM not available");*/
								Log.printLine(getName() + ": Postponing execution of cloudlet "
										+ cloudlet_new.getCloudletId() + ": bount VM not available");
								return;
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
						    
					    	cloudlet =(VideoSegment) getCloudletNewList().get(0);
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
							
								 for(VideoSegment vs:cloudletNewList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }
								 for(VideoSegment vs:cloudletList){
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
			                
						    totalCompletionTime_vmMap_New.putAll(totalCompletionTime_vmMap);
						    
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
									
	
										 for(VideoSegment vs:cloudletNewList){
											 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
											    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
											 }
										 }
										 for(VideoSegment vs:cloudletList){
											 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
											    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
											 }
										 }	 
									}
									
									
						     }	
						    
						    
			           }   
					    
					    
					    
				//If batch queue is empty, but new arrival queue is not, send the new arrival cloudlet	
					}else {
					       cloudlet =(VideoSegment) getCloudletNewList().get(0);
					       switch_flag = 1;
					       
					       /**
					        * reset cloudlet deadline
					        */
					       // calculate the expected time for cloudlet completion
							if (cloudlet_new.getVmId() == -1) {
								//vm = getVmsCreatedList().get(vmIndex);
								vm = (TranscodingVm) getVmList().get(vmIndex);
	
							} else { // submit to the specific vm
								vm = VmList.getById(getVmList(), cloudlet_new.getVmId());
								if (vm == null) { 
									// vm was not created
									/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
											+ cloudlet.getCloudletId() + ": bount VM not available");*/
									Log.printLine(getName() + ": Postponing execution of cloudlet "
											+ cloudlet_new.getCloudletId() + ": bount VM not available");
									return;
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
					        cloudlet =(VideoSegment) getCloudletNewList().get(0);
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
							
								 for(VideoSegment vs:cloudletNewList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }
								 for(VideoSegment vs:cloudletList){
									 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
									    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
									 }
								 }	 
							}
					       
					       
					       
					}
	        //If new arrival queue is empty, checek batch queue
	        }else{	
	        	if(getCloudletList().size() > 0){
	        
			       cloudlet =(VideoSegment) getCloudletList().get(0);
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
	        		return;
	        	}
	        }
		}else{
			/*
			 * without new arrival queue
			 */
			//get the first cloudlet in the batch queue
			if(getCloudletList().size() > 0){
			    cloudlet_batch = (VideoSegment) getCloudletList().get(0);
			    //check if cloudlet in new arrial and batch are the same video stream
			    //If they are the same video, always send the new arrival queue first
			    
			    
			   // calculate the expected time for cloudlet completion
				if (cloudlet_batch.getVmId() == -1) {
					vm = getVmList().get(vmIndex);
				} else { // submit to the specific vm
					vm = VmList.getById(getVmList(), cloudlet_batch.getVmId());
					if (vm == null) { 
						// vm was not created
						/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
								+ cloudlet.getCloudletId() + ": bount VM not available");*/
						Log.printLine(getName() + ": Postponing execution of cloudlet "
								+ cloudlet_batch.getCloudletId() + ": bount VM not available");
						return;
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
				
				/**
		    	 * Calculate random smaple from normal distribution
		    	 */
		    	Random r = new Random();
		    	double val = r.nextGaussian()*cloudlet_batch.getCloudletStd() + cloudlet_batch.getAvgCloudletLength();
		    	long sampleLength = (long) Math.round(val);
		    	cloudlet_batch.setCloudletLength(sampleLength);

			    estimated_completionTime = cloudlet_batch.getCloudletLength() / capacity + minCompletionTime_vm.getValue();
	            
			    if(!displayStartupTimeMap.containsKey(cloudlet_batch.getCloudletVideoId()) || cloudlet_batch.getCloudletId() == 0){	
			         displayStartupTimeMap.put(cloudlet_batch.getCloudletVideoId(), estimated_completionTime + CloudSim.clock());
					// cloudlet.setCloudletDeadline(minToCompleteTime);
				
					 for(VideoSegment vs:cloudletList){
						 if(vs.getCloudletVideoId() == cloudlet_batch.getCloudletVideoId()){
						    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeMap.get(vs.getCloudletVideoId()));	
						 }
					 }	 
				}
			    cloudlet = cloudlet_batch;
			}else{
				return;
			}
		}
		
		
		
		// if user didn't bind this cloudlet and it has not been executed yet
		if (cloudlet.getVmId() == -1) {
			//vm = getVmsCreatedList().get(vmIndex);
			vm = (TranscodingVm) getVmList().get(vmIndex);

		} else { // submit to the specific vm
			vm = VmList.getById(getVmList(), cloudlet.getVmId());
			if (vm == null) { 
				// vm was not created
				/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Postponing execution of cloudlet "
						+ cloudlet.getCloudletId() + ": bount VM not available");*/
				Log.printLine(getName() + ": Postponing execution of cloudlet "
						+ cloudlet.getCloudletId() + ": bount VM not available");
				return;
			}
		}	
		//Set Each VM CloudletWaitingList to 2, so if a VM's waitingList is beyound 2, current cloudlet 
		//won't be sent to specific vm.
		VideoSchedulerSpaceShared vcsch = (VideoSchedulerSpaceShared) vm.getCloudletScheduler();
		List<? extends ResCloudlet> waitinglist = vcsch.getCloudletWaitingList();
		/*double capacity = 0.0;
		int cpus = 0;
		for (Double mips : vcsch.getCurrentMipsShare()) {
			capacity += mips;
			if (mips > 0) {
				cpus++;
			}
		}

		int currentCpus = cpus;
		capacity /= cpus;
		estimated_completionTime = cloudlet.getCloudletLength() / capacity + minCompletionTime_vm.getValue();*/
		if (waitinglist.size() >= waitinglist_max) {
			return;
		} else {

			/*Log.printLine(CloudSim.clock() + ": " + getName() + ": Sending cloudlet "
					+ cloudlet.getCloudletId() + " to VM #" + vm.getId());*/
	
		
			Log.printLine(CloudSim.clock() + getName() + ": Sending Video ID: " + cloudlet.getCloudletVideoId() +" Cloudlet "
						+ cloudlet.getCloudletId() + " to VM #" + vm.getId());
			cloudlet.setVmId(vm.getId());
			
	        EventData data = new EventData(cloudlet, totalCompletionTime_vmMap);
	       //totalCompletionTime_vmMap_Min.put(vmIndex, estimated_completionTime);
	        
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.CLOUDLET_SUBMIT, data);
			cloudletsSubmitted++;
					
			getCloudletSubmittedList().add(cloudlet);	
			
			if(switch_flag == 0){
				getCloudletList().remove(cloudlet);
			}else {
				getCloudletNewList().remove(cloudlet);
			}
	   }	
	    
      
	}
	
	protected void processCloudletReturn(SimEvent ev) {
		
		//Coordinator coordinator;
		VideoSegment cloudlet = (VideoSegment) ev.getData();
		getCloudletReceivedList().add(cloudlet);
	/*	Log.printLine(CloudSim.clock() + ": " + getName() + ": Cloudlet " + cloudlet.getCloudletId()
				+ " finished in VM" + cloudlet.getVmId());*/
		Log.printLine(CloudSim.clock() + getName() + " : Video Id" + cloudlet.getCloudletVideoId() + " Cloudlet " + cloudlet.getCloudletId()
				+ " finished in VM" + cloudlet.getVmId());
		cloudletsSubmitted--;
		
		
		//copy cloudletSubmitted for Coordinator use.
		cloudletSubmittedCount = cloudletsSubmitted;
		
		//Get this finished cloudlet's vm Id
		TranscodingVm vm = (TranscodingVm) VmList.getById(getVmsCreatedList(), cloudlet.getVmId());
		
        //reset local queue completion time map		
		double totalCompletionTime_temp =0;
		int vmId = cloudlet.getVmId();
		double execTime = cloudlet.getActualCPUTime();
		if(!vm.getDeallocationFlag() || vm.getRemainingTime() > 0){	
			//calculate the difference between the real exec time and estimated exec time
			double estimatedExecTime = cloudlet.getCloudletLength() / 8000;
			double execTime_dif = execTime - estimatedExecTime;

			
			totalCompletionTime_temp = totalCompletionTime_vmMap.get(vmId) - execTime + execTime_dif;
			totalCompletionTime_vmMap.put(vmId, totalCompletionTime_temp);		
		}
		
		
		//get this video's start up time
		if(cloudlet.getCloudletId() == 0){
			double videoStartupTime =0;
			videoStartupTime = cloudlet.getFinishTime() - cloudlet.getArrivalTime();
			videoStartupTimeMap.put(cloudlet.getCloudletVideoId(), videoStartupTime);
		}
		
		/*
		 * Remodify videos' deadline based on the real display time
		 */
		if(!displayStartupTimeRealMap.containsKey(cloudlet.getCloudletVideoId()) || cloudlet.getCloudletId() == 0){	
	         displayStartupTimeRealMap.put(cloudlet.getCloudletVideoId(), cloudlet.getFinishTime());
			// cloudlet.setCloudletDeadline(minToCompleteTime);
	         cloudlet.setCloudletDeadline(cloudlet.getFinishTime());
		

			 for(VideoSegment vs:cloudletNewList){
				 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
				    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeRealMap.get(vs.getCloudletVideoId()));	
				 }
			 }
			 for(VideoSegment vs:cloudletList){
				 if(vs.getCloudletVideoId() == cloudlet.getCloudletVideoId()){
				    vs.setCloudletDeadline(vs.getCloudletDeadlineAfterPlay() + displayStartupTimeRealMap.get(vs.getCloudletVideoId()));	
				 }
			 }		 
		}else{
			cloudlet.setCloudletDeadline(cloudlet.getCloudletDeadlineAfterPlay() + displayStartupTimeRealMap.get(cloudlet.getCloudletVideoId()));
		}
		
		//debug
		/*if(cloudlet.getCloudletVideoId() == 0 && cloudlet.getCloudletId() >= 5){
			System.out.println("Test cloudlet return");
		}*/
		
        
		//Check if this vm is set to be destroyed
		if(vm.getDeallocationFlag() && vm.getRemainingTime() <= 0) {	
			
			//Before destroying this vm, make sure all the cloudlets in this vm are finished.
			VideoSchedulerSpaceShared scheduler = (VideoSchedulerSpaceShared)vm.getCloudletScheduler();
	    	if(scheduler.getCloudletExecList().size() == 0 && scheduler.getCloudletWaitingList().size() == 0 && scheduler.getCloudletPausedList().size() == 0) {
	    		
	    		System.out.println(CloudSim.clock() + "\n********************Cloudles in VM_" + vm.getId() + " have finished***********************" );
	    		sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
                vmDestroyedList.add(vm);
                
                //set Vm's finish time.
                vm.setVmFinishTime(CloudSim.clock());
    	      
                //Calculate vm cost based on the time it last.
                setVmCost(vm);
	    	}
		}
		
		if (getCloudletList().size() == 0 && getCloudletNewList().size() == 0 && cloudletsSubmitted == 0 && !generatePeriodicEvent) { // all cloudlets executed
			//Log.printLine(CloudSim.clock() + ": " + getName() + ": All Cloudlets executed. Finishing...");
			Log.printLine(CloudSim.clock() + ": " + getName() + ": All Cloudlets executed. Finishing...");
            getVmsCreatedList().removeAll(vmDestroyedList);
			clearDatacenters();
			finishExecution();
		} else { // some cloudlets haven't finished yet
			
			if (getCloudletList().size() > 0 || getCloudletNewList().size() > 0) {
				submitCloudlets();
			}
				
			if (getCloudletList().size() > 0 && getCloudletNewList().size() > 0 && cloudletsSubmitted == 0) {
				// all the cloudlets sent finished. It means that some bount
				// cloudlet is waiting its VM be created 
				clearDatacenters();
				createVmsInDatacenter(0);
			}

		}
		
	}
	
	
	/**
	 * This method is used to send to the broker the list of cloudlets.
	 * 
	 * @param list the list
	 * @pre list !=null
	 * @post $none
	 */
	public void submitCloudletList(List<? extends Cloudlet> cloudletBatchQueue, List<? extends Cloudlet> cloudletNewArrivalQueue) {
		//Before submit new cloudlet list, delete those who have already been submitted. 
	   // List<? extends Cloudlet> cloudletNewArrivalQueue_temp = Collections.synchronizedList(new ArrayList<Cloudlet>());
	    List<Cloudlet> cloudletBatchQueue_temp = Collections.synchronizedList(new ArrayList<Cloudlet>());	
	    List<Cloudlet> cloudletNewQueue_temp = Collections.synchronizedList(new ArrayList<Cloudlet>());	
	   
	   // cloudletNewArrivalQueue_temp = cloudletNewArrivalQueue;
	    cloudletBatchQueue_temp.addAll(cloudletBatchQueue);
	    cloudletNewQueue_temp.addAll(cloudletNewArrivalQueue);
	    
		for(Cloudlet cl:cloudletBatchQueue_temp) {
			 if (getCloudletSubmittedList().contains(cl)){
				 cloudletBatchQueue.remove(cl);
			 }			 
		}
		
		//Delete duplicated cloudlets which are already in the batch queue.
		for(Cloudlet cl:cloudletBatchQueue_temp) {
			 if (getCloudletList().contains(cl)){
				 cloudletBatchQueue.remove(cl);
			 }			 
		}
		
		for(Cloudlet cl:cloudletNewQueue_temp) {
			 if (getCloudletSubmittedList().contains(cl)){
				 cloudletNewArrivalQueue.remove(cl);
			 }			 
		}
		
		//getCloudletNewList().clear();
		

		getCloudletList().addAll(cloudletBatchQueue);
		getCloudletNewList().addAll(cloudletNewArrivalQueue);
		ArrayList<Integer> newcloudlets = new ArrayList<Integer>();
		
		for(int i=0; i < cloudletNewList.size(); i++){
			 newcloudlets.add(cloudletNewList.get(i).getCloudletId());

		}
		
		
   	    System.out.println(Thread.currentThread().getName() + "*****New arrival queue Video ID_" + videoId + ": " + newcloudlets + " **********");

   	    System.out.println("**********************The size of batch queue is: " + getCloudletList().size() + " **************");

	}
	/**
	 * Gets the cloudlet batch queue list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletList() {
		return (List<T>) cloudletList;
	}
	
	
	/**
	 * Gets the cloudlet new arrival list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet list
	 */
	@SuppressWarnings("unchecked")
	public <T extends Cloudlet> List<T> getCloudletNewList() {
		return (List<T>) cloudletNewList;
	}
	
	
	public Map<Integer, Double> getVideoStartupTimeMap() {
		return videoStartupTimeMap;
	}
	
	
	public void clearDatacenters() {
		for (Vm vm : getVmsCreatedList()) {
			Log.printConcatLine(CloudSim.clock(), ": " + getName(), ": Destroying VM #", vm.getId());
			sendNow(getVmsToDatacentersMap().get(vm.getId()), CloudSimTags.VM_DESTROY, vm);
			
			
			//set Vm's finish time.
	        TranscodingVm vmm = (TranscodingVm)vm;
	        vmm.setVmFinishTime(CloudSim.clock());
	        
	        //Calculate vm cost based on the time it last.
	        setVmCost(vmm);
		}

		getVmsCreatedList().clear();
		
		
		
	}
	
	
	
	/**
	 * Gets the vms to datacenters map.
	 * 
	 * @return the vms to datacenters map
	 */
	public Map<Integer, Integer> getVmsToDatacentersMap() {
		return vmsToDatacentersMap;
	}
	
	
	/**
	 * Send an internal event communicating the end of the simulation.
	 * 
	 * @pre $none
	 * @post $none
	 */
	public void finishExecution() {
		sendNow(getId(), CloudSimTags.END_OF_SIMULATION);
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
	
	//sorted by cloudlet length
		protected void FCFS() {
			List<VideoSegment> lstCloudlets = getCloudletList();
			List<VideoSegment> lstNewCloudlets = getCloudletNewList();
			
			for (int a = 0; a < lstCloudlets.size(); a++) {
	            for (int b = a + 1; b < lstCloudlets.size(); b++) {
	            	
	            	if(lstCloudlets.get(b).getArrivalTime() < lstCloudlets.get(a).getArrivalTime()){
                		VideoSegment temp = lstCloudlets.get(a);
	                    lstCloudlets.set(a, lstCloudlets.get(b));
	                    lstCloudlets.set(b, temp);
                	}
	                /*if(lstCloudlets.get(b).getCloudletVideoId() == lstCloudlets.get(a).getCloudletVideoId()){
	                	if (lstCloudlets.get(b).getCloudletDeadline() < lstCloudlets.get(a).getCloudletDeadline()) {
		                    VideoSegment temp = lstCloudlets.get(a);
		                    lstCloudlets.set(a, lstCloudlets.get(b));
		                    lstCloudlets.set(b, temp);
		                }
	                }else{
	                	
	                }*/
	            }
	        }
			setCloudletList(lstCloudlets);
			
			/*for(Cloudlet newcl:lstNewCloudlets){
	            System.out.println("Cloudlet id = " + newcl.getCloudletId() + " - Length = " + newcl.getCloudletLength());

			}

	        //Printing ordered list of cloudlets
	        for (Cloudlet cl : lstCloudlets) {
	            System.out.println("Cloudlet id = " + cl.getCloudletId() + " - Length = " + cl.getCloudletLength());
	        }*/
		}
		
		
		//sorted by cloudlet deadline
		protected void SortedbyDeadline() {
			List<VideoSegment> lstCloudlets = getCloudletList();
			List<VideoSegment> lstNewCloudlets = getCloudletNewList();
			
			for (int a = 0; a < lstCloudlets.size(); a++) {
	            for (int b = a + 1; b < lstCloudlets.size(); b++) {
	                if (lstCloudlets.get(b).getCloudletDeadline() < lstCloudlets.get(a).getCloudletDeadline()) {
	                    VideoSegment temp = lstCloudlets.get(a);
	                    lstCloudlets.set(a, lstCloudlets.get(b));
	                    lstCloudlets.set(b, temp);
	                }
	            }
	        }
			
			setCloudletList(lstCloudlets);

			/*for(VideoSegment newcl:lstNewCloudlets){
		           System.out.println("VIDEO ID: " + newcl.getCloudletVideoId() + " Cloudlet id = " + newcl.getCloudletId() + "   Deadline = " + newcl.getCloudletDeadline());

			}
			
			System.out.println("\n");*/
			
	        //Printing ordered list of cloudlets
	      /*  for (VideoSegment cl : lstCloudlets) {
	           System.out.println("VIDEO ID: " + cl.getCloudletVideoId() + " Cloudlet id = " + cl.getCloudletId() + "   Deadline = " + cl.getCloudletDeadline());
	        }*/
		}
		
	    //sorted by cloudlet's estimated shortest execution time (SJF)
		protected void SortedbySJF() {
			List<VideoSegment> lstCloudlets = getCloudletList();
			//lstCloudlets= getCloudletList();
			//setCloudletList(lstCloudlets);
			int reqTasks=lstCloudlets.size();
			int reqVms=vmList.size();
			ArrayList<Double> executionTimeList = new ArrayList<Double>();
	      //  System.out.println("\n\t PRIORITY  Broker Schedules\n");
	       // System.out.println("Before ordering");

	          for (int i=0;i<reqTasks;i++)
	          {
	            executionTimeList.add(( lstCloudlets.get(i).getCloudletLength())/ (lstCloudlets.get(i).getNumberOfPes() * vmList.get(i%reqVms).getMips()) );
	          //  System.out.println("CLOUDLET ID" + " " +lstCloudlets.get(i).getCloudletId() +" EXE TIME   " +  executionTimeList.get(i));

	          }
	             for(int i=0;i<reqTasks;i++)
	                    {
	                    for (int j=i+1;j<reqTasks;j++)
	                            {
	                            if (executionTimeList.get(i) > executionTimeList.get(j))
	                                {

	                                    VideoSegment temp1 = lstCloudlets.get(i);
	                                    lstCloudlets.set(i, lstCloudlets.get(j));
	                                    lstCloudlets.set(j, temp1);    
	                                    
	                                    double temp2 = executionTimeList.get(i);
	                                    executionTimeList.set(i, executionTimeList.get(j));
	                                    executionTimeList.set(j, temp2);

	                            }

	                            }
	                    }

	         setCloudletList(lstCloudlets);

	        /*System.out.println("After  ordering");
	         for(int i=0;i<reqTasks;i++) {
	        	 
		         System.out.println("VIDEO ID: " + lstCloudlets.get(i).getCloudletVideoId() +  " CLOUDLET ID" + " " +lstCloudlets.get(i).getCloudletId() +" EXE TIME   " +  executionTimeList.get(i));
	         }*/
			
		}
		
		

}
