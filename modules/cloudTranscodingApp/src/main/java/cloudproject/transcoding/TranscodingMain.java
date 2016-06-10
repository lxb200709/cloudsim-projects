package cloudproject.transcoding;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.beust.jcommander.JCommander;




/**
 * An example showing how to create
 * scalable simulations.
 */
public class TranscodingMain {
	
	/** The vmlist. */
	private static List<Vm> vmlist;
	
	
	private static int vmIdShift = 1;

	
	/**
	 * Create datatcenter
	 */
	
	private static TranscodingDatacenter datacenter;
	
	/**
	 * Calculate the total cost
	 */
	public static DatacenterCharacteristics characteristics;
	public static double memCost = 0;
	public static double storageCost = 0;
	public static double bwCost = 0;
	public static double vmCost = 0;
	public static double totalCost = 0;
	public static double totalTranscodingTime = 0;
	
	/*
	 * URL file
	 */
	public static String propertiesFileURL = null;
	public static String inputdataFolderURL = null;
	public static String outputdataFileURL = null;
	public static String sortalgorithm = null;
	public static boolean startupqueue = true;
	public static int jobNum = 0;
	public static int waitinglist_max = 0;
	public static int vmNum = 0;
	public static int frequency = 0;
	public static String estimatedGopLength = null;
	public static int seedShift = 0;
	public static double upthredshold = 0.0;
	public static double lowthredshold = 0.0;
    public static long rentingTime = 0;
    public static double testPeriod = 0.0;
    public static boolean stqprediction = true;
	
			
	//create period event
	private final static int PERIODIC_EVENT = 127;
	
	
	//All the instance share cloudletNewArrivalQueue and cloudletBatchqueue, both of them are synchronized list
	private static List<VideoSegment> cloudletNewArrivalQueue = Collections.synchronizedList(new ArrayList<VideoSegment>());
	private static List<VideoSegment> cloudletBatchQueue = Collections.synchronizedList(new ArrayList<VideoSegment>());
    public static Properties prop = new Properties();
	
	public TranscodingMain(){
		
	}
	
//////////////////////////STATIC METHODS ///////////////////////
	
	/**
	* Creates main() to run this example
	*/
	public static void main(String[] args) {
		
	/*printOutputFile pof = new printOutputFile();  
  	pof.printOutToFile("cloudTranscoding_v3"); */	
		
	Log.printLine("Starting Video Transcoding Simulation...");
	
    Log.printLine("Seting up configuration property file...\n");	
    
    /*String[] args = {"-property", "/Users/lxb200709/Documents/TransCloud/jarfiles/properties/config.properties", 
            "-input", "/Users/lxb200709/Documents/TransCloud/jarfiles/inputdata",
            "-output", "/Users/lxb200709/Documents/TransCloud/jarfiles/outputdata/test.txt", 
            "-sortalgorithm", "SDF",
            "-startupqueue",
            "-stqprediction",
            "-videonum", "500",
            "-vmqueue", "1",
            "-vmNum", "0",  
            "-vmfrequency", "10000",
            "-goplength", "AVERAGE",
            "-upthreshold", "0.10",
            "-lowthreshold", "0.05",
            "-testPeriod", "1200000",
            "-rentingTime", "100000",
            "-seedshift", "2"};*/
    
	ParseCmdLine pcl = new ParseCmdLine();
	CmdLineParser parser = new CmdLineParser(pcl);
	try {
        parser.parseArgument(args);
        //pcl.run(test);
        propertiesFileURL = pcl.getPropertiesFileURL();
		System.out.println("**Property file url: " + propertiesFileURL);
		
		inputdataFolderURL = pcl.getInputdataFolderURL();
	  	System.out.println("**Input folder url: " + inputdataFolderURL);
       
	  	outputdataFileURL = pcl.getOutputdataFileURL();
	  	System.out.println("**Output file url: " +outputdataFileURL);

	  	
	  	sortalgorithm = pcl.getSortAlgorithm();	  	
	  	startupqueue = pcl.getStarupQueue();	  	
	  	jobNum = pcl.getVideoNum();	  	
	  	waitinglist_max = pcl.getVmQueueSize();	  	
	  	vmNum = pcl.getVmNum();
	  	frequency = pcl.getVmFrequency();
	  	seedShift = pcl.getSeedShift();
	  	estimatedGopLength = pcl.getEstimatedGopLength();
	  	upthredshold = pcl.getUpThredshold();
	  	lowthredshold = pcl.getLowThreshold();
	  	rentingTime = pcl.getRentingTime();
	  	testPeriod = pcl.getTestPeriod();
	  	stqprediction = pcl.getStqPrediction();
	  	
	  	
	  	
    } catch (CmdLineException e) {
        // handling of wrong arguments
        System.err.println(e.getMessage());
        parser.printUsage(System.err);
    }
  	
	
    
    //set up configuration 
	
	OutputStream output = null;


	try {

		//output = new FileOutputStream("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudTranscodingApp/config.properties");
		output = new FileOutputStream(propertiesFileURL);
		/**
		 * Configuration properties for datacenter 
		 */
		prop.setProperty("datacenterMips", "8000");
		prop.setProperty("datacenterHostId", "0");
		prop.setProperty("datacenterRam", "16384"); //host memory 16GB
		prop.setProperty("datacenterStorage", "1048576"); //1TB
		prop.setProperty("datacenterBw", "16384"); //16GB
		
		/**
		 * Configuration properties for VM
		 */
		
		prop.setProperty("vmSize", "30720"); //30GB
		prop.setProperty("vmRam", "1024"); //vm memory(MB)
		prop.setProperty("vmMips", "8000"); 
		prop.setProperty("vmBw", "1024"); //1GB
		prop.setProperty("vmPesNumber", "1"); //number of cups
		prop.setProperty("vmName", "Xeon"); 	
		
		if(vmNum == 0){
			prop.setProperty("vmNum", "1");
			System.out.println("**The vm policy is: dynamic");
		}else{
			prop.setProperty("vmNum", String.valueOf(vmNum));
			System.out.println("**The number of Vm is: " + vmNum);
		}
		
		//Configure VM renting time
		if(rentingTime == 0){
		    prop.setProperty("rentingTime", "60000");
			System.out.println("**VM renting time is: 60000ms" );

		}else{
			prop.setProperty("rentingTime", String.valueOf(rentingTime));
			System.out.println("**VM renting time is: " + rentingTime +"ms");
		}
		
		
		/**
		 * Configuration properties for cost
		 */
		
		prop.setProperty("vmCostPerSec", "0.0000036");
		prop.setProperty("storageCostPerGb", "0.03"); 
		
		/**
		 * Configuration of GOP
		 */
		if(estimatedGopLength == null)
		    prop.setProperty("estimatedGopLength", "WORST");
		else
			prop.setProperty("estimatedGopLength", estimatedGopLength);
		
		/**
		 * Configuration properties for broker
		 */
		if(sortalgorithm == null){
		   prop.setProperty("sortalgorithm", "SDF");
		   System.out.println("**The sorting algorithm is: SDF...");
		}else{
		   prop.setProperty("sortalgorithm", sortalgorithm);
		   System.out.println("**The sorting algorithm is: " + sortalgorithm + "...");
		}
		
		if(startupqueue == true){
		    prop.setProperty("startupqueue", "true");
		    System.out.println("**The process has startup queue....");
		}else{
			prop.setProperty("startupqueue", String.valueOf(startupqueue));
			System.out.println("**The process doesn't have startup queue...");
		}
		
		if(stqprediction == true){
			prop.setProperty("stqprediction", "true");
		    System.out.println("**The process has startup queue prediction....");
		}else{
			prop.setProperty("stqprediction", "false");
			System.out.println("**The process doesn't have startup queue prediction...");
		}
		
			prop.setProperty("seedShift", String.valueOf(seedShift));
		
		
		/**
		 * configuration properties in Cooridnator
		 */
		//video job number, value i means i videos
		//File folder = new File("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudsim-impl/resources/inputdatafile"); 
		File folder = new File(inputdataFolderURL);
        File[] listOfFiles = folder.listFiles();
        int jobCount = 0;
        
        for (int i = 0; i < listOfFiles.length; i++) {
        	File inputfile = listOfFiles[i];
        	if(inputfile.isFile() && inputfile.getName().toLowerCase().endsWith(".txt")){
        		jobCount++;
        	}
        	
        }
      //  int jobNum = listOfFiles.length;
		
        if(jobNum != 0){
		   prop.setProperty("periodEventNum", String.valueOf(jobNum));
		   System.out.println("**There are " + jobNum + " videos...");
        }else{
 		   prop.setProperty("periodEventNum", String.valueOf(jobCount));
		   System.out.println("**There are " + jobCount + " videos...");


        }
        
        
		//check vm provision frequence
        if(frequency ==0 ){
		   prop.setProperty("periodicDelay", "1000");	
		   System.out.println("**Vm checking frequency is: 1000ms");

        }else{
           prop.setProperty("periodicDelay", String.valueOf(frequency));
		   System.out.println("**Vm checking frequency is: " + frequency);

        }
        
        //configure test time period
        
        if(testPeriod == 0.0){
        	prop.setProperty("testPeriod", "1200000");
        	System.out.println("**Test Period is: 1200000");
        }else{
        	prop.setProperty("testPeriod", String.valueOf(testPeriod));
        	System.out.println("**Test Period is: " + testPeriod + "ms");
        }
        
        
        
		//The maximum number of vm a user can create
		prop.setProperty("MAX_VM_NUM", "100");
		
		/**
		 * configuration properties in broker and datacenter's VM local queue size
		 */
		
		if(waitinglist_max == 0){
		    prop.setProperty("waitinglist_max", "2");
			System.out.println("**Vm local waiting queue size is: 2");
		}else{
			prop.setProperty("waitinglist_max", String.valueOf(waitinglist_max));
			System.out.println("**Vm local waiting queue size is: "+ waitinglist_max);

		}

		/**
		 * configuration properties for transcoding provisioning 
		 */
		if(upthredshold == 0){
		    prop.setProperty("DEADLINE_MISS_RATE_UPTH", "0.1");
		    System.out.println("**deadline miss rate upthredshold is: 0.1");
		}else{
			prop.setProperty("DEADLINE_MISS_RATE_UPTH", String.valueOf(upthredshold));
		    System.out.println("**deadline miss rate upthredshold is: " + upthredshold);

		}
		
		if(lowthredshold == 0){
		    prop.setProperty("DEADLINE_MISS_RATE_LOWTH", "0.05");
		    System.out.println("**deadline miss rate lowthredshold is: 0.05");
		    
		}else{
			prop.setProperty("DEADLINE_MISS_RATE_LOWTH", String.valueOf(lowthredshold));
		    System.out.println("**deadline miss rate lowthredshold is: " + lowthredshold);

		}
		prop.setProperty("ARRIVAL_RATE_TH", "0.5");
		
		System.out.println('\n');

		// save properties to project root folder
		prop.store(output, null);
		
		

	} catch (IOException io) {
		io.printStackTrace();
	} finally {
		if (output != null) {
			try {
				output.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
	
	try {
		// First step: Initialize the CloudSim package. It should be called
		// before creating any entities.
		int num_user = 1;   // number of grid users
		Calendar calendar = Calendar.getInstance();
		boolean trace_flag = false;  // mean trace events
		
		// Initialize the CloudSim library
		CloudSim.init(num_user, calendar, trace_flag);
		
		// Second step: Create Datacenters
		//Datacenters are the resource providers in CloudSim. We need at list one of them to run a CloudSim simulation
		@SuppressWarnings("unused")
		TranscodingDatacenter datacenter0 = createDatacenter("Datacenter_0");
		
		//choose datacenter0 as the datacenter to create new vm, which can improve later
		datacenter = datacenter0;
		
		@SuppressWarnings("unused")
	//	TranscodingDatacenter datacenter1 = createDatacenter("Datacenter_1");
		
		//Third step: Create Broker and VideoStreams
		TranscodingMain ct = new TranscodingMain();
        TranscodingMain.Coordinator coordinator = ct.new Coordinator("Coordinator");	
		//TranscodingMain.Coordinator coordinator = new Coordinator("Coordinator");	
		
		
       /* //Fourth Step: Create a initial VM list
		vmlist = createVM(coordinator.getBroker().getId(), 2); //creating vms
		coordinator.getBroker().submitVmList(vmlist);*/

		double startTranscodingTime = System.currentTimeMillis();
		// Fifth step: Starts the simulation
		CloudSim.startSimulation();
		
		totalTranscodingTime = (System.currentTimeMillis() - startTranscodingTime)/1000;
		
		// Final step: Print results when simulation is over
		List<VideoSegment> newList = coordinator.getBroker().getCloudletSubmittedList();
				
		Map<Integer, Double> videoStartupTimeMap = coordinator.getBroker().getVideoStartupTimeMap();
		
		vmCost = coordinator.getBroker().getVmCost();
		storageCost = getStorageCost(newList);
		
		calculateTotalCost(storageCost, vmCost);	
		
		printVideoStatistic(videoStartupTimeMap, newList);
				
		printCloudletList(newList);
		
		
		CloudSim.stopSimulation();
		 
		
		
		Log.printLine("Video Transcoding Simulation Finished!");
		}
	catch (Exception e)
		{
		e.printStackTrace();
		Log.printLine("The simulation has been terminated due to an unexpected error");
		}
    }
	
	
	
	/**
	 * Create VMs
	 * @param userId
	 * @param vms
	 * @return
	 */
 
	private static List<TranscodingVm> createVM(int userId, int vms, int idShift, long rentingTime) {

		//Creates a container to store VMs. This list is passed to the broker later
		LinkedList<TranscodingVm> list = new LinkedList<TranscodingVm>();

		//VM Parameters
		/*long size = 20000; //image size (MB)
		int ram = 1024; //vm memory (MB)
		int mips = 8000;
		long bw = 1000;
		int pesNumber = 1; //number of cpus
		String vmm = "Xeon"; //VMM name
*/		
		
		String sizeStr = prop.getProperty("vmSize", "30720");
		String ramStr = prop.getProperty("vmRam", "1024");
		String mipsStr = prop.getProperty("vmMips", "8000");
		String bwStr = prop.getProperty("vmBw", "1000");
		String pesNumberStr = prop.getProperty("vmPesNumber", "1");
		String vmm = prop.getProperty("vmName", "Xeon");
		
		long size = Long.valueOf(sizeStr);
		int ram = Integer.valueOf(ramStr);
		int mips = Integer.valueOf(mipsStr);
		long bw = Long.valueOf(bwStr);
		int pesNumber = Integer.valueOf(pesNumberStr);
		
		
		//memCost += characteristics.getCostPerMem()*ram;
		
		//Whenver ther is a Vm created, add storage cost.
		//storageCost += characteristics.getCostPerStorage()*size/1024;

		//create VMs
		TranscodingVm[] vm = new TranscodingVm[vms];

		for(int i=0;i<vms;i++){
			//vm[i] = new Vm(i, userId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
			//for creating a VM with a space shared scheduling policy for cloudlets:
			
			//always run vm#0
			if(idShift == 0){
				vm[i] = new TranscodingVm(i+idShift, userId, mips, pesNumber, ram, bw, size, Long.MAX_VALUE, vmm, new VideoSchedulerSpaceShared());

			}else{
			
		    	vm[i] = new TranscodingVm(i+idShift, userId, mips, pesNumber, ram, bw, size, rentingTime, vmm, new VideoSchedulerSpaceShared());
			}
			list.add(vm[i]);
		}

		return list;
	}
    
	private static double getBwCost(List<VideoSegment> list){
		for(VideoSegment cl:list){
			bwCost += cl.getProcessingCost();
		}
		return bwCost;
	}
	
	private static double getStorageCost(List<VideoSegment> list){
		double byteConvertToGb = 1024*1024*1024;
		double storageSize =0;
		for(VideoSegment cl:list){
			storageSize += cl.getCloudletFileSize();
			storageSize += cl.getCloudletOutputSize();
			
			/*storageCost += cl.getCloudletFileSize()/byteConvertToGb*characteristics.getCostPerStorage();
			storageCost += cl.getCloudletOutputSize()/byteConvertToGb*characteristics.getCostPerStorage();*/
		}
		System.out.println("The storage size is: " + new DecimalFormat("#0.00").format(storageSize));
		storageCost = storageSize/byteConvertToGb*characteristics.getCostPerStorage();
		
		return storageCost;
	}
	
	private static void calculateTotalCost(double storageCost, double vmCost){
		
		//totalCost = storageCost + vmCost;
		totalCost = vmCost;
		System.out.println("\n");
		System.out.println("*******The storage cost is: " + new DecimalFormat("#0.0000").format(storageCost));
		System.out.println("*******The time cost is: " + new DecimalFormat("#0.0000").format(vmCost));
		System.out.println("*******The total cost is: " + new DecimalFormat("#0.0000").format(totalCost));
		
		//return totalCost;
	}
    
	
    
	
	
	
	private static TranscodingDatacenter createDatacenter(String name){

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store one or more
		//    Machines
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores. Therefore, should
		//    create a list to store these PEs before creating
		//    a Machine.
		List<Pe> peList1 = new ArrayList<Pe>();

		//int mips = 8000;
		String mipsStr = prop.getProperty("datacenterMips", "8000");
        int mips = Integer.valueOf(mipsStr);

		// 3. Create PEs and add these into the list.
		//for a quad-core machine, a list of 4 PEs is required:
		peList1.add(new Pe(0, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
		peList1.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(4, new PeProvisionerSimple(mips))); // need to store Pe id and MIPS Rating
		peList1.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList1.add(new Pe(7, new PeProvisionerSimple(mips)));

		//Another list, for a dual-core machine
		List<Pe> peList2 = new ArrayList<Pe>();

		peList2.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(4, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList2.add(new Pe(7, new PeProvisionerSimple(mips)));
		
		List<Pe> peList3 = new ArrayList<Pe>();

		peList3.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(4, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList3.add(new Pe(7, new PeProvisionerSimple(mips)));
		
		List<Pe> peList4 = new ArrayList<Pe>();

		peList4.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(4, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList4.add(new Pe(7, new PeProvisionerSimple(mips)));
		
		List<Pe> peList5 = new ArrayList<Pe>();

		peList5.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(4, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList5.add(new Pe(7, new PeProvisionerSimple(mips)));
		
		List<Pe> peList6 = new ArrayList<Pe>();

		peList6.add(new Pe(0, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(1, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(2, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(3, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(4, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(5, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(6, new PeProvisionerSimple(mips)));
		peList6.add(new Pe(7, new PeProvisionerSimple(mips)));

		//4. Create Hosts with its id and list of PEs and add them to the list of machines
		/*int hostId=0;
		int ram = 16384; //host memory (MB)
		long storage = 1000000; //host storage
		int bw = 10000;*/
		
		prop.setProperty("datacenterMips", "8000");
		prop.setProperty("datacenterHostId", "0");
		prop.setProperty("datacenterRam", "131072"); //host memory 128GB
		prop.setProperty("datacenterStorage", "1048576"); //1TB
		prop.setProperty("datacenterBw", "131072"); //128GB
		
		String storageStr = prop.getProperty("datacenterStorage", "1048576");
		String ramStr = prop.getProperty("datacenterRam", "131072");
		String bwStr = prop.getProperty("datacenterBw", "131072"); //16GB
		String hostIdStr = prop.getProperty("datacenterHostId", "0");
		
		long storage = Long.valueOf(storageStr);
		int ram = Integer.valueOf(ramStr);
		long bw = Long.valueOf(bwStr);
		int hostId = Integer.valueOf(hostIdStr);
		

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList1,
    				new VmSchedulerTimeShared(peList1)
    			)
    		); // This is our first machine

		hostId++;

		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList2,
    				new VmSchedulerTimeShared(peList2)
    			)
    		); // Second machine

		hostId++; 
		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList3,
    				new VmSchedulerTimeShared(peList3)
    			)
    		);
		
		hostId++; 
		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList4,
    				new VmSchedulerTimeShared(peList4)
    			)
    		);
		
		hostId++; 
		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList5,
    				new VmSchedulerTimeShared(peList5)
    			)
    		);
		
		hostId++; 
		hostList.add(
    			new Host(
    				hostId,
    				new RamProvisionerSimple(ram),
    				new BwProvisionerSimple(bw),
    				storage,
    				peList6,
    				new VmSchedulerTimeShared(peList6)
    			)
    		);
		
		//To create a host with a space-shared allocation policy for PEs to VMs:
		//hostList.add(
    	//		new Host(
    	//			hostId,
    	//			new CpuProvisionerSimple(peList1),
    	//			new RamProvisionerSimple(ram),
    	//			new BwProvisionerSimple(bw),
    	//			storage,
    	//			new VmSchedulerSpaceShared(peList1)
    	//		)
    	//	);

		//To create a host with a oportunistic space-shared allocation policy for PEs to VMs:
		//hostList.add(
    	//		new Host(
    	//			hostId,
    	//			new CpuProvisionerSimple(peList1),
    	//			new RamProvisionerSimple(ram),
    	//			new BwProvisionerSimple(bw),
    	//			storage,
    	//			new VmSchedulerOportunisticSpaceShared(peList1)
    	//		)
    	//	);


		// 5. Create a DatacenterCharacteristics object that stores the
		//    properties of a data center: architecture, OS, list of
		//    Machines, allocation policy: time- or space-shared, time zone
		//    and its price (G$/Pe time unit).
		String arch = "x86";      // system architecture
		String os = "Linux";          // operating system
		String vmm = "Xeon";
		
		String vmCostStr = prop.getProperty("vmCostPerSec", "0.0000036");
		String storageCostStr = prop.getProperty("storageCostPerGb", "0.03");
		
		double cost = Double.valueOf(vmCostStr);
		double costPerStorage = Double.valueOf(storageCostStr);
		
		double time_zone = 10.0;         // time zone this resource located
		//double cost = 0.013/3600;              // the cost of using processing in this resource
		double costPerMem = 0.05;		// the cost of using memory in this resource
		//double costPerStorage = 0.1;	// the cost of using storage in this resource
		double costPerBw = 0.1;			// the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>();	//we are not adding SAN devices by now

		/*DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);*/
		
	   characteristics = new DatacenterCharacteristics(arch, os, vmm, hostList, time_zone, cost, costPerMem, costPerStorage, costPerBw);


		// 6. Finally, we need to create a PowerDatacenter object.
		TranscodingDatacenter datacenter = null;
		try {
			datacenter = new TranscodingDatacenter(name, propertiesFileURL, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	//We strongly encourage users to develop their own broker policies, to submit vms and cloudlets according
	//to the specific rules of the simulated scenario
	private static TranscodingBroker createBroker(){

		TranscodingBroker broker = null;
		try {
			broker = new TranscodingBroker("TranscodingBroker", characteristics, propertiesFileURL);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
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
	/**
	 * Print out each video's start up time and average startup time
	 * @param videoStartupTimeMap
	 * @throws IOException 
	 */
	
	private static void printVideoStatistic(Map<Integer, Double> videoStartupTimeMap, List<VideoSegment> list) throws IOException{
		System.out.println("\n");

		double videoStartupTimeAverage=0;
		int videoCount =0;
		int size = list.size();
		VideoSegment cloudlet;
		Map<Integer, Integer> videoGopNumMap = new HashMap<Integer,Integer>();
		Map<Integer, Integer> videoDeadlineMissNumMap = new HashMap<Integer, Integer>();
		Map<Integer, Double> videoDeadlineMissRateMap = new HashMap<Integer, Double>();

		
		int deadlineMissCount = 0;

		for(VideoSegment cl:list){
			if(!videoDeadlineMissNumMap.containsKey(cl.getCloudletVideoId())){
				
				videoGopNumMap.put(cl.getCloudletVideoId(), 1);
				videoDeadlineMissNumMap.put(cl.getCloudletVideoId(), 0);
				if(cl.getCloudletDeadline() < cl.getFinishTime()){
					videoDeadlineMissNumMap.put(cl.getCloudletVideoId(), videoDeadlineMissNumMap.get(cl.getCloudletVideoId())+1);
			    }

			}else{
				videoGopNumMap.put(cl.getCloudletVideoId(), videoGopNumMap.get(cl.getCloudletVideoId())+1);
				if(cl.getCloudletDeadline() < cl.getFinishTime()){
					videoDeadlineMissNumMap.put(cl.getCloudletVideoId(), videoDeadlineMissNumMap.get(cl.getCloudletVideoId())+1);
			    }
			}
			
		  			
		}
		
		for(Map.Entry<Integer, Integer> entry:videoGopNumMap.entrySet()){
			double videoDeadlineMissNum = videoDeadlineMissNumMap.get(entry.getKey());
			double videoGopNum = videoGopNumMap.get(entry.getKey());
			double videoDeadlineMissRate = videoDeadlineMissNum/videoGopNum;
			videoDeadlineMissRateMap.put(entry.getKey(), videoDeadlineMissRate);
		}

		
		//PrintWriter pw = new PrintWriter("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudTranscodingApp/resources/outputdata/video_startup_time.txt");
		PrintWriter pw = new PrintWriter(new FileWriter(outputdataFileURL, true));
		//pw.printf("%-16s%-16s%-25s", "VideoID", "Startup Time", "Deadline Miss Rate");
		//pw.println("\n");
		
		for(Integer vId:videoStartupTimeMap.keySet()){
		//	System.out.println("VideoId#" + vId + "'s start up time is: " + new DecimalFormat("#0.00").format(videoStartupTimeMap.get(vId)));
			videoStartupTimeAverage += videoStartupTimeMap.get(vId);
			videoCount++;
		//	pw.printf("%-16d%-16.2f%-25.4f", vId, videoStartupTimeMap.get(vId), videoDeadlineMissRateMap.get(vId));
		//	pw.println("\n");

			
		}
		
		//pw.close();
		double totalAverageStartupTime = videoStartupTimeAverage/videoCount;
		System.out.println("Video average start up time is: "+ new DecimalFormat("#0.0000").format(totalAverageStartupTime));
		
		double deadlineMissNum = 0;
		double totalDeadlineMissRate;
		
		for(VideoSegment cl:list){
		    if(cl.getCloudletDeadline() < cl.getFinishTime()){
		    	deadlineMissNum++;
		    }
		}
		
		totalDeadlineMissRate = deadlineMissNum/list.size();
		
		if(startupqueue){
		
			/**
			 * ouput with new arrival queuue 
			 */
			if(vmNum == 0){
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25s%-20s%-12s%-12s%-12s%-12s", "Startup Queue", "Sorting", "Video Numbers", "VM Number ", "VQS", "Average Startup Time", "Deadline Miss Rate", "Total Cost", "Total Time", "DMR_UPTH", "DMR_LTH");
				pw.println("\n");
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25.2f%-20.4f%-12.4f%-12.4f%-12.4f%-12.4f", "YES", prop.getProperty("sortalgorithm", "SDF"), prop.getProperty("periodEventNum", "2"), "Dynamic", prop.getProperty("waitinglist_max", "2"), totalAverageStartupTime, totalDeadlineMissRate, totalCost, totalTranscodingTime, Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.10")), Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.05")));
				
				pw.println("\n");
			}else{
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25s%-20s%-12s%-12s%-12s%-12s", "Startup Queue", "Sorting", "Video Numbers", "VM Number ", "VQS", "Average Startup Time", "Deadline Miss Rate", "Total Cost", "Total Time", "DMR_UPTH", "DMR_LTH");
				pw.println("\n");
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25.2f%-20.4f%-12.4f%-12.4f%-12.4f%-12.4f", "YES", prop.getProperty("sortalgorithm", "SDF"), prop.getProperty("periodEventNum", "2"), prop.getProperty("vmNum", "2"), prop.getProperty("waitinglist_max", "2"), totalAverageStartupTime, totalDeadlineMissRate, totalCost, totalTranscodingTime, Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.10")), Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.05")));
				
				pw.println("\n");
				
			}
		
		}else{
			
			/**
			 * ouput without new arrival queuue 
			 */
			
			if(vmNum == 0){
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25s%-20s%-12s%-12s%-12s%-12s", "Startup Queue", "Sorting", "Video Numbers", "VM Number ", "VQS", "Average Startup Time", "Deadline Miss Rate", "Total Cost", "Total Time", "DMR_UPTH", "DMR_LTH");
				pw.println("\n");
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25.2f%-20.4f%-12.4f%-12.4f%-12.4f%-12.4f", "NO", prop.getProperty("sortalgorithm", "SDF"), prop.getProperty("periodEventNum", "2"), "Dynamic", prop.getProperty("waitinglist_max", "2"), totalAverageStartupTime, totalDeadlineMissRate, totalCost,totalTranscodingTime, Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.10")), Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.05")));
				
				pw.println("\n");
			}else{
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25s%-20s%-12s%-12s%-12s%-12s", "Startup Queue", "Sorting", "Video Numbers", "VM Number ", "VQS", "Average Startup Time", "Deadline Miss Rate", "Total Cost", "Total Time", "DMR_UPTH", "DMR_LTH");
				pw.println("\n");
				pw.printf("%-18s%-10s%-20s%-16s%-10s%-25.2f%-20.4f%-12.4f%-12.4f%-12.4f%-12.4f", "NO", prop.getProperty("sortalgorithm", "SDF"), prop.getProperty("periodEventNum", "2"), prop.getProperty("vmNum", "2"), prop.getProperty("waitinglist_max", "2"), totalAverageStartupTime, totalDeadlineMissRate, totalCost, totalTranscodingTime, Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.10")), Double.valueOf(prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.05")));
				
				pw.println("\n");
				
			}
			
		}
		
		pw.close();
	}

	/**
	 * Prints the Cloudlet objects
	 * @param list  list of Cloudlets
	 * @throws Exception 
	 */
	private static void printCloudletList(List<VideoSegment> list) throws Exception {
		int size = list.size();
		VideoSegment cloudlet;
		int deadlineMissCount = 0;
		double totalDeadlineMissRate;
		
		for(VideoSegment cl:list){
		    if(cl.getCloudletDeadline() < cl.getFinishTime()){
		    	deadlineMissCount++;
		    }
		}
		/*System.out.println("\nThere are: " + list.size() + " cloudlets...");
		System.out.println("\nThere are: " + deadlineMissCount + " cloudlets missed deadline...");*/
		
		totalDeadlineMissRate = (double) deadlineMissCount/list.size();
        
		System.out.println("\nThe total deadline miss rate is: " + new DecimalFormat("#0.0000").format(totalDeadlineMissRate));
		
		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		/*Log.printLine("Video ID" + indent +
				      "Cloudlet ID" + indent + "STATUS" + indent + "Data center ID" + indent + 
				      "VM ID" + indent + indent + 
				      "Arrival Time"+ indent + indent + 
				      "Start Exec Time" + indent + indent +
				      "Exec Time" + indent  + indent + 
				      "Finish Time" + indent  + indent +
				      "Deadline");*/
        System.out.format("%-18s%-18s%-18s%-18s%-18s%-18s%-18s%-18s%-18s%-18s", "Video ID","Cloudlet ID",
        		"STATUS", "Data center ID", "VM ID", "Arrival Time",  "Start Exec Time", "Exec Time","Finish Time","Deadline");
        System.out.println("\n");
		
		//DecimalFormat dft = new DecimalFormat("###.##");
		DecimalFormat dft = new DecimalFormat("###");
		for (int i = 0; i < size; i++) {
			cloudlet = (VideoSegment) list.get(i);
		//	Log.print(indent + cloudlet.getCloudletVideoId() + indent + indent + indent + cloudlet.getCloudletId() + indent + indent);
			
			if (cloudlet.getCloudletStatus() == VideoSegment.SUCCESS){
				/*Log.print("SUCCESS");

				Log.printLine( 
						indent + indent + cloudlet.getResourceId() + 
						indent + indent + indent + indent + cloudlet.getVmId() + 
						indent + indent + indent + indent + dft.format(cloudlet.getArrivalTime()) + 
						indent + indent + indent + indent + dft.format(cloudlet.getExecStartTime()) +
						indent + indent + indent + indent + indent + dft.format(cloudlet.getActualCPUTime()) +
					    indent + indent + indent + indent + dft.format(cloudlet.getFinishTime()) +
					    indent + indent + indent + indent + dft.format(cloudlet.getCloudletDeadline()));*/
				
	            System.out.format("%-18d%-18d%-18s%-18d%-18d%-18.2f%-18.2f%-18.2f%-18.2f%-18.2f", cloudlet.getCloudletVideoId(), cloudlet.getCloudletId(), "SUCCESS", 
	            		cloudlet.getResourceId(), cloudlet.getVmId(), cloudlet.getArrivalTime(), cloudlet.getExecStartTime(), cloudlet.getActualCPUTime(),
	            		cloudlet.getFinishTime(),cloudlet.getCloudletDeadline());

			}
		}
		
		System.out.println("There are " + size + " cloudlets are processed.");

	}
	
	
	/**
	 * Coordinator class is used to control the whole cloud system:
	 * 1. Reading New Video streams
	 * 2. Split Video Streams
	 * 3. Create Broker
	 * 4. Based VMProvision allocate and deallocate VMs.
	 * 
	 * @author Bill
	 *
	 */
	
	public class Coordinator extends SimEntity {
	    
		private static final int CREATE_BROKER = 150;
	 
		private static final int PERIODIC_UPDATE = 153;
		
		private static final int CREATE_JOB = 155;
		
		private int MAX_VM_NUM;
		
		
		
		public int videoId = 0;
		
		private int periodCount = 0;
		
		//Max jobs we are about to create
		//private final static int periodEventNum = 50;
		private int periodEventNum;
		
	   // private final static int periodicDelay = 20000; //contains the delay to the next periodic event
		
		private int periodicDelay;
	 //   public boolean generatePeriodicEvent = true; //true if new internal events have to be generated

		
		private List<TranscodingVm> vmList;
		private List<VideoSegment> cloudletList;
		private TranscodingBroker broker;
		
	    private int jobCount = 0;
	    private int previousJobCount = 0;
		private double jobDelay = 0.0;
		private int vmNumber = 0;
		private int seedShift = 0;
		private String estimatedGopLength = null;
		private double DEADLINE_MISS_RATE_UPTH = 0.0;
		private double DEADLINE_MISS_RATE_LOWTH = 0.0;
		private double testPeriod = 0.0;
		private long rentingTime = 0;
		
		private boolean prediction = true;
		private boolean stqprediction = true;

		
		//private Properties prop = new Properties();
		
		

		public Coordinator(String name) throws IOException {
			super(name);
			//this.prop = prop;
			
			//InputStream input = new FileInputStream("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudsim-impl/config.properties");
			InputStream input = new FileInputStream(propertiesFileURL);
			prop.load(input);
			
			String jobNum;
			String frequence;
			String MAX_VM_NUM;
			String vmNumber;
			String seedShift;
			String upTh; 
			String lowTh;
			String testPeriod;
			String rentingTime;
			String stqprediction;
			
			
			jobNum = prop.getProperty("periodEventNum");
			this.periodEventNum = Integer.valueOf(jobNum);
			
			frequence = prop.getProperty("periodicDelay");
			this.periodicDelay = Integer.valueOf(frequence);
			
			MAX_VM_NUM = prop.getProperty("MAX_VM_NUM", "16");
			this.MAX_VM_NUM = Integer.valueOf(MAX_VM_NUM);
			
			vmNumber = prop.getProperty("vmNum");
			this.vmNumber = Integer.valueOf(vmNumber);
			
			seedShift = prop.getProperty("seedShift");
			this.seedShift = Integer.valueOf(seedShift);
			
		    upTh = prop.getProperty("DEADLINE_MISS_RATE_UPTH", "0.2");
			this.DEADLINE_MISS_RATE_UPTH = Double.valueOf(upTh);
			
			lowTh = prop.getProperty("DEADLINE_MISS_RATE_LOWTH", "0.01");
			this.DEADLINE_MISS_RATE_LOWTH = Double.valueOf(lowTh);
			
			testPeriod = prop.getProperty("testPeriod", "1800000");
			this.testPeriod = Double.valueOf(testPeriod);
			
			rentingTime = prop.getProperty("rentingTime", "60000");
			this.rentingTime = Long.valueOf(rentingTime);
			
			stqprediction = prop.getProperty("stqprediction", "true");
			this.stqprediction = Boolean.valueOf(stqprediction);

			
			estimatedGopLength = prop.getProperty("estimatedGopLength");
			
		}

		@Override
		public void processEvent(SimEvent ev) {
			switch (ev.getTag()) {
			case CREATE_BROKER:
			    setBroker(createBroker());
			    //create and submit intial vm list
			    
			    if(vmNum == 0){
			        setVmList(createVM(broker.getId(), 1, 0, Long.MAX_VALUE));
			    }else{
			        setVmList(createVM(broker.getId(), vmNumber, 0, Long.MAX_VALUE));
			    }
			    
				broker.submitVmList(getVmList());
			    
				break;
			
			case CREATE_JOB:
				processNewJobs();
				break;
		
			case PERIODIC_UPDATE:				
				processProvisioning();
                break;
				
			default:
				Log.printLine(getName() + ": unknown event type");
				break;
			}
		}

		@Override
		/**
		 * Open video stream arrival rate file and scheduling the initial tasks of creates jobs and VMs
		 */
		public void startEntity() {
			Log.printLine(getName()+" is starting...");
			schedule(getId(), 0, CREATE_BROKER);
			schedule(getId(), 1, CREATE_JOB);
			
			send(getId(), periodicDelay, PERIODIC_UPDATE);
		}

		@Override
		/**
		 * Close all the files readers that has been opened in the simulation
		 */
		public void shutdownEntity() {
			Log.printLine(getName()+" is shuting down...");

			
		}
		
		/**
		 * Periodically process new jobs
		 */
		
		public void processNewJobs(){
				System.out.println(CloudSim.clock() + " : Creating a new job....");
				
				int brokerId = getBroker().getId();
				
				
				// Create a thread pool that can create video streams
				
		 		Random random = new Random();
	
				//List<Cloudlet> newList = new ArrayList<Cloudlet>();
	            
		 		
		 		
		 		
				ExecutorService executorService = Executors.newFixedThreadPool(2);
				 
		        CompletionService<String> taskCompletionService = new ExecutorCompletionService<String>(
		                executorService);
		 	        
		        try {
		        	
		        	ArrayList<Callable<String>> callables = new ArrayList<Callable<String>>();
		            for (int i = 0; i < 1; i++) {
		            	int cloudlets = (int)getRandomNumber(10, 50, random);
		            	  callables.add(new VideoStreams("" + videoId, inputdataFolderURL, startupqueue, estimatedGopLength, seedShift, brokerId, videoId, cloudlets));
			           // Thread.sleep(1000);
		                videoId++;
		            }
		            
		            
		            
		            
		           // List<Callable<String>> callables = createCallableList();
		            for (Callable<String> callable : callables) {
		                taskCompletionService.submit(callable);     
		               // Thread.sleep(500);
		            }
		            
		           // Thread.sleep(1000);
						            
		            
		            for (int i = 0; i < callables.size(); i++) {
		                Future<String> result = taskCompletionService.take(); 
		                System.out.println(result.get() + " End."); 
		            }
		        } catch (InterruptedException e) {
		            // no real error handling. Don't do this in production!
		            e.printStackTrace();
		        } catch (ExecutionException e) {
		            // no real error handling. Don't do this in production!
		            e.printStackTrace();
		        }
		        executorService.shutdown();
		      
			    VideoStreams vt = new VideoStreams();
			    cloudletBatchQueue = vt.getBatchQueue();
			    cloudletNewArrivalQueue = vt.getNewArrivalQueue();
			
			    //update the cloudlet list and back to simulation
			    broker.submitCloudletList(cloudletBatchQueue, cloudletNewArrivalQueue);
			    
			  //  broker.submitVmList(getVmList());
			    			    
			    /**
			     * check startup queue length to provision vm
			     */
			   if(vmNum == 0 && stqprediction){ // only for dynamci cloud resources
			    	List<Integer> videoIdList = new ArrayList<Integer>();
	                int videoNum = 0;
	                int vmToBeCreated = 0;
	                double val = 0.0;
				    for(Cloudlet cl:broker.getCloudletNewList()){
				    	VideoSegment vs = (VideoSegment)cl;
				    	if(!videoIdList.contains(vs.getCloudletVideoId())){
			    			videoNum ++;
			    			videoIdList.add(vs.getCloudletVideoId());
			    			
			    		}else{
			    			continue;
			    		} 		
				    }
				    
				    val =(videoNum-1)/(DEADLINE_MISS_RATE_UPTH*10);
				    
				    vmToBeCreated = (int)val;
				    
				    provisonVM(vmToBeCreated);
			   }
			 
			    broker.submitCloudlets();
			    
			    
			    //Check if there are more jobs to process
		    if (broker.generatePeriodicEvent){

			    jobCount++;
			    if(jobCount < periodEventNum){
			    	Random r = new Random(jobCount);
			        
				  //  jobDelay = (int)getRandomNumber(3000, 3000, random);	
			    	/*jobDelay = 180000.00/periodEventNum;
			    	send(getId(),jobDelay,CREATE_JOB);*/

			    	
			    	jobDelay = testPeriod/periodEventNum;

			    	
			    	double val = r.nextGaussian()*(jobDelay/3);
			    	double stdJobDelay = val + jobDelay;
				    				    
					send(getId(),stdJobDelay,CREATE_JOB);
					
					
			    	
			    }else{
			    	broker.generatePeriodicEvent = false;	
	
			    }
		     }   
				//periodCount++;
			    
			    CloudSim.resumeSimulation();
			
	 
		}
		
		/**
		 * Periodically check Resource Provisioning,and update VMs 
		 */
		
		public void processProvisioning(){
			/**
			 * Dynamic Vm policy
			 * 
			 */
			
			
			if(vmNum == 0 && !prediction){
				/**
				 * no prediction mode
				 */
	 
				System.out.println(CloudSim.clock() + ": " + "Upating Transcoding provisioning...");
				
				//check vm remaining time;
				for(Vm vm_rt : datacenter.getVmList()){
					TranscodingVm vmm_rt = (TranscodingVm) vm_rt;
					if(vmm_rt.getRemainingTime() <= 0){
					//	System.out.println(CloudSim.clock() + "Vm#: " +  vmm_rt.getId() +" has beyoned renting time, is about to deallocate...");
						vmm_rt.setDeallocationFlag(true);
					}
				}
					
					
				TranscodingProvisioner tp;
				try {
					tp = new TranscodingProvisioner(broker, propertiesFileURL);
					int allcoationDeallocationFlag;
					allcoationDeallocationFlag = tp.allocateDeallocateVm();
					
					System.out.println("The VMList size is: " + datacenter.getVmList().size());
					System.out.println("The Created VMList size is: " + broker.getVmsCreatedList().size());
	
					//1 means allocate new a vm, 2 means deallocate a vm
					if(allcoationDeallocationFlag == 1 ){
						if(datacenter.getVmList().size() < MAX_VM_NUM) {
							//Create a new vm
							List<TranscodingVm> vmNew = (List<TranscodingVm>) createVM(broker.getId(), 1, vmIdShift, rentingTime);
			                vmIdShift++;
			                
			                //submit it to broker
							broker.submitVmList(vmNew);
							
							//creat a event for datacenter to create a vm
							sendNow(datacenter.getId(),CloudSimTags.VM_CREATE_ACK, vmNew.get(0));
						}else{
							System.out.println("\n******The account has hit the maximum vm that can created*****");
						}
						
					}else if(allcoationDeallocationFlag == 3){
						if(datacenter.getVmList().size() < MAX_VM_NUM) {
							//Create two new vms
							System.out.println("creating two vms...");
							List<TranscodingVm> vmNew1 = (List<TranscodingVm>) createVM(broker.getId(), 1, vmIdShift, rentingTime);
			                vmIdShift++;
			                
			                //submit it to broker
							broker.submitVmList(vmNew1);
							
							//creat a event for datacenter to create a vm
							sendNow(datacenter.getId(),CloudSimTags.VM_CREATE_ACK, vmNew1.get(0));
							
							List<TranscodingVm> vmNew2 = (List<TranscodingVm>) createVM(broker.getId(), 1, vmIdShift, rentingTime);
			                vmIdShift++;
			                
			                //submit it to broker
							broker.submitVmList(vmNew2);
							
							//creat a event for datacenter to create a vm
							sendNow(datacenter.getId(),CloudSimTags.VM_CREATE_ACK, vmNew2.get(0));
							
							
							
						}else{
							System.out.println("\n******The account has hit the maximum vm that can created*****");
						}
						
					}else if(allcoationDeallocationFlag == 2){
						int vmIndex = 0;
						int runningVmNum = 0;
						for(Vm vm : datacenter.getVmList()){
							TranscodingVm vmm = (TranscodingVm) vm;
							
							if(vmm.getRemainingTime() >= 0){
								runningVmNum ++;
							}
						}
						
						
						if(datacenter.getVmList().size() > 1 && runningVmNum >1){
							
							//Looking for the smallest completion time VM to be destroyed. 
							/*Map.Entry<Integer, Double> minCompletionTime_vm = null;
							for (Map.Entry<Integer, Double> entry : broker.totalCompletionTime_vmMap.entrySet()) {
							    if (minCompletionTime_vm == null || minCompletionTime_vm.getValue() > entry.getValue()) {
							        minCompletionTime_vm = entry;
							    }
							}
							if(minCompletionTime_vm == null){
								vmIndex = 0;
							}else {
						        vmIndex = minCompletionTime_vm.getKey();
							}*/
							
							//find the minimum remaining time vm to deallocate
							
							
							double minRemainingTime = 0;
							
							
							
							for(Vm vm : datacenter.getVmList()){
								TranscodingVm vmm = (TranscodingVm) vm;
								VideoSchedulerSpaceShared vmcsch = (VideoSchedulerSpaceShared) vmm.getCloudletScheduler();

								/*System.out.println(CloudSim.clock() + "VM#: " + vmm.getId() + "'s renting time until " + vmm.getRentingTime() + 
										 "...The remaining time is: " + vmm.getRemainingTime());*/								
								if(vmm.getRemainingTime() > 0){
									if(minRemainingTime == 0 || vmm.getRemainingTime() < minRemainingTime){
										minRemainingTime = vmm.getRemainingTime();
										vmIndex = vmm.getId();
									
									}
								}else if(vmm.getRemainingTime() <= 0 && vmcsch.getCloudletExecList().size() == 0 && vmcsch.getCloudletWaitingList().size() == 0 && vmcsch.getCloudletPausedList().size() == 0){
							    	//System.out.println(CloudSim.clock() + "\n********************VM_" + vmm.getId() + "'s renting time is over and to be destroyed***********************" );
									sendNow(broker.getVmsToDatacentersMap().get(vmm.getId()), CloudSimTags.VM_DESTROY, vmm);
					                broker.vmDestroyedList.add(vmm);
					                
					                //set Vm's finish time.
					                vmm.setVmFinishTime(CloudSim.clock());
					    	      
					                //Calculate vm cost based on the time it last.
					                broker.setVmCost(vmm);
					                
								}else{
									continue;
								}
								
							}	
							
							System.out.println("\n");
							TranscodingVm vmToBeDestroy = (TranscodingVm) broker.getVmList().get(vmIndex);
						//	System.out.println(CloudSim.clock() + "VM#: " + vmToBeDestroy.getId() + " is abou to be destroy...\n");
							//Set this vm toBedestroyed flag as true, so that it won't send new cloudlets to this vm again in the future.
							if(!vmToBeDestroy.getDeallocationFlag()) {
								
								vmToBeDestroy.setDeallocationFlag(true);
		
							}
							
						}else{
							System.out.println("\n******The account has hit the minimul vm that can have*****");
						}
					}
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0 && jobCount == periodEventNum) {
				    return;
				}else{
				    send(getId(),periodicDelay,PERIODIC_UPDATE);
	
			    }
			}else if(vmNum == 0 && prediction){
				/**
				 * prediction mode
				 */
				
                System.out.println(CloudSim.clock() + ": " + "Upating Transcoding provisioning...");
				
				//check vm remaining time;
				for(Vm vm_rt : datacenter.getVmList()){
					TranscodingVm vmm_rt = (TranscodingVm) vm_rt;
					if(vmm_rt.getRemainingTime() <= 0){
					//	System.out.println(CloudSim.clock() + "Vm#: " +  vmm_rt.getId() +" has beyoned renting time, is about to deallocate...");
						vmm_rt.setDeallocationFlag(true);
					}
				}
				
                long vmToBeCreated = 0;
                double val = 0.0;
                double futureDmr = 0.0;
                double currentDmr = 0.0;
                double dmrVariation = 0.0;
                double k;
                double arrivalRate = 0.0;
                
                
                
                TranscodingProvisioner tp;
                try{
                	
                	tp = new TranscodingProvisioner(broker, propertiesFileURL);
                	
					currentDmr = tp.getDeadlineMissRate();                	
					futureDmr = tp.getEstimatedDeadlineMissRate() ;
					dmrVariation = futureDmr - currentDmr;
					
					arrivalRate = (jobCount - previousJobCount)*1000.0/periodicDelay;
                	previousJobCount = jobCount;
					
					//If the deadline miss rate of next period of time is higher the upth, and the deadline miss rate
					//compared to current on is increasing, allocate VMs
                    if(dmrVariation >= 0.2 || futureDmr > DEADLINE_MISS_RATE_UPTH){ 
                    	                   	
                    	if(arrivalRate < 1){
                    		k =0.5;
                    	}else{
                    		k = 1;
                    	}
                    	
                    	
                    	System.out.println("\nThe arrival rate is: " + k);
                    	
                        val =k*futureDmr/DEADLINE_MISS_RATE_UPTH;      			    
					    vmToBeCreated = Math.round(val);
					    provisonVM(vmToBeCreated);
					    
				    //If the deadline miss rate of next period of time is lower the lowth, and the deadline miss rate
					//compared to current on is decreasing, allocate VMs    
                    }else if(dmrVariation <= -0.2 || futureDmr < DEADLINE_MISS_RATE_LOWTH){ //Deallocate VMs
                    	int vmIndex = 0;
						int runningVmNum = 0;
						for(Vm vm : datacenter.getVmList()){
							TranscodingVm vmm = (TranscodingVm) vm;
							
							if(vmm.getRemainingTime() >= 0){
								runningVmNum ++;
							}
						}
						
						
						if(datacenter.getVmList().size() > 1 && runningVmNum >1){
							
							
							//find the minimum remaining time vm to deallocate
							
							
							double minRemainingTime = 0;
							
							
							
							for(Vm vm : datacenter.getVmList()){
								TranscodingVm vmm = (TranscodingVm) vm;
								VideoSchedulerSpaceShared vmcsch = (VideoSchedulerSpaceShared) vmm.getCloudletScheduler();

								System.out.println(CloudSim.clock() + "VM#: " + vmm.getId() + "'s renting time until " + vmm.getRentingTime() + 
										 "...The remaining time is: " + vmm.getRemainingTime());							
								if(vmm.getRemainingTime() > 0){
									if(minRemainingTime == 0 || vmm.getRemainingTime() < minRemainingTime){
										minRemainingTime = vmm.getRemainingTime();
										vmIndex = vmm.getId();
									
									}
								}else if(vmm.getRemainingTime() <= 0 && vmcsch.getCloudletExecList().size() == 0 && vmcsch.getCloudletWaitingList().size() == 0 && vmcsch.getCloudletPausedList().size() == 0){
							    	System.out.println(CloudSim.clock() + "\n********************VM_" + vmm.getId() + "'s renting time is over and to be destroyed***********************" );
									sendNow(broker.getVmsToDatacentersMap().get(vmm.getId()), CloudSimTags.VM_DESTROY, vmm);
					                broker.vmDestroyedList.add(vmm);
					                
					                //set Vm's finish time.
					                vmm.setVmFinishTime(CloudSim.clock());
					    	      
					                //Calculate vm cost based on the time it last.
					                broker.setVmCost(vmm);
					                
								}else{
									continue;
								}
								
							}	
							
							System.out.println("\n");
							TranscodingVm vmToBeDestroy = (TranscodingVm) broker.getVmList().get(vmIndex);
						//	System.out.println(CloudSim.clock() + "VM#: " + vmToBeDestroy.getId() + " is abou to be destroy...\n");
							//Set this vm toBedestroyed flag as true, so that it won't send new cloudlets to this vm again in the future.
							if(!vmToBeDestroy.getDeallocationFlag()) {
								
								vmToBeDestroy.setDeallocationFlag(true);
		
							}else{
								System.out.println("\n******The account has hit the minimul vm that can have*****");
							}
						}
                    	
                    }
                    
                    if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0 && jobCount == periodEventNum) {
    				    return;
    				}else{
    				    send(getId(),periodicDelay,PERIODIC_UPDATE);
    	
    			    }
    			    
    			    
                }catch(IOException e){
                
					// TODO Auto-generated catch block
					e.printStackTrace();
    				
                }
                
               
				
			}else{
				/**
				 * fixed vm policy
				 */
				return;
			}
		    
		}
		
		
		/**
		 * Allocate VMs based on Startup Queue Length
		 */
		public void provisonVM(long vmNum){
			System.out.println("\n**creating "+ vmNum +" vms...\n");
			
			//test
			if(vmNum >3){
				System.out.println("test");
			}
			
			for(int i=0; i<vmNum; i++){
				
				List<TranscodingVm> vmNew = (List<TranscodingVm>) createVM(broker.getId(), 1, vmIdShift, rentingTime);
	            vmIdShift++;
	            
	            //submit it to broker
				broker.submitVmList(vmNew);
				
				//creat a event for datacenter to create a vm
				sendNow(datacenter.getId(),CloudSimTags.VM_CREATE_ACK, vmNew.get(0));
			}
			
		}
		
		
		/**
		 * Create a periodic event for process new jobs and update resource provisioning
		 * periodEventNumb is the number of periodic events
		 * @throws IOException 
		 * 
		 */
		
		public void periodicUpdate(){
					
			/*if (broker.generatePeriodicEvent)  {
				//processNewJobs();
				int jobDelay = 0;
				for(int i=0; i<periodEventNum; i++){
					Random random = new Random();
				    jobDelay += (int)getRandomNumber(1000, 5000, random);	
					send(getId(),jobDelay,CREATE_JOB);
					
				}
				//periodCount++;
		    	broker.generatePeriodicEvent = false;

				
		    }
			//Set the the number of periodic event to create new job, so that it won't run forever.
		    if (periodCount < periodEventNum) {
		    	  broker.generatePeriodicEvent = true;
		    	  
		    }else{
		    	  broker.generatePeriodicEvent = false;
		    }
		      
		    */
          
		   //when cloudlets are finished, stop periodic event
		  //  if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0) {
		    /*if (broker.getCloudletList().size() == 0 && broker.getCloudletNewList().size() == 0 && broker.cloudletSubmittedCount == 0 ) {
		    	return;
		    }
		    
			processProvisioning();*/
		  //  send(getId(),periodicDelay,PERIODIC_UPDATE);
		}
        
		
		public List<TranscodingVm> getVmList() {
			return vmList;
		}

		protected void setVmList(List<TranscodingVm> vmList) {
			this.vmList = vmList;
		}

		public List<VideoSegment> getCloudletList() {
			return cloudletList;
		}

		protected void setCloudletList(List<VideoSegment> cloudletList) {
			this.cloudletList = cloudletList;
		}

		public TranscodingBroker getBroker() {
			return broker;
		}

		public void setBroker(TranscodingBroker broker) {
			this.broker = broker;
		}
		
		public int  getJobCount(){
			return jobCount;
		}
		
		public double  getJobDelay(){
			return jobDelay;
		}
		
		

	}
}
