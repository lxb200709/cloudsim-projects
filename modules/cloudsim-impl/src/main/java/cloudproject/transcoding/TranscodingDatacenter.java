package cloudproject.transcoding;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.File;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicy;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEvent;

public class TranscodingDatacenter extends Datacenter {
	
	public final static int CLOUDLET_SUBMIT_RESUME = 125;
	public final static int ESTIMATED_COMPLETION_TIME = 126;
	//when a vm is sent to be destroyed, check if the cloudlets in this vm are all excuted.
	
	//private final static int waitinglist_max = 2;
	private int waitinglist_max;
	private Map<Integer, Double> totalCompletionTime_vmMap = new HashMap<Integer, Double>();
	double totalCompletionTime_vm;
	boolean initialVmTime = true;
	
	static int testCount=0;
	
	
	public TranscodingDatacenter(String name, String propertiesFileURL,
			DatacenterCharacteristics characteristics,
			VmAllocationPolicy vmAllocationPolicy, 
			List<Storage> storageList,
			double schedulingInterval) throws Exception {
		    super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
		    
		    Properties prop = new Properties();
			//InputStream input = new FileInputStream("/Users/lxb200709/Documents/TransCloud/cloudsim/modules/cloudsim-impl/config.properties");
		    InputStream input = new FileInputStream(propertiesFileURL);
		    prop.load(input);
			
			String waitinglist = prop.getProperty("waitinglist_max", "0.05");
			this.waitinglist_max = Integer.valueOf(waitinglist);
			
			
		// TODO Auto-generated constructor stub
	}
	
	
	/**
	 * @overide processCloudletSubmit at Datacenter
	 * To send signal back to broker each VM's status, including execution cloudlets and waiting cloudlets
	 */
	protected void processCloudletSubmit(SimEvent ev, boolean ack) {
		updateCloudletProcessing();
        
		try {
			
			
			EventData eventdata =(EventData) ev.getData();
			// gets the Cloudlet object
			VideoSegment cl = (VideoSegment) eventdata.getDataCloudlet();
			totalCompletionTime_vmMap = eventdata.getDataTimeMap();
			
			//for debug
			/*if(cl.getCloudletVideoId() >= 14){
				System.out.println("Test for datacenter...");
			}*/
			
			

			// checks whether this Cloudlet has finished or not
			if (cl.isFinished()) {
				String name = CloudSim.getEntityName(cl.getUserId());
				Log.printLine(getName() + ": Warning - Cloudlet #" + cl.getCloudletId() + " owned by " + name
						+ " is already completed/finished.");
				Log.printLine("Therefore, it is not being executed again");
				Log.printLine();

				// NOTE: If a Cloudlet has finished, then it won't be processed.
				// So, if ack is required, this method sends back a result.
				// If ack is not required, this method don't send back a result.
				// Hence, this might cause CloudSim to be hanged since waiting
				// for this Cloudlet back.
				if (ack) {
					int[] data = new int[3];
					data[0] = getId();
					data[1] = cl.getCloudletId();
					data[2] = CloudSimTags.FALSE;

					// unique tag = operation tag
					int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
					sendNow(cl.getUserId(), tag, data);
				}

				sendNow(cl.getUserId(), CloudSimTags.CLOUDLET_RETURN, cl);

				return;
			}

			// process this Cloudlet to this CloudResource
			cl.setResourceParameter(getId(), getCharacteristics().getCostPerSecond(), getCharacteristics()
					.getCostPerBw());

			int userId = cl.getUserId();
			int vmId = cl.getVmId();

			// time to transfer the files
			double fileTransferTime = predictFileTransferTime(cl.getRequiredFiles());

			Host host = getVmAllocationPolicy().getHost(vmId, userId);
			Vm vm = host.getVm(vmId, userId);
			VideoSchedulerSpaceShared scheduler = (VideoSchedulerSpaceShared)vm.getCloudletScheduler();
			//each cloudlet's estimated completion time
			double estimatedFinishTime = scheduler.cloudletSubmit(cl, fileTransferTime);
			//put total completion time (exec list + waiting list) of each vm in a map<Integer(vmId), Double(completion time)>
			//Map<Integer, Double> estimatedFinishTime_vm = new HashMap<Integer, Double>();
			//estimatedFinishTime_vm.put(vmId, estimatedFinishTime);
			//double totalCompletionTime_vm = 0;
			if(initialVmTime) {
				for(Vm vmlist: getVmList()){
		        	totalCompletionTime_vmMap.put(vmlist.getId(), 0.0);
		        }
				initialVmTime = false;
			}
				
			
			
			for(Integer key:totalCompletionTime_vmMap.keySet()){
				if(key == vmId) {
					totalCompletionTime_vm = totalCompletionTime_vmMap.get(key);
					totalCompletionTime_vm += estimatedFinishTime;
				}
				
			}
			totalCompletionTime_vmMap.put(vmId,totalCompletionTime_vm);

			
            //create a event to send estimatedFinishTime to broker, so that broker can choose the smallestest
			//completion time vm to recieve cloudlets.
			sendNow(userId,ESTIMATED_COMPLETION_TIME,totalCompletionTime_vmMap);
			
			List<? extends ResCloudlet> waitinglist = scheduler.getCloudletWaitingList();
			ArrayList<Integer> waitingcloudlets = new ArrayList<Integer> ();
			Cloudlet wcloudlet;
			if(waitinglist.size() >= waitinglist_max){
			//stop sending cloudlets to this vm, start to processing
				for(ResCloudlet crl:waitinglist) {
					wcloudlet = crl.getCloudlet();
					waitingcloudlets.add(wcloudlet.getCloudletId());
				}
				System.out.println(CloudSim.clock() + getId() + ":CloudletID " + waitingcloudlets + " are in VM_" + + vm.getId() + " waiting queue. STOP SENDING it cloudlets\n");
				// if this cloudlet is in the exec queue
				if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
					estimatedFinishTime += fileTransferTime;
					send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
				}
	
				if (ack) {
					int[] data = new int[3];
					data[0] = getId();
					data[1] = cl.getCloudletId();
					data[2] = CloudSimTags.TRUE;
	
					// unique tag = operation tag
					int tag = CloudSimTags.CLOUDLET_SUBMIT_ACK;
					sendNow(cl.getUserId(), tag, data);
				}
			} else { 
				for(ResCloudlet crl:waitinglist) {
					wcloudlet = crl.getCloudlet();
					waitingcloudlets.add(wcloudlet.getCloudletId());
				}
				
				// if this cloudlet is in the exec queue
				if (estimatedFinishTime > 0.0 && !Double.isInfinite(estimatedFinishTime)) {
					estimatedFinishTime += fileTransferTime;
					send(getId(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT);
				}
				//keep sending
			    System.out.println(CloudSim.clock() + getId() + ":CloudletID" + waitingcloudlets + " are in VM_" + + vm.getId() + " waiting queue. There are " + (waitinglist_max - waitinglist.size()) + " available spots in VM_" + vm.getId() +" waiting queue. KEEP SENDING it cloudlets\n");
				sendNow(userId, CLOUDLET_SUBMIT_RESUME);
			}
		} catch (ClassCastException c) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "ClassCastException error.");
			c.printStackTrace();
		} catch (Exception e) {
			Log.printLine(getName() + ".processCloudletSubmit(): " + "Exception error.");
			e.printStackTrace();
		}
        
		checkCloudletCompletion();
		
	}
	/**
	 * Predict file transfer time.
	 * 
	 * @param requiredFiles the required files
	 * @return the double
	 */
	public double predictFileTransferTime(List<String> requiredFiles) {
		double time = 0.0;

		Iterator<String> iter = requiredFiles.iterator();
		while (iter.hasNext()) {
			String fileName = iter.next();
			for (int i = 0; i < getStorageList().size(); i++) {
				Storage tempStorage = getStorageList().get(i);
				File tempFile = tempStorage.getFile(fileName);
				if (tempFile != null) {
					time += tempFile.getSize() / tempStorage.getMaxTransferRate();
					break;
				}
			}
		}
		return time;
	}
	/**
	 * Updates processing of each cloudlet running in this PowerDatacenter. It is necessary because
	 * Hosts and VirtualMachines are simple objects, not entities. So, they don't receive events and
	 * updating cloudlets inside them must be called from the outside.
	 * 
	 * @pre $none
	 * @post $none
	 */
	public void updateCloudletProcessing() {
		// if some time passed since last processing
		// R: for term is to allow loop at simulation start. Otherwise, one initial
		// simulation step is skipped and schedulers are not properly initialized
		if (CloudSim.clock() < 0.111 || CloudSim.clock() > getLastProcessTime() + CloudSim.getMinTimeBetweenEvents()) {
			List<? extends Host> list = getVmAllocationPolicy().getHostList();
			double smallerTime = Double.MAX_VALUE;
			// for each host...
			for (int i = 0; i < list.size(); i++) {
				Host host = list.get(i);
				// inform VMs to update processing
				double time = host.updateVmsProcessing(CloudSim.clock());
				// what time do we expect that the next cloudlet will finish?
				if (time < smallerTime) {
					smallerTime = time;
				}
			}
			// gurantees a minimal interval before scheduling the event
			if (smallerTime < CloudSim.clock() + CloudSim.getMinTimeBetweenEvents() + 0.01) {
				smallerTime = CloudSim.clock() + CloudSim.getMinTimeBetweenEvents() + 0.01;
			}
			if (smallerTime != Double.MAX_VALUE) {
				schedule(getId(), (smallerTime - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT);
			}
			setLastProcessTime(CloudSim.clock());
		}
	}
	
	


}
