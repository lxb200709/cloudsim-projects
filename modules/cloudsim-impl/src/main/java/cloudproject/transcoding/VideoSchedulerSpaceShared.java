package cloudproject.transcoding;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.Consts;
import org.cloudbus.cloudsim.ResCloudlet;
import org.cloudbus.cloudsim.core.CloudSim;


/**
 * VideoCloudletScheduler implements a policy of scheduling performed by a virtual machine. It
 * consider that there will be only one cloudlet per VM. Other cloudlets will be in a waiting list.
 * We consider that file transfer from cloudlets waiting happens before cloudlet execution. I.e.,
 * even though cloudlets must wait for CPU, data transfer happens as soon as cloudlets are
 * submitted.
 * 
 * @author Rodrigo N. Calheiros
 * @author Anton Beloglazov
 * @since CloudSim Toolkit 1.0
 */
public class VideoSchedulerSpaceShared extends CloudletSchedulerSpaceShared {
	


	public VideoSchedulerSpaceShared(){
		super();
	}
	
	/**
	 * Updates the processing of cloudlets running under management of this scheduler.
	 * 
	 * @param currentTime current simulation time
	 * @param mipsShare array with MIPS share of each processor available to the scheduler
	 * @pre currentTime >= 0
	 * @post $none
	 */
	@Override
    public double updateVmProcessing(double currentTime, List<Double> mipsShare) {
	    Cloudlet cloudlet;
	    
	    
		setCurrentMipsShare(mipsShare);
		double timeSpam = currentTime - getPreviousTime(); // time since last update
		double capacity = 0.0;
		int cpus = 0;

		for (Double mips : mipsShare) { // count the CPUs available to the VMM
			capacity += mips;
			if (mips > 0) {
				cpus++;
			}
		}
		currentCpus = cpus;
		capacity /= cpus; // average capacity of each cpu

		// each machine in the exec list has the same amount of cpu
		for (ResCloudlet rcl : getCloudletExecList()) {
			rcl.updateCloudletFinishedSoFar((long) (capacity * timeSpam * rcl.getNumberOfPes() * Consts.MILLION));
		}

		// no more cloudlets in this scheduler
		if (getCloudletExecList().size() == 0 && getCloudletWaitingList().size() == 0) {
			setPreviousTime(currentTime);
			return 0.0;
		}

		// update each cloudlet
		int finished = 0;
		List<ResCloudlet> toRemove = new ArrayList<ResCloudlet>();
		for (ResCloudlet rcl : getCloudletExecList()) {

			// finished anyway, rounding issue...
			if (rcl.getRemainingCloudletLength() == 0) {
				toRemove.add(rcl);
				cloudletFinish(rcl);
				finished++;
			}
		}
		getCloudletExecList().removeAll(toRemove);
		
		//TEST the size of WatingList size @Bill
	//	System.out.println("The size of CloudletWaitingList is " + getCloudletWaitingList().size());

		// for each finished cloudlet, add a new one from the waiting list
		if (!getCloudletWaitingList().isEmpty()) {
			for (int i = 0; i < finished; i++) {
				toRemove.clear();
				for (ResCloudlet rcl : getCloudletWaitingList()) {
					if ((currentCpus - usedPes) >= rcl.getNumberOfPes()) {
						rcl.setCloudletStatus(Cloudlet.INEXEC);
						for (int k = 0; k < rcl.getNumberOfPes(); k++) {
							rcl.setMachineAndPeId(0, i);
						}
						getCloudletExecList().add(rcl);
						usedPes += rcl.getNumberOfPes();
						toRemove.add(rcl);
						break;
					}
				}
				getCloudletWaitingList().removeAll(toRemove);
			}
		}

		// estimate finish time of cloudlets in the execution queue
		double nextEvent = Double.MAX_VALUE;
		for (ResCloudlet rcl : getCloudletExecList()) {
			double remainingLength = rcl.getRemainingCloudletLength();
			double estimatedFinishTime = currentTime + (remainingLength / (capacity * rcl.getNumberOfPes()));
			if (estimatedFinishTime - currentTime < CloudSim.getMinTimeBetweenEvents()) {
				estimatedFinishTime = currentTime + CloudSim.getMinTimeBetweenEvents();
			}
			if (estimatedFinishTime < nextEvent) {
				nextEvent = estimatedFinishTime;
			}
		}
		setPreviousTime(currentTime);
		return nextEvent;
	}
	
	/**
	 * Receives an cloudlet to be executed in the VM managed by this scheduler.
	 * 
	 * @param cloudlet the submited cloudlet
	 * @param fileTransferTime time required to move the required files from the SAN to the VM
	 * @return expected finish time of this cloudlet, or 0 if it is in the waiting queue
	 * @pre gl != null
	 * @post $none
	 */
	@Override
	public double cloudletSubmit(Cloudlet cloudlet, double fileTransferTime) {
		// it can go to the exec list
		if ((currentCpus - usedPes) >= cloudlet.getNumberOfPes()) {
			ResCloudlet rcl = new ResCloudlet(cloudlet);
			rcl.setCloudletStatus(Cloudlet.INEXEC);
			for (int i = 0; i < cloudlet.getNumberOfPes(); i++) {
				rcl.setMachineAndPeId(0, i);
			}
			getCloudletExecList().add(rcl);
			usedPes += cloudlet.getNumberOfPes();
		} else {// no enough free PEs: go to the waiting queue
			ResCloudlet rcl = new ResCloudlet(cloudlet);
			rcl.setCloudletStatus(Cloudlet.QUEUED);
			getCloudletWaitingList().add(rcl);
            
			
			//return 0.0;
		}

		// calculate the expected time for cloudlet completion
		double capacity = 0.0;
		int cpus = 0;
		for (Double mips : getCurrentMipsShare()) {
			capacity += mips;
			if (mips > 0) {
				cpus++;
			}
		}

		currentCpus = cpus;
		capacity /= cpus;

		// use the current capacity to estimate the extra amount of
		// time to file transferring. It must be added to the cloudlet length
		double extraSize = capacity * fileTransferTime;
		long length = cloudlet.getCloudletLength();
		length += extraSize;
		cloudlet.setCloudletLength(length);
		return cloudlet.getCloudletLength() / capacity;
	}

	
	/**
	 * Gets the cloudlet waiting list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet waiting list
	 */
	public <T extends ResCloudlet> List<T> getCloudletWaitingList() {
		return super.getCloudletWaitingList();
	}
    
	/**
	 * Cloudlet waiting list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletWaitingList the cloudlet waiting list
	 */
	public <T extends ResCloudlet> void cloudletWaitingList(List<T> cloudletWaitingList) {
		super.setCloudletWaitingList(cloudletWaitingList);
	}

	/**
	 * Gets the cloudlet exec list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet exec list
	 */
	@SuppressWarnings("unchecked")
	public <T extends ResCloudlet> List<T> getCloudletExecList() {
		return super.getCloudletExecList();
	}

	/**
	 * Sets the cloudlet exec list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletExecList the new cloudlet exec list
	 */
	public <T extends ResCloudlet> void setCloudletExecList(List<T> cloudletExecList) {
		super.setCloudletExecList(cloudletExecList);
	}

	/**
	 * Gets the cloudlet paused list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet paused list
	 */
	@SuppressWarnings("unchecked")
	public <T extends ResCloudlet> List<T> getCloudletPausedList() {
		return super.getCloudletPausedList();
	}

	/**
	 * Sets the cloudlet paused list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletPausedList the new cloudlet paused list
	 */
	public<T extends ResCloudlet> void setCloudletPausedList(List<T> cloudletPausedList) {
		super.setCloudletPausedList(cloudletPausedList);
	}

	/**
	 * Gets the cloudlet finished list.
	 * 
	 * @param <T> the generic type
	 * @return the cloudlet finished list
	 */
	@SuppressWarnings("unchecked")
	public <T extends ResCloudlet> List<T> getCloudletFinishedList() {
		return super.getCloudletFinishedList();
	}

	/**
	 * Sets the cloudlet finished list.
	 * 
	 * @param <T> the generic type
	 * @param cloudletFinishedList the new cloudlet finished list
	 */
	public <T extends ResCloudlet> void setCloudletFinishedList(List<T> cloudletFinishedList) {
		super.setCloudletFinishedList(cloudletFinishedList);
	}

	

}
