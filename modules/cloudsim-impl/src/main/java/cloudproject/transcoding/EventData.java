package cloudproject.transcoding;

import java.util.Map;

import org.cloudbus.cloudsim.Cloudlet;

public class EventData {
	private Cloudlet datacloudlet;
	private Map<Integer, Double> dataTotalCompletionTime_vmMap;
	
	public EventData(Cloudlet cloudlet, Map<Integer, Double> totalCompletionTime_vmMap){
		this.datacloudlet = cloudlet;
		this.dataTotalCompletionTime_vmMap = totalCompletionTime_vmMap;
	}
	
	public Cloudlet getDataCloudlet() {
		return datacloudlet;
	}
	
	public Map<Integer, Double> getDataTimeMap() {
		return dataTotalCompletionTime_vmMap;
	}
	
}
