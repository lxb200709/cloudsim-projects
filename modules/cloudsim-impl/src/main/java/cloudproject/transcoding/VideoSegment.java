package cloudproject.transcoding;


import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModel;


public class VideoSegment extends Cloudlet{
	
	private double cloudletDeadline;
	//The time when cloudlet are created and put in the batch queue, which is different with the
	//the arrival time in ResCloudlet which is the time when cloudlet are queued in vm local queue
	private double arrivalTime;
	private double finishTime;
	private double cloudletDeadlineAfterPlay;
	private int videoId;
	private int status;
	private boolean record;
	private int std;
	private long avglength;
	
	//Create a buffer to store the video gop raw data
	//Buffer buffer = Buffer.make(null, 1000);

    
	
	public VideoSegment(
			final int cloudletId,
			final int videoId,
		    final long cloudletLength,
			final long avglength,
			final int std,
			final double arrivalTime,
		    final double cloudletDeadline,
		    final double cloudletDeadlineAfterPlay,
			final int pesNumber,
			final long cloudletFileSize,
			final long cloudletOutputSize,
			final UtilizationModel utilizationModelCpu,
			final UtilizationModel utilizationModelRam,
			final UtilizationModel utilizationModelBw) {
		super(
				cloudletId,
				cloudletLength,
				pesNumber,
				cloudletFileSize,
				cloudletOutputSize,
				utilizationModelCpu,
				utilizationModelRam,
				utilizationModelBw,
				false
				);
	   this.arrivalTime = arrivalTime;
	   this.cloudletDeadline = cloudletDeadline;
	   this.videoId = videoId;
	   this.cloudletDeadlineAfterPlay = cloudletDeadlineAfterPlay;
	   this.avglength = avglength;
	   this.std = std;
		
	}
		
	
	public long getAvgCloudletLength(){
		return avglength;
	}
	
	public long getCloudletStd(){
		return std;
	}
	
	
	public double getArrivalTime(){
		return arrivalTime;
	}
	
	public double getCloudletDeadline() {
		return cloudletDeadline;
	}
	
	public void setCloudletDeadline(double cloudletDeadline){
		this.cloudletDeadline = cloudletDeadline;
	}
	
	public double getCloudletDeadlineAfterPlay(){
		return cloudletDeadlineAfterPlay;
	}
	
	public void setCloudletDeadlineAfterPlay(double cloudletDeadlineAfterPlay){
		this.cloudletDeadlineAfterPlay = cloudletDeadlineAfterPlay;
	}
	
	public int getCloudletVideoId() {
		return videoId;
	}
		
}
