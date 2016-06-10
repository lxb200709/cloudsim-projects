package cloudproject.transcoding;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class ParseData {
	private ArrayList<Integer> gopIdList = new ArrayList<Integer>();
	private ArrayList<Integer> gopTranscodingTimeList = new ArrayList<Integer>();
	private ArrayList<Integer> deviationGopTranscodingTimeList = new ArrayList<Integer>();
	private ArrayList<Integer> gopPtsList = new ArrayList<Integer>();
	private ArrayList<Integer> gopInputSizeList = new ArrayList<Integer>();
	private ArrayList<Integer> gopOutputSizeList = new ArrayList<Integer>();
	
	private ArrayList<Integer> stdList = new ArrayList<Integer>();

	static int variance = 0;
	
	
	public ParseData(){
		
	}
	
	//get gop id list
    public ArrayList<Integer> getGopIdList(){
    	return gopIdList;
    }
	
    //get gop transcoding time
    public ArrayList<Integer> getGopTranscodingTimeList(){
    	return gopTranscodingTimeList;
    }

    //get gop pts
    public ArrayList<Integer> getGopPtsList(){
    	return gopPtsList;
    }
    
    //get gop input size
    public ArrayList<Integer> getGopInputSizeList(){
    	return gopInputSizeList;
    }
    
   //get gop input size
    public ArrayList<Integer> getGopOutputSizeList(){
    	return gopOutputSizeList;
    }
    
    public void parseData(File file){
		BufferedReader br = null;
       
		 try {
				//File file = new File(inputUrl);
		//		System.out.println(file.getAbsolutePath());
				FileReader fr = new FileReader(file);
		        br = new BufferedReader(fr);
		        
		        String sCurrentLine;
		        String resolution = "Resolution";
		        boolean flag = true;
		        Map<Integer, ArrayList<Integer>> addupGopTranscodingTimeMap = new HashMap<Integer, ArrayList<Integer>>();
		       
		        
		       int averageGopTranscodingTime = 0;
		       int averageGopPts = 0;
		       int averageGopInputSize = 0;
		       int averageGopOutputSize = 0;
		        
		       ArrayList<Integer> recordGopTranscodingTimeList = new ArrayList<Integer>();
		       
		        
		        
		        
		        
		        int i=0;
		        int gopId = 0;
		        int fileCount = 0;
		        
		        while ((sCurrentLine = br.readLine()) != null) {
		            //skip blank rows
		            if(sCurrentLine.length() > 0) {
			            String[] arr = sCurrentLine.split("\\s+");
			            if(arr[0].equals(resolution)){
			            	fileCount++;
 	            		}
			           
			            if(i > 0){
			            	if(arr[0].equals(resolution)){
		            			flag = false;
		            		}
			            	if(flag){
				            	gopIdList.add(Integer.parseInt(arr[1]));
				            	gopTranscodingTimeList.add(Integer.parseInt(arr[2]));
				            	gopPtsList.add(Integer.parseInt(arr[3]));
				            	gopInputSizeList.add(Integer.parseInt(arr[4]));
				            	gopOutputSizeList.add(Integer.parseInt(arr[5]));   	
				            	
		            			recordGopTranscodingTimeList.add(Integer.parseInt(arr[2]));

				            	
			            	}else{
			            		if(arr[0].equals(resolution)){
			         		        ArrayList<Integer> tempTecordGopTranscodingTimeList = new ArrayList<Integer>();
			         		        tempTecordGopTranscodingTimeList.addAll(recordGopTranscodingTimeList);
			            			addupGopTranscodingTimeMap.put(fileCount - 1, tempTecordGopTranscodingTimeList);
			            			recordGopTranscodingTimeList.clear();
			            			gopId = 0;
			            			continue;
			            		}else{
			            			
			            			
			            			recordGopTranscodingTimeList.add(Integer.parseInt(arr[2]));
			            			
			            			
			            			averageGopTranscodingTime = gopTranscodingTimeList.get(gopId) + Integer.parseInt(arr[2]);
			            			averageGopPts = gopPtsList.get(gopId) + Integer.parseInt(arr[3]);
			            			averageGopInputSize = gopInputSizeList.get(gopId) + Integer.parseInt(arr[4]);
			            			averageGopOutputSize = gopOutputSizeList.get(gopId) + Integer.parseInt(arr[5]);
			            			
			            			gopTranscodingTimeList.set(gopId, averageGopTranscodingTime);
			            			gopPtsList.set(gopId, averageGopPts);
			            			gopInputSizeList.set(gopId, averageGopInputSize);
			            			gopOutputSizeList.set(gopId, averageGopOutputSize);
			            			gopId++;
		
 			            		}
			            		
			            	}
			            	
			            	
			            }
			            i++;
		            }
		            
    	
		        }
		        
		        
    			addupGopTranscodingTimeMap.put(fileCount, recordGopTranscodingTimeList);

		        
		        //calculate the standard deviation
		        
		        
		        
		        for(int id=0; id<getGopIdList().size(); id++){
		            variance = 0;
		        	
			        averageGopTranscodingTime = gopTranscodingTimeList.get(id)/fileCount;
	    			averageGopPts = gopPtsList.get(id)/fileCount;
	    			averageGopInputSize = gopInputSizeList.get(id)/fileCount;
	    			averageGopOutputSize = gopOutputSizeList.get(id)/fileCount;
	    			
	    			for(Integer fileId:addupGopTranscodingTimeMap.keySet()){
	    				int att = addupGopTranscodingTimeMap.get(fileId).get(id);
		    			variance += (Math.pow(att - averageGopTranscodingTime, 2) / fileCount);		    			
	    			}
	    			
	    			stdList.add((int) Math.sqrt(variance));
	    			
	    			gopTranscodingTimeList.set(id, averageGopTranscodingTime);
	    			deviationGopTranscodingTimeList.add(averageGopTranscodingTime + stdList.get(id));
	    			
	    			gopPtsList.set(id, averageGopPts);
	    			gopInputSizeList.set(id, averageGopInputSize);
	    			gopOutputSizeList.set(id, averageGopOutputSize);
		        }
		        
		        
		        
		        
		 } catch (IOException e) {
		        e.printStackTrace();
		 } finally {
		       try {
		            if (br != null)br.close();
		        } catch (IOException ex) {
		            ex.printStackTrace();
		        }
		    }
    	
     }
    
    public ArrayList<Integer> getStdList(){
    	return stdList;
    }
    
    
    public ArrayList<Integer> getDeviationGopTranscodingTimeList(){
    	return deviationGopTranscodingTimeList;
    }
    
    public void listFilesForFolder(final File folder) {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                System.out.println(fileEntry.getName());
            }
        }
    }
    
   /*public static void main(String arg[]){
	   
	  printOutputFile pof = new printOutputFile();  
	  pof.printOutToFile("AnalyseData");  
	   
	  File folder = new File("/Users/lxb200709/Documents/TransCloud/amazon_aws/outputdata"); 
	  
	  File[] listOfFiles = folder.listFiles();
	  
	  for (int i = 0; i < listOfFiles.length; i++) {
		  File file = listOfFiles[i];
		  if (file.isFile() && file.getName().endsWith(".txt")) {
			//  System.out.println(file.getName());
			  
			  ParseData pd = new ParseData();
			  pd.parseData(file);   
              
			  System.out.println("\n\n************" + file.getName() + "**********");
			  for(Integer id:pd.getGopIdList()){
				   System.out.println("Gop#" + id + "  Average Transcoding Time: " + pd.getGopTranscodingTimeList().get(id-1) + 
						   "   Standard Deviation: " + pd.getStdList().get(id-1) + "   Final Transcoding Time: " + pd.getDeviationGopTranscodingTimeList().get(id-1));
				   
			   }
		  } 
	  }	   	   
	   
   }*/
    
    
    
}

