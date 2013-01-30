package eu.cloudtm.wpm.logService.remote.observables;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import eu.cloudtm.wpm.logService.remote.events.PublishAttribute;
import eu.cloudtm.wpm.logService.remote.events.PublishStatisticsEvent;
import eu.cloudtm.wpm.logService.remote.listeners.WPMStatisticsRemoteListener;
import eu.cloudtm.wpm.logService.remote.publisher.PublishStatsEventInternal;
import eu.cloudtm.wpm.parser.Measurement;
import eu.cloudtm.wpm.parser.MeasurementAttribute;
import eu.cloudtm.wpm.parser.ResourceType;

public class StatsSubscriptionEntry {
	
	private Handle handle;
	
	private String[] ips;
	
	private Measurement[] measurements;
			
    private boolean[] ready;
    
    private boolean[] changed;
	
	private WPMStatisticsRemoteListener remoteListener;
	
	public StatsSubscriptionEntry(Handle handle, String[] ips, WPMStatisticsRemoteListener remoteListener){
		
		this.handle = handle;
		
		this.ips = ips;
		
		this.remoteListener = remoteListener;
		
		this.ready = new boolean[(ips.length) * ResourceType.values().length];
		
		this.changed = new boolean[(ips.length) * ResourceType.values().length];
		
		this.measurements = new Measurement[(ips.length) * ResourceType.values().length]; //We want a measure for each resource type of each machine
		
		
		for(int i = 0; i< ready.length; i++){
			this.ready[i] = false;
			this.changed[i] = false;
			this.measurements [i] = null;
		}
		
	}
	
	
	
	
	public Handle getHandle() {
		return handle;
	}




	public WPMStatisticsRemoteListener getListener() {
		return remoteListener;
	}


	public void addMeasurement(Measurement measurement){
		
		int index;
		
		if(measurement!=null){
			
			for(int i = 0; i < ips.length; i++){
				
				if(ips[i].equals(measurement.getIp())){
					
					index = (i*ResourceType.values().length)+measurement.getResourceType().ordinal();
					
					this.measurements[index] = measurement;
					
					this.ready[index] = true;

					this.changed[index] = true;
					
					
				}
				
			}
			
			
		}
		
	}
	
	
	public PublishStatsEventInternal computePublishStatsEventInternal(){
		
		int numResourceTypes = ResourceType.values().length;
		
		boolean allReady = true;
		boolean oneVMReady;
		
		int currentIndex;
		
		PublishStatsEventInternal pei = new PublishStatsEventInternal(this.remoteListener);
		
		for(int i=0; i < ips.length; i++){
			
			oneVMReady = true;
			
			for(int j=0; j<numResourceTypes; j++){
				
				currentIndex = (i*numResourceTypes)+j;
				
				if(!changed[currentIndex]){
					oneVMReady = false;
				}
				
				if(!ready[currentIndex]){
					allReady=false;
				}
				
				
			}
			
			if(oneVMReady){//Get statistics for this VM an reset flag changed
				
				String[] eventIps = new String[1];
				eventIps[0] = ips[i];
				
				PublishStatisticsEvent pe = new PublishStatisticsEvent(eventIps);
				
				
				for(int j=0; j<numResourceTypes; j++){
					
					currentIndex = (i*numResourceTypes)+j;
					
					Measurement m = this.measurements[currentIndex];
					
					List<PublishAttribute> pa = getPublishAttributes(m);
					
					pe.addMeasure(ips[i], m.getGroup_ID(), m.getProvider_ID(), m.getTimestamp(), m.getResourceType(), pa);
					
					changed[currentIndex] = false;
					
				}
				
				
				pei.pushPerVMPublishStatisticsEvent(ips[i], pe);
				
				
			}
			
			
		}
		
		
		if(allReady){
			
			//This event matches the whole subscription
			
			PublishStatisticsEvent pe = new PublishStatisticsEvent(ips);
			
			for(int i=0; i < ips.length; i++){
				
			
				
				for(int j=0; j<numResourceTypes; j++){
					
					currentIndex = (i*numResourceTypes)+j;
					
					Measurement m = this.measurements[currentIndex];
					
					List<PublishAttribute> pa = getPublishAttributes(m);
					
					pe.addMeasure(ips[i], m.getGroup_ID(), m.getProvider_ID(), m.getTimestamp(), m.getResourceType(), pa);
					
					
					this.ready[currentIndex] = false;
					
				}
			}	
			
			pei.pushPerSubscriptionPublishStatisticsEvent(pe);
		}
		
		
		
		return pei;
		
		
		
	}
	
	
	
	
	private static List<PublishAttribute> getPublishAttributes(Measurement m){
		
		List<PublishAttribute> result= new LinkedList<PublishAttribute>();
		
		if(m!=null){
			
			ArrayList<MeasurementAttribute> ma = m.getMeasurementAttributes();
			
			ResourceType rt = m.getResourceType();
			
			if(ma != null){
				
				Iterator<MeasurementAttribute> itr = ma.iterator();
				MeasurementAttribute current;
				
				while(itr.hasNext()){
					
					current = itr.next();
					
					if("DOUBLE".equals(current.getJava_type())){
						
						
						result.add(new PublishAttribute<Double>(rt, current.getResource_index(), current.getShort_name(), Double.parseDouble(current.getValue())));
						
					}
					else if("INTEGER".equals(current.getJava_type())){
						result.add(new PublishAttribute<Integer>(rt, current.getResource_index(), current.getShort_name(), Integer.parseInt(current.getValue())));
					}
					else if("STRING".equals(current.getJava_type())){
						result.add(new PublishAttribute<String>(rt, current.getResource_index(), current.getShort_name(), current.getValue()));
					}
					else if("FLOAT".equals(current.getJava_type())){
						result.add(new PublishAttribute<Float>(rt, current.getResource_index(), current.getShort_name(), Float.parseFloat(current.getValue())));
					}
					
				}
			}
			
		}
		
		
		return result;
		
	}

}
