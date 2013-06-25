package eu.cloudtm.wpm.logService.remote.events;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

import eu.cloudtm.wpm.parser.ResourceType;

/*
 * @author Sebastiano Peluso
 */
public class AggregatedPublishAttributes {
	
	private HashMap<ResourceType, PublishAttribute[]> aggregations;
	
	private long timestamp;
	
	
	public AggregatedPublishAttributes(long timestamp){
		
		this.aggregations = new HashMap<ResourceType, PublishAttribute[]>();
		
		this.timestamp = timestamp;
		
	}
	
	
	public void add(ResourceType rtype, PublishAttribute[] aggregations){
		
		this.aggregations.put(rtype, aggregations);
		
		
	}
	
	public PublishAttribute[] get(ResourceType rtype){
		
		return this.aggregations.get(rtype);
		
	}
	
	public Set<ResourceType> getResources(){
		
		return this.aggregations.keySet();
		
	}
	
	public long getTimestamp(){
		
		return this.timestamp;
		
	}
	
	public boolean hasAggregations(){
		
		Set<Entry<ResourceType, PublishAttribute[]>> set = this.aggregations.entrySet();
		
		for(Entry<ResourceType, PublishAttribute[]> e : set){
			
			if(e.getValue()!=null){
				return true;
			}
		}
		
		return false;
	}
	

}
