/*
 * CINI, Consorzio Interuniversitario Nazionale per l'Informatica
 * Copyright 2013 CINI and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
 
package eu.cloudtm.wpm.logService.remote.publisher;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import eu.cloudtm.wpm.logService.remote.events.AggregatedPublishAttributes;
import eu.cloudtm.wpm.logService.remote.events.PublishAggregatedStatisticsEvent;
import eu.cloudtm.wpm.logService.remote.events.PublishAttribute;
import eu.cloudtm.wpm.logService.remote.events.PublishStatisticsEvent;
import eu.cloudtm.wpm.logService.remote.listeners.WPMStatisticsRemoteListener;
import eu.cloudtm.wpm.parser.ResourceType;

/*
* @author Sebastiano Peluso
*/
public class PublishStatsEventInternal {
	
	private HashMap<String, PublishStatisticsEvent> perVMEvents;
	
	private PublishStatisticsEvent perSubscriptionEvent;
	
	private PublishAggregatedStatisticsEvent aggregatedStatsEvent;
	
	private WPMStatisticsRemoteListener listener;
	
	public PublishStatsEventInternal(WPMStatisticsRemoteListener listener){
		
		perVMEvents = new HashMap<String, PublishStatisticsEvent>();
		
		
		this.listener = listener;
	}

	public void pushPerSubscriptionPublishStatisticsEvent(
			PublishStatisticsEvent pe) {
		
	
		this.perSubscriptionEvent = pe;
		
	}

	public void pushPerVMPublishStatisticsEvent(String ip,
			PublishStatisticsEvent pe) {
		
		
		
		this.perVMEvents.put(ip, pe);
		
	}
	
	public Collection<PublishStatisticsEvent> getPerVMEvents(){
		
		return this.perVMEvents.values();
	}
	
	public PublishStatisticsEvent getPerSubscriptionEvent(){
		return this.perSubscriptionEvent;
	}
	
	public PublishAggregatedStatisticsEvent getAggregatedStatisticsEvent(){
		return this.aggregatedStatsEvent;
	}

	public WPMStatisticsRemoteListener getListener() {
		return listener;
	}
	
	
	public void addAggregations(AggregatedPublishAttributes aggregations){
		if(aggregations!=null && aggregations.hasAggregations()){
			this.aggregatedStatsEvent = new PublishAggregatedStatisticsEvent();
			
			Set<ResourceType> resources = aggregations.getResources();
			PublishAttribute[] attributes;
			for(ResourceType r : resources){
				
				attributes = aggregations.get(r);
				
				if(attributes != null){
					this.aggregatedStatsEvent.addMeasure(aggregations.getTimestamp(), r, attributes);
				}
				
			}
		}
	}
	
	

}
