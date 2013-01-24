package eu.cloudtm.wpm.logService.remote.publisher;

import java.util.Collection;
import java.util.HashMap;

import eu.cloudtm.wpm.logService.remote.events.PublishStatisticsEvent;
import eu.cloudtm.wpm.logService.remote.listeners.WPMStatisticsRemoteListener;

public class PublishStatsEventInternal {
	
	private HashMap<String, PublishStatisticsEvent> perVMEvents;
	
	private PublishStatisticsEvent perSubscriptionEvent;
	
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

	public WPMStatisticsRemoteListener getListener() {
		return listener;
	}
	
	

}
