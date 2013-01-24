package eu.cloudtm.wpm.logService.remote.publisher;

import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import eu.cloudtm.wpm.logService.remote.events.PublishAttribute;
import eu.cloudtm.wpm.logService.remote.events.PublishMeasurement;
import eu.cloudtm.wpm.logService.remote.events.PublishStatisticsEvent;
import eu.cloudtm.wpm.logService.remote.observables.WPMObservableImpl;
import eu.cloudtm.wpm.parser.ResourceType;

public class PublisherStatsThread extends Thread{

	private PublishStatsEventInternal event; 
	
	private WPMObservableImpl observableImpl;

	public PublisherStatsThread(PublishStatsEventInternal event, WPMObservableImpl observableImpl){

		this.event = event;
		this.observableImpl = observableImpl;
	}

	public void run(){

		Collection<PublishStatisticsEvent> perVM = event.getPerVMEvents();

		if(perVM != null){

			for(PublishStatisticsEvent pe: perVM){

				try {
					event.getListener().onNewPerVMStatistics(pe);
				} catch (RemoteException e) {
					
					this.observableImpl.garbageCollect(event.getListener());
				}

			}
		}

		PublishStatisticsEvent perSubscription = event.getPerSubscriptionEvent();

		if(perSubscription != null){

			try {
				event.getListener().onNewPerSubscriptionStatistics(perSubscription);
			} catch (RemoteException e) {
				this.observableImpl.garbageCollect(event.getListener());
			}


		}

	}

}
