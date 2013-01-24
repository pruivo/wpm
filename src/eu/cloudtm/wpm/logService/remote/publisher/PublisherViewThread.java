package eu.cloudtm.wpm.logService.remote.publisher;

import java.rmi.RemoteException;

import eu.cloudtm.wpm.logService.remote.events.PublishViewChangeEvent;
import eu.cloudtm.wpm.logService.remote.observables.ViewSubscriptionEntry;
import eu.cloudtm.wpm.logService.remote.observables.WPMObservableImpl;

public class PublisherViewThread extends Thread{
	
	
	private ViewSubscriptionEntry se; 
	private PublishViewChangeEvent event;
	
	private WPMObservableImpl observableImpl;
	
	public PublisherViewThread(ViewSubscriptionEntry se, PublishViewChangeEvent event, WPMObservableImpl observableImpl){
		
		this.se = se;
		this.event = event;
		this.observableImpl = observableImpl;
		
	}
	
	public void run(){
		
		
		try {
			se.getListener().onViewChange(event);
		} catch (RemoteException e) {
			this.observableImpl.garbageCollect(se.getListener());
		}
		
	}

}
