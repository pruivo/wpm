package eu.cloudtm.wpm.logService.remote.listeners;

import java.rmi.Remote;
import java.rmi.RemoteException;

import eu.cloudtm.wpm.logService.remote.events.PublishStatisticsEvent;

public interface WPMStatisticsRemoteListener extends Remote {
	
	
	public void onNewPerVMStatistics(PublishStatisticsEvent event)throws RemoteException;
	
	public void onNewPerSubscriptionStatistics(PublishStatisticsEvent event) throws RemoteException;

}
