package eu.cloudtm.wpm.logService.remote.observables;

import java.rmi.Remote;
import java.rmi.RemoteException;

import eu.cloudtm.wpm.logService.remote.events.SubscribeEvent;
import eu.cloudtm.wpm.logService.remote.listeners.WPMStatisticsRemoteListener;
import eu.cloudtm.wpm.logService.remote.listeners.WPMViewChangeRemoteListener;

public interface WPMObservable extends Remote {
	
	
	public void registerWPMStatisticsRemoteListener(SubscribeEvent event, WPMStatisticsRemoteListener listener) throws RemoteException;
	
	public void registerWPMViewChangeRemoteListener(WPMViewChangeRemoteListener listener) throws RemoteException;

}
