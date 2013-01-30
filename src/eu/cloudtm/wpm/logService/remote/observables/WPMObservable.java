package eu.cloudtm.wpm.logService.remote.observables;

import java.rmi.Remote;
import java.rmi.RemoteException;

import eu.cloudtm.wpm.logService.remote.events.SubscribeEvent;
import eu.cloudtm.wpm.logService.remote.listeners.WPMStatisticsRemoteListener;
import eu.cloudtm.wpm.logService.remote.listeners.WPMViewChangeRemoteListener;

public interface WPMObservable extends Remote {
	
	
	public Handle registerWPMStatisticsRemoteListener(SubscribeEvent event, WPMStatisticsRemoteListener listener) throws RemoteException;
	
	public Handle registerWPMViewChangeRemoteListener(WPMViewChangeRemoteListener listener) throws RemoteException;
	
	public void removeWPMStatisticsRemoteListener(Handle handle) throws RemoteException;
	
	public void removeWPMViewChangeRemoteListener(Handle handle) throws RemoteException;

}
