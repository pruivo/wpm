package eu.cloudtm.wpm.logService.remote.listeners;

import java.rmi.Remote;
import java.rmi.RemoteException;

import eu.cloudtm.wpm.logService.remote.events.PublishViewChangeEvent;

public interface WPMViewChangeRemoteListener extends Remote {
	
	public void onViewChange(PublishViewChangeEvent event) throws RemoteException;;

}
