package eu.cloudtm.wpm.logService.remote.observables;

import eu.cloudtm.wpm.logService.remote.listeners.WPMViewChangeRemoteListener;

public class ViewSubscriptionEntry {
	
	private WPMViewChangeRemoteListener listener;
	
	public ViewSubscriptionEntry(WPMViewChangeRemoteListener listener){
		
		this.listener = listener;
		
	}

	public WPMViewChangeRemoteListener getListener() {
		return listener;
	}
	
	
	

}
