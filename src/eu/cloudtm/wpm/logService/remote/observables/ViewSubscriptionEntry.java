package eu.cloudtm.wpm.logService.remote.observables;

import eu.cloudtm.wpm.logService.remote.listeners.WPMViewChangeRemoteListener;

public class ViewSubscriptionEntry {
	
	private WPMViewChangeRemoteListener listener;
	
	private Handle handle;
	
	public ViewSubscriptionEntry(Handle handle, WPMViewChangeRemoteListener listener){
		this.handle = handle;
		this.listener = listener;
		
	}

	public WPMViewChangeRemoteListener getListener() {
		return listener;
	}

	public Handle getHandle() {
		return handle;
	}
	
	
	
	
	

}
