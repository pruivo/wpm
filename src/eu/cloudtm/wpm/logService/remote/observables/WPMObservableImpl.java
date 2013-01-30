package eu.cloudtm.wpm.logService.remote.observables;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

import eu.cloudtm.wpm.logService.remote.events.SubscribeEvent;
import eu.cloudtm.wpm.logService.remote.listeners.WPMStatisticsRemoteListener;
import eu.cloudtm.wpm.logService.remote.listeners.WPMViewChangeRemoteListener;

public class WPMObservableImpl extends UnicastRemoteObject implements WPMObservable{
	
	private AtomicLong generator = new AtomicLong(0L);
	
	
	private ConcurrentLinkedQueue<StatsSubscriptionEntry> statsSubscriptions;
	
	private ConcurrentLinkedQueue<ViewSubscriptionEntry> viewSubscriptions;
	
	
	public WPMObservableImpl() throws RemoteException{
		
		this.statsSubscriptions = new ConcurrentLinkedQueue<StatsSubscriptionEntry>();
		this.viewSubscriptions = new ConcurrentLinkedQueue<ViewSubscriptionEntry>();
		
	}
	
	public Iterator<StatsSubscriptionEntry> getStatsIterator(){
		
		return this.statsSubscriptions.iterator();
	}
	
	public int numStatsSubscriptions(){
		return this.statsSubscriptions.size();
	}
	
	public Iterator<ViewSubscriptionEntry> getViewIterator(){
		
		return this.viewSubscriptions.iterator();
	}
	
	public int numViewSubscriptions(){
		return this.viewSubscriptions.size();
	}

	@Override
	public Handle registerWPMStatisticsRemoteListener(SubscribeEvent event, WPMStatisticsRemoteListener listener) throws RemoteException {
	
		System.out.println("Registered Statistics Listener");
		Handle handle = new Handle(this.generator.incrementAndGet());
		this.statsSubscriptions.add(new StatsSubscriptionEntry(handle, event.getVMs(),listener));
		
		return handle;
		
	}

	@Override
	public Handle registerWPMViewChangeRemoteListener(
			WPMViewChangeRemoteListener listener) throws RemoteException {
		
		System.out.println("Registered View Change Listener");
		Handle handle = new Handle(this.generator.incrementAndGet());
		this.viewSubscriptions.add(new ViewSubscriptionEntry(handle, listener));
		
		return handle;
	}

	public void garbageCollect(WPMViewChangeRemoteListener wpmViewChangeRemoteListener) {
		
		
		for(ViewSubscriptionEntry entry: this.viewSubscriptions){
			
			if(entry.getListener() == wpmViewChangeRemoteListener){
				this.viewSubscriptions.remove(entry);
				break;
			}
		}
		
	}

	public void garbageCollect(WPMStatisticsRemoteListener listener) {
		for(StatsSubscriptionEntry entry: this.statsSubscriptions){
			
			if(entry.getListener() == listener){
				this.statsSubscriptions.remove(entry);
				break;
			}
		}
		
	}

	@Override
	public void removeWPMStatisticsRemoteListener(Handle handle)
			throws RemoteException {
		
		for(StatsSubscriptionEntry entry: this.statsSubscriptions){
			
			if(entry.getHandle() != null && entry.getHandle().equals(handle)){
				
				this.statsSubscriptions.remove(entry);
				
			}
			
		}
		
	}

	@Override
	public void removeWPMViewChangeRemoteListener(Handle handle)
			throws RemoteException {
		
		for(ViewSubscriptionEntry entry: this.viewSubscriptions){
			
			if(entry.getHandle() != null && entry.getHandle().equals(handle)){
				
				this.viewSubscriptions.remove(entry);
				
			}
			
		}
		
	}

	
	
	
}
