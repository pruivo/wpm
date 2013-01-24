package eu.cloudtm.wpm.logService.remote.events;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SubscribeEvent implements Externalizable{
	
	private String[] VMs;
	
	public SubscribeEvent(){
		
	}
	
	public SubscribeEvent(String[] VMs){
		this.VMs = VMs;
	}
	
	

	public String[] getVMs() {
		return VMs;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		int size = in.readInt();
		
		if(size > 0){
			this.VMs = new String[size];
			
			for(int i=0; i<this.VMs.length; i++){
				this.VMs[i] = in.readUTF();
			}
		}
		
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		if(this.VMs != null){
			
			out.writeInt(this.VMs.length);
			
			for(int i=0; i<this.VMs.length; i++){
				out.writeUTF(this.VMs[i]);
			}
		}
		else{
			out.writeInt(0);
		}
		
	}
	
	
	

}
