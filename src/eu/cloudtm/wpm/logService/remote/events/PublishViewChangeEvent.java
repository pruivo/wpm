package eu.cloudtm.wpm.logService.remote.events;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class PublishViewChangeEvent extends PublishEvent{
	
	
	private String[] currentVMs;
	
	public PublishViewChangeEvent(){
		
	}
	
	

	public String[] getCurrentVMs() {
		return currentVMs;
	}



	public PublishViewChangeEvent(String[] currentVMs){
		this.currentVMs = currentVMs;
	}
	
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		int size = in.readInt();
		
		if(size > 0){
			this.currentVMs = new String[size];
			
			for(int i=0; i<this.currentVMs.length; i++){
				this.currentVMs[i] = in.readUTF();
			}
		}
		
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		if(this.currentVMs != null){
			
			out.writeInt(this.currentVMs.length);
			
			for(int i=0; i<this.currentVMs.length; i++){
				out.writeUTF(this.currentVMs[i]);
			}
		}
		else{
			out.writeInt(0);
		}
		
	}

}
