package eu.cloudtm.wpm.logService.remote.observables;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public final class Handle implements Externalizable{
	
	private long id;
	
	public Handle(){}
	
	public Handle(long id){
		
		this.id = id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (id ^ (id >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Handle other = (Handle) obj;
		if (id != other.id)
			return false;
		return true;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		this.id = in.readLong();
		
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		
		out.writeLong(this.id);
		
	}
	
	

}
