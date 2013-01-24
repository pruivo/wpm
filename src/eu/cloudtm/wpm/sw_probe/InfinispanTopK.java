package eu.cloudtm.wpm.sw_probe;

import java.util.Map;

class InfinispanTopK {

	private Map RemoteTopPuts;
	private Map TopLockFailedKeys;
	private Map TopLockedKeys;
	private Map TopContendedKeys;
	private Map LocalTopPuts;
	private Map LocalTopGets;
	private Map RemoteTopGets;

	public InfinispanTopK() {

	}

	public Map getRemoteTopPuts() {
		return RemoteTopPuts;
	}

	public void setRemoteTopPuts(Map remoteTopPuts) {
		RemoteTopPuts = remoteTopPuts;
	}

	public Map getTopLockFailedKeys() {
		return TopLockFailedKeys;
	}

	public void setTopLockFailedKeys(Map topLockFailedKeys) {
		TopLockFailedKeys = topLockFailedKeys;
	}

	public Map getTopLockedKeys() {
		return TopLockedKeys;
	}

	public void setTopLockedKeys(Map topLockedKeys) {
		TopLockedKeys = topLockedKeys;
	}

	public Map getTopContendedKeys() {
		return TopContendedKeys;
	}

	public void setTopContendedKeys(Map topContendedKeys) {
		TopContendedKeys = topContendedKeys;
	}

	public Map getLocalTopPuts() {
		return LocalTopPuts;
	}

	public void setLocalTopPuts(Map localTopPuts) {
		LocalTopPuts = localTopPuts;
	}

	public Map getLocalTopGets() {
		return LocalTopGets;
	}

	public void setLocalTopGets(Map localTopGets) {
		LocalTopGets = localTopGets;
	}

	public Map getRemoteTopGets() {
		return RemoteTopGets;
	}

	public void setRemoteTopGets(Map remoteTopGets) {
		RemoteTopGets = remoteTopGets;
	}

}