package eu.cloudtm.wpm.logService;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

public class ClearCache {

	public static void main(String[] args) {
		GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
		gc.setClusterName("LogServiceConnection");
		Configuration c = new Configuration();
		c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
		c.setExpirationLifespan(-1);
		c.setExpirationMaxIdle(-1);
		EmbeddedCacheManager cm = new DefaultCacheManager(gc, c);
		System.out.println("Dataitem in cache: "+cm.getCache().size());
		cm.getCache().clear();
		System.out.println("Cache empty");
		System.exit(0);
	}

}
