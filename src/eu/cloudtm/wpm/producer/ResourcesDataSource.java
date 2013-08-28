/*
 * CINI, Consorzio Interuniversitario Nazionale per l'Informatica
 * Copyright 2013 CINI and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 3.0 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package eu.cloudtm.wpm.producer;

import eu.cloudtm.resources.MonitorableResources;
import eu.cloudtm.wpm.hw_probe.CpuResourceProbe;
import eu.cloudtm.wpm.hw_probe.DiskResourceProbe;
import eu.cloudtm.wpm.hw_probe.MemoryResourceProbe;
import eu.cloudtm.wpm.hw_probe.NetworkResourceProbe;
import eu.cloudtm.wpm.sw_probe.FenixFrameworkResourceProbe;
import eu.cloudtm.wpm.sw_probe.InfinispanResourceProbe;
import eu.reservoir.monitoring.appl.BasicDataSource;
import eu.reservoir.monitoring.core.DataSource;
import eu.reservoir.monitoring.core.Probe;
import org.apache.log4j.Logger;

/**
 * @author Roberto Palmieri
 */
public class ResourcesDataSource extends BasicDataSource implements DataSource {

    private final static Logger log = Logger.getLogger(ResourcesController.class);
    private final static boolean INFO = log.isInfoEnabled();

    public ResourcesDataSource(String hostname){
        setName(hostname);
    }

    public boolean addProbe(String name, int probe_timeout, MonitorableResources resource_type) {
        if (INFO) {
            log.info("Trying to add probe. name=" + name + ", timeout=" + probe_timeout + ", type=" + resource_type);
        }

        if (getProbeByName(name) == null) {
            // it doesn't exists, so add it
            //Probe p = new HwResourceProbe(name,cpu_timeout, memory_timeout,network_timeout, disk_timeout,probe_timeout,group_id);
            Probe p;
            switch(resource_type){
                case CPU : p = new CpuResourceProbe(name, probe_timeout);break;
                case MEMORY : p = new MemoryResourceProbe(name, probe_timeout);break;
                case NETWORK : p = new NetworkResourceProbe(name, probe_timeout);break;
                case DISK : p = new DiskResourceProbe(name, probe_timeout);break;
                case JMX : p = new InfinispanResourceProbe(name, probe_timeout);break;
                case FENIX: p = new FenixFrameworkResourceProbe(name, probe_timeout); break;
                default : throw new RuntimeException("Unknown monitorable resource type");
            }
            addProbe(p);
            activateProbe(p);
            turnOnProbe(p);
            if (INFO) {
                log.info("Probe [name=" + name + "] added successfully!");
            }
            return true;
        } else {
            if (INFO) {
                log.info("Probe [name=" + name + "] already exists!");
            }
            return false;
        }
    }

    public boolean deleteProbe(String name) {
        if (INFO) {
            log.info("Trying to delete probe [name=" + name + "]");
        }
        // try and get the probe
        Probe veeProbe = getProbeByName(name);
        if (veeProbe != null) {
            // it exists, so remove it
            turnOffProbe(veeProbe);
            deactivateProbe(veeProbe);
            removeProbe(veeProbe);
            if (INFO) {
                log.info("Probe [name=" + name + "] deleted successfully");
            }
            return true;
        } else {
            // it doesn't exist
            if (INFO) {
                log.info("Probe [name=" + name + "] does not exists!");
            }
            return false;
        }
    }
}
