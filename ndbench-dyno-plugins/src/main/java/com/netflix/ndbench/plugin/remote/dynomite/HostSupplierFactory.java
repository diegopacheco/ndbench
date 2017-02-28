package com.netflix.ndbench.plugin.remote.dynomite;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostSupplier;

/**
 * 
 * @author diego.pacheco
 *
 */
public class HostSupplierFactory {
	
	public static HostSupplier build(List<DynomiteNodeInfo> nodes){
		final List<Host> hosts = new ArrayList<Host>();
		
		for(DynomiteNodeInfo node: nodes){
			hosts.add(buildHost(node));
		}
		
		final HostSupplier customHostSupplier = new HostSupplier() {
		   @Override
		   public Collection<Host> getHosts() {
			   return hosts;
		   }
		};
		return customHostSupplier;
	}
	
	private static Host buildHost(DynomiteNodeInfo node){
		Host host = new Host(node.getServer(),8102,node.getDc());
		host.setStatus(Status.Up);
		return host;
	}
	
}