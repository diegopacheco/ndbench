package com.netflix.ndbench.plugin.remote.dynomite;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.RetryNTimes;
import com.netflix.dyno.contrib.ArchaiusConnectionPoolConfiguration;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.ndbench.api.plugin.DataGenerator;
import com.netflix.ndbench.api.plugin.NdBenchClient;
import com.netflix.ndbench.api.plugin.annotations.NdBenchClientPlugin;

/**
 * 
 * @author diego.pacheco
 *
 */
@Singleton
@NdBenchClientPlugin("RemoteDynomitePlugin")
public class RemoteDynomitePlugin implements NdBenchClient {

	private static final Logger logger = LoggerFactory.getLogger(RemoteDynomitePlugin.class);
	private static final String ResultOK = "Ok";
	private static final String CacheMiss = null;
	private static final String ClusterName = "dyn_o_mite";

	private DataGenerator dataGenerator;

	private final AtomicReference<DynoJedisClient> jedisClient = new AtomicReference<DynoJedisClient>(null);

	@Override
	public void init(DataGenerator dataGenerator) throws Exception {
		this.dataGenerator = dataGenerator;
		if (jedisClient.get() != null) {
			return;
		}

		logger.info("Initing dyno jedis client");
		logger.info("\nIniting RemoteDynomite Plugin: " + ClusterName);
		
		String seeds = System.getenv("DYNOMITE_SEEDS");
		logger.info("Using Seeds: " + seeds );

		List<DynomiteNodeInfo> nodes = DynomiteSeedsParser.parse(seeds);
		TokenMapSupplier tms = TokenMapSupplierFactory.build(nodes);
		HostSupplier hs = HostSupplierFactory.build(nodes);
		
		
		DynoJedisClient dynoClient = new DynoJedisClient.Builder().withApplicationName(ClusterName)
				.withDynomiteClusterName(ClusterName)
				.withCPConfig(new ArchaiusConnectionPoolConfiguration(ClusterName)
						.withTokenSupplier(tms).setMaxConnsPerHost(1).setConnectTimeout(2000)
						.setRetryPolicyFactory(new RetryNTimes.RetryFactory(1)))
				.withHostSupplier(hs).build();

		jedisClient.set(dynoClient);
	}

	@Override
	public String readSingle(String key) throws Exception {

		String res = jedisClient.get().get(key);

		if (res != null) {
			if (res.isEmpty()) {
				throw new Exception("Data retrieved is not ok ");
			}
		} else {
			return CacheMiss;
		}
		return ResultOK;

	}

	@Override
	public String writeSingle(String key) throws Exception {
		OperationResult<String> result = jedisClient.get().d_set(key, dataGenerator.getRandomValue());

		if (!"OK".equals(result.getResult())) {
			logger.error("SET_ERROR: GOT " + result.getResult() + " for SET operation");
			throw new RuntimeException(String.format("DynoJedis: value %s for SET operation is NOT VALID", key));

		}
		return result.getResult();
	}

	@Override
	public void shutdown() throws Exception {
		if (jedisClient.get() != null) {
			jedisClient.get().stopClient();
			jedisClient.set(null);
		}
	}

	@Override
	public String getConnectionInfo() throws Exception {
		return String.format("DynoJedisPlugin - ConnectionInfo ::Cluster Name - %s", ClusterName);
	}

	@Override
	public String runWorkFlow() throws Exception {
		return null;
	}
}