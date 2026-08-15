/*
 * Copyright 2023 IT-Systemhaus der Bundesagentur fuer Arbeit
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.phasetwo.keycloak.redis.testsuite.session;

import io.phasetwo.keycloak.redis.RedisHashCas;
import java.net.ServerSocket;
import java.util.Set;
import org.junit.Assert;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.UnifiedJedis;

/**
 * Shared deterministic single-node Valkey cluster harness for cluster-mode tests.
 *
 * <p>Extracted from {@link RedisClusterUserSessionProviderModelTest} so that the PR&nbsp;#77
 * regression test and the new cluster-reconciliation tests ({@code
 * RedisClusterIndexReconciliationTest}) share one source of truth for the (historically flaky)
 * container bring-up rather than copying it. The strategy is unchanged: run a <em>single</em>
 * Valkey node with {@code --cluster-enabled yes}, bind it to a known fixed host port, announce that
 * exact port, and assign all 16384 slots to it, so {@code CLUSTER SLOTS} advertises a reachable
 * address and there are no cross-node MOVED redirects. A single-node cluster is still enough to
 * make {@code UnifiedJedis.pipelined()} return a {@code ClusterPipeline}, which is all cluster-mode
 * code paths require.
 *
 * <p>Also provides index-inspection helpers. Because every secondary-index Set stores each member
 * as the referenced entity's own Redis key, {@link #expireIndexedEntities} can simulate TTL-expiry
 * of the underlying session hashes — leaving <em>dangling</em> index members — without hardcoding
 * any entity-key format: it reads the Set and {@code DEL}s each member key.
 */
public final class ClusterTestSupport {

  private ClusterTestSupport() {}

  /**
   * A running single-node cluster: its container, a connected cluster client, and the host port.
   */
  public static final class Handle {
    public final FixedHostPortGenericContainer<?> container;
    public final UnifiedJedis jedis;
    public final int port;

    private Handle(FixedHostPortGenericContainer<?> container, UnifiedJedis jedis, int port) {
      this.container = container;
      this.jedis = jedis;
      this.port = port;
    }
  }

  /**
   * Starts a deterministic single-node Valkey cluster, connects a {@link RedisClusterClient},
   * verifies the client is genuinely cluster-mode, and loads the CAS Lua script.
   */
  @SuppressWarnings("deprecation")
  public static Handle start() throws Exception {
    int port = findFreePort();

    FixedHostPortGenericContainer<?> container =
        new FixedHostPortGenericContainer<>("valkey/valkey:8.1.5")
            .withFixedExposedPort(port, 6379)
            .withCommand(
                "valkey-server",
                "--port",
                "6379",
                "--cluster-enabled",
                "yes",
                "--cluster-node-timeout",
                "5000",
                "--appendonly",
                "no",
                "--protected-mode",
                "no",
                "--cluster-announce-ip",
                "127.0.0.1",
                "--cluster-announce-port",
                String.valueOf(port))
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1));
    container.start();

    // Assign every hash slot to the single node so the cluster reaches the 'ok' state.
    container.execInContainer("valkey-cli", "cluster", "addslotsrange", "0", "16383");

    // Wait for the cluster to report a healthy state before connecting.
    boolean ready = false;
    for (int i = 0; i < 40 && !ready; i++) {
      Container.ExecResult info = container.execInContainer("valkey-cli", "cluster", "info");
      ready = info.getStdout().contains("cluster_state:ok");
      if (!ready) {
        Thread.sleep(500);
      }
    }
    if (!ready) {
      throw new IllegalStateException("Valkey cluster did not reach state 'ok' in time");
    }

    JedisClientConfig clientConfig =
        DefaultJedisClientConfig.builder()
            .connectionTimeoutMillis(5000)
            .socketTimeoutMillis(5000)
            .build();
    UnifiedJedis jedis =
        RedisClusterClient.builder()
            .nodes(Set.of(new HostAndPort("127.0.0.1", port)))
            .clientConfig(clientConfig)
            .build();

    // Sanity check: topology is discovered and pipelining yields a genuine ClusterPipeline.
    Assert.assertTrue(
        "Expected a cluster-mode client whose pipeline is a ClusterPipeline",
        jedis.pipelined() instanceof redis.clients.jedis.ClusterPipeline);

    // Load the CAS Lua script onto the cluster node used by the provider's write path.
    RedisHashCas.initialize(jedis);

    return new Handle(container, jedis, port);
  }

  /** Closes the client and stops the container; null-safe. */
  public static void stop(Handle handle) {
    if (handle == null) {
      return;
    }
    if (handle.jedis != null) {
      handle.jedis.close();
    }
    if (handle.container != null) {
      handle.container.stop();
    }
  }

  private static int findFreePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  // ---------------------------------------------------------------------------
  // Index-inspection helpers
  // ---------------------------------------------------------------------------

  /** The user-index Set key for a given user id (members = user-session entity keys). */
  public static String userIndexKey(String userId) {
    return "user-session:user-index:" + userId;
  }

  /** The client-index Set key for a client uuid (members = authenticated-client entity keys). */
  public static String clientIndexKey(String clientUuid) {
    return "authenticated-client:client-index:" + clientUuid;
  }

  /**
   * The parent-index Set key for a user-session id (members = authenticated-client entity keys).
   */
  public static String parentIndexKey(String userSessionId) {
    return "authenticated-client:parent-index:" + userSessionId;
  }

  /** Members currently held by an index Set (never null; empty if the Set is absent). */
  public static Set<String> members(UnifiedJedis jedis, String indexKey) {
    Set<String> m = jedis.smembers(indexKey);
    return m == null ? Set.of() : m;
  }

  /** Remaining TTL in millis for a key: {@code -1} = no expiry set, {@code -2} = key absent. */
  public static long pttl(UnifiedJedis jedis, String key) {
    return jedis.pttl(key);
  }

  /**
   * Simulates TTL-expiry of the session hashes referenced by an index Set, leaving the Set's
   * members dangling (present in the Set but resolving to no entity). Returns the number of member
   * entity keys deleted. The index Set itself is left untouched.
   */
  public static int expireIndexedEntities(UnifiedJedis jedis, String indexKey) {
    Set<String> members = members(jedis, indexKey);
    long deleted = 0L;
    for (String memberKey : members) {
      deleted += jedis.del(memberKey);
    }
    return (int) deleted;
  }
}
