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

import static io.phasetwo.keycloak.redis.testsuite.session.SessionTestUtils.createClients;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;

import io.phasetwo.keycloak.redis.RedisHashCas;
import io.phasetwo.keycloak.redis.connection.RedisMode;
import io.phasetwo.keycloak.redis.testsuite.KeycloakModelTest;
import io.phasetwo.keycloak.redis.userSession.RedisUserSessionProvider;
import java.net.ServerSocket;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.keycloak.models.Constants;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserSessionModel;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClusterClient;
import redis.clients.jedis.UnifiedJedis;

/**
 * Reproduces (RED) and proves the fix (GREEN) for the Redis <b>cluster-mode</b> pipeline {@code
 * ClassCastException}.
 *
 * <p>In cluster mode {@code UnifiedJedis.pipelined()} returns a {@link
 * redis.clients.jedis.ClusterPipeline}, a sibling of {@link redis.clients.jedis.Pipeline} (both
 * extend {@code AbstractPipeline}). The pre-fix code in {@code
 * RedisUserSessionProvider.getUserSessionsStreamByIndexKey} cast the pipeline to {@code Pipeline},
 * throwing a {@code ClassCastException} that is caught and swallowed. The visible effect: every
 * by-index user-session lookup silently returns an empty stream in cluster mode (get-sessions-by-
 * user, broker lookups, active-client-session stats, ...), while single-node/standalone mode is
 * unaffected because it yields a real {@code Pipeline}.
 *
 * <p>This test drives the <em>real</em> {@link RedisUserSessionProvider} against a genuine
 * cluster-mode Jedis client, so {@code .pipelined()} really returns a {@code ClusterPipeline}. It
 * writes a user session through the provider (the write path already handles cluster mode) and then
 * reads it back via {@link RedisUserSessionProvider#getUserSessionsStream(RealmModel, UserModel)},
 * which routes through the buggy {@code getUserSessionsStreamByIndexKey} pipeline path.
 *
 * <p><b>Cluster strategy — deterministic single-node cluster.</b> Multi-node Redis clusters over
 * Testcontainers are notoriously flaky because the topology returned by {@code CLUSTER SLOTS}
 * advertises internal addresses/ports that do not match the randomly mapped host ports (MOVED
 * redirects then fail). We instead run a <em>single</em> Valkey node with {@code --cluster-enabled
 * yes}, bind it to a known fixed host port, and advertise that exact port via {@code
 * --cluster-announce-ip 127.0.0.1 --cluster-announce-port <port>}. All 16384 slots are assigned to
 * the one node, so {@code CLUSTER SLOTS} returns {@code 127.0.0.1:<port>} — an address the client
 * can always reach — and there are no cross-node MOVED redirects. A single-node cluster still
 * causes {@code UnifiedJedis.pipelined()} to return a {@code ClusterPipeline}, which is all that is
 * required to exercise the bug, making this the most robust (non-flaky) option.
 */
@SuppressWarnings("deprecation")
public class RedisClusterUserSessionProviderModelTest extends KeycloakModelTest {

  private static FixedHostPortGenericContainer<?> clusterContainer;
  private static UnifiedJedis clusterJedis;

  private String realmId;

  @BeforeClass
  public static void startCluster() throws Exception {
    int port = findFreePort();

    clusterContainer =
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
    clusterContainer.start();

    // Assign every hash slot to the single node so the cluster reaches the 'ok' state.
    clusterContainer.execInContainer("valkey-cli", "cluster", "addslotsrange", "0", "16383");

    // Wait for the cluster to report a healthy state before connecting.
    boolean ready = false;
    for (int i = 0; i < 40 && !ready; i++) {
      Container.ExecResult info = clusterContainer.execInContainer("valkey-cli", "cluster", "info");
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
    clusterJedis =
        RedisClusterClient.builder()
            .nodes(Set.of(new HostAndPort("127.0.0.1", port)))
            .clientConfig(clientConfig)
            .build();

    // Sanity check: topology is discovered and pipelining yields a genuine ClusterPipeline.
    Assert.assertTrue(
        "Expected a cluster-mode client whose pipeline is a ClusterPipeline",
        clusterJedis.pipelined() instanceof redis.clients.jedis.ClusterPipeline);

    // Load the CAS Lua script onto the cluster node used by the provider's write path.
    RedisHashCas.initialize(clusterJedis);
  }

  private static int findFreePort() throws Exception {
    try (ServerSocket socket = new ServerSocket(0)) {
      socket.setReuseAddress(true);
      return socket.getLocalPort();
    }
  }

  @AfterClass
  public static void stopCluster() {
    if (clusterJedis != null) {
      clusterJedis.close();
      clusterJedis = null;
    }
    if (clusterContainer != null) {
      clusterContainer.stop();
      clusterContainer = null;
    }
  }

  @Override
  public void createEnvironment(org.keycloak.models.KeycloakSession s) {
    RealmModel realm = createRealm(s, "test-cluster");
    s.getContext().setRealm(realm);

    realm.setOfflineSessionIdleTimeout(Constants.DEFAULT_OFFLINE_SESSION_IDLE_TIMEOUT);
    realm.setDefaultRole(
        s.roles().addRealmRole(realm, Constants.DEFAULT_ROLES_ROLE_PREFIX + "-" + realm.getName()));
    realm.setSsoSessionIdleTimeout(1800);
    realm.setSsoSessionMaxLifespan(36000);
    realm.setClientSessionIdleTimeout(500);
    this.realmId = realm.getId();

    s.users().addUser(realm, "user1").setEmail("user1@localhost");

    createClients(s, realm);
  }

  @Override
  public void cleanEnvironment(org.keycloak.models.KeycloakSession s) {
    RealmModel realm = s.realms().getRealm(realmId);
    s.getContext().setRealm(realm);
    s.realms().removeRealm(realmId);
  }

  /**
   * Creates a user session through a real cluster-mode {@link RedisUserSessionProvider}, then reads
   * it back by user. The read routes through {@code getUserSessionsStreamByIndexKey}, whose
   * pipeline cast is the bug under test.
   *
   * <p>GREEN (with the fix): the session is found. RED (fix stashed): the swallowed {@code
   * ClassCastException} makes the lookup return an empty stream and the {@code hasSize(1)}
   * assertion fails.
   */
  @Test
  public void testGetUserSessionsByUserInClusterMode() {
    // Write a user session through the provider; flushes to Redis on transaction commit.
    String sessionId =
        withRealm(
            realmId,
            (s, realm) -> {
              UserModel user = s.users().getUserByUsername(realm, "user1");
              RedisUserSessionProvider provider =
                  new RedisUserSessionProvider(s, clusterJedis, RedisMode.CLUSTER);
              UserSessionModel userSession =
                  provider.createUserSession(
                      realm, user, "user1", "127.0.0.1", "form", true, null, null);
              return userSession.getId();
            });

    // Read the session back by user in a fresh transaction / fresh provider.
    withRealm(
        realmId,
        (s, realm) -> {
          UserModel user = s.users().getUserByUsername(realm, "user1");
          RedisUserSessionProvider provider =
              new RedisUserSessionProvider(s, clusterJedis, RedisMode.CLUSTER);

          List<String> foundIds =
              provider
                  .getUserSessionsStream(realm, user)
                  .map(UserSessionModel::getId)
                  .collect(Collectors.toList());

          assertThat(foundIds, hasSize(1));
          assertThat(foundIds, contains(sessionId));
          return null;
        });
  }
}
