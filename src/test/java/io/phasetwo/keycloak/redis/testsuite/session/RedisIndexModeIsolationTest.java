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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.phasetwo.keycloak.redis.RedisHashCas;
import io.phasetwo.keycloak.redis.connection.RedisMode;
import io.phasetwo.keycloak.redis.testsuite.KeycloakModelTest;
import io.phasetwo.keycloak.redis.userSession.RedisUserSessionProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.keycloak.models.Constants;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.UnifiedJedis;

/**
 * Mode-isolation non-regression tests (issue&nbsp;#78, Tier&nbsp;B).
 *
 * <p>Proves the hard requirement that the cluster-only self-reconciliation feature <b>never fires in
 * standalone or sentinel mode</b>: index Sets get no TTL backstop, and reads over a dangling member
 * do <em>not</em> {@code SREM} it. Both non-cluster modes take the same {@code redisMode !=
 * RedisMode.CLUSTER} branch, so one standalone Valkey node is sufficient to exercise both — the mode
 * enum alone selects the branch, and the non-cluster paths (MULTI/EXEC writes, lazy reads) all work
 * against a standalone client. These assertions pass today and would only fail if a future change
 * accidentally un-gated the cluster behavior.
 */
public class RedisIndexModeIsolationTest extends KeycloakModelTest {

  private static GenericContainer<?> container;
  private static UnifiedJedis jedis;

  private String realmId;

  @BeforeClass
  public static void startStandalone() {
    container =
        new GenericContainer<>("valkey/valkey:8.1.5")
            .withExposedPorts(6379)
            .waitingFor(Wait.forLogMessage(".*Ready to accept connections.*", 1));
    container.start();

    JedisClientConfig clientConfig =
        DefaultJedisClientConfig.builder()
            .connectionTimeoutMillis(5000)
            .socketTimeoutMillis(5000)
            .build();
    jedis =
        RedisClient.builder()
            .hostAndPort(new HostAndPort(container.getHost(), container.getMappedPort(6379)))
            .clientConfig(clientConfig)
            .build();

    RedisHashCas.initialize(jedis);
  }

  @AfterClass
  public static void stopStandalone() {
    if (jedis != null) {
      jedis.close();
      jedis = null;
    }
    if (container != null) {
      container.stop();
      container = null;
    }
  }

  @Override
  public void createEnvironment(KeycloakSession s) {
    RealmModel realm = createRealm(s, "test-mode-isolation");
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
  public void cleanEnvironment(KeycloakSession s) {
    RealmModel realm = s.realms().getRealm(realmId);
    s.getContext().setRealm(realm);
    s.realms().removeRealm(realmId);
  }

  @Test
  public void standaloneNeitherReconcilesNorSetsTtl() {
    assertNoClusterBehavior(RedisMode.STANDALONE);
  }

  @Test
  public void sentinelNeitherReconcilesNorSetsTtl() {
    assertNoClusterBehavior(RedisMode.SENTINEL);
  }

  /**
   * In a non-cluster mode: (B3) an index Set written by the provider carries NO TTL, and (B2) a read
   * over a dangling member leaves that member in the Set (no reconciliation).
   */
  private void assertNoClusterBehavior(RedisMode mode) {
    String userId =
        withRealm(
            realmId,
            (s, realm) -> {
              UserModel user = s.users().getUserByUsername(realm, "user1");
              RedisUserSessionProvider provider = new RedisUserSessionProvider(s, jedis, mode);
              provider.createUserSession(realm, user, "user1", "127.0.0.1", "form", true, null, null);
              return user.getId();
            });
    String indexKey = ClusterTestSupport.userIndexKey(userId);

    // B3: no TTL backstop is written outside cluster mode.
    assertThat(ClusterTestSupport.members(jedis, indexKey), hasSize(1));
    assertThat(
        "non-cluster index Set must have NO TTL backstop (" + mode + ")",
        ClusterTestSupport.pttl(jedis, indexKey),
        is(-1L));

    // Plant a dangling member (session hash gone, index member left behind).
    assertThat(ClusterTestSupport.expireIndexedEntities(jedis, indexKey), is(1));

    // The read returns nothing (the member resolves to no entity) ...
    withRealm(
        realmId,
        (s, realm) -> {
          UserModel user = s.users().getUserByUsername(realm, "user1");
          RedisUserSessionProvider provider = new RedisUserSessionProvider(s, jedis, mode);
          assertThat(provider.getUserSessionsStream(realm, user).count(), is(0L));
          return null;
        });

    // B2: ... but the dead member REMAINS — reconciliation must not fire outside cluster mode.
    assertThat(
        "non-cluster read must NOT reconcile stale members (" + mode + ")",
        ClusterTestSupport.members(jedis, indexKey),
        hasSize(1));
  }
}
