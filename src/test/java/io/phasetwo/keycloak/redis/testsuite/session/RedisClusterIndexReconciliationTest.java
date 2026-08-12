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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.phasetwo.keycloak.redis.connection.RedisMode;
import io.phasetwo.keycloak.redis.testsuite.KeycloakModelTest;
import io.phasetwo.keycloak.redis.userSession.RedisUserSessionProvider;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.keycloak.models.Constants;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserSessionModel;
import redis.clients.jedis.UnifiedJedis;

/**
 * Cluster-mode <b>self-reconciling index</b> acceptance tests (issue&nbsp;#78, Tier&nbsp;A).
 *
 * <p>Secondary-index Sets ({@code user-session:user-index:*}, etc.) accumulate <em>dangling</em>
 * members whenever a session hash TTL-expires without going through the explicit delete path: the
 * member string stays in the Set even though the entity it points to is gone. Reads already filter
 * these out of their results, but they never remove them from the Set, so it grows unbounded in
 * cluster mode (where the delete path cannot use a MULTI/EXEC across slots).
 *
 * <p>The feature makes reads <em>self-reconciling</em>: when a by-index lookup encounters a member
 * that resolves to no entity, it {@code SREM}s that member from the Set it was read from. This is
 * gated to {@link RedisMode#CLUSTER} — standalone/sentinel behavior is unchanged and proven so by
 * {@code RedisIndexModeIsolationTest}.
 *
 * <p>Runs against a real cluster-mode Jedis client via {@link ClusterTestSupport}.
 */
public class RedisClusterIndexReconciliationTest extends KeycloakModelTest {

  private static ClusterTestSupport.Handle cluster;
  private static UnifiedJedis clusterJedis;

  private String realmId;

  @BeforeClass
  public static void startCluster() throws Exception {
    cluster = ClusterTestSupport.start();
    clusterJedis = cluster.jedis;
  }

  @AfterClass
  public static void stopCluster() {
    ClusterTestSupport.stop(cluster);
    cluster = null;
    clusterJedis = null;
  }

  @Override
  public void createEnvironment(KeycloakSession s) {
    RealmModel realm = createRealm(s, "test-cluster-reconcile");
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

  /**
   * A stale user-index member (its session hash has TTL-expired) is removed from the index Set the
   * first time the index is read by user, and the lookup returns nothing.
   *
   * <p>RED (before the fix): the read correctly returns empty, but the dead member is left in the
   * Set, so the final {@code SMEMBERS} assertion fails. GREEN: the read {@code SREM}s the dead
   * member and the Set is empty.
   */
  /**
   * Creates one user session for {@code user1} through a real cluster-mode provider (SADDs into the
   * user-index on transaction commit) and returns that user's id.
   */
  private String createUserSessionReturningUserId() {
    return withRealm(
        realmId,
        (s, realm) -> {
          UserModel user = s.users().getUserByUsername(realm, "user1");
          RedisUserSessionProvider provider =
              new RedisUserSessionProvider(s, clusterJedis, RedisMode.CLUSTER);
          provider.createUserSession(realm, user, "user1", "127.0.0.1", "form", true, null, null);
          return user.getId();
        });
  }

  @Test
  public void testStaleUserIndexMemberReconciledOnRead() {
    String userId = createUserSessionReturningUserId();

    String indexKey = ClusterTestSupport.userIndexKey(userId);

    // Precondition: exactly one live member in the index.
    assertThat(ClusterTestSupport.members(clusterJedis, indexKey), hasSize(1));

    // Simulate TTL-expiry of the session hash, leaving the member dangling in the index Set.
    int expired = ClusterTestSupport.expireIndexedEntities(clusterJedis, indexKey);
    assertThat(expired, is(1));
    assertThat(ClusterTestSupport.members(clusterJedis, indexKey), hasSize(1));

    // Read by user: returns nothing (member resolves to no entity) AND self-heals the index.
    withRealm(
        realmId,
        (s, realm) -> {
          UserModel user = s.users().getUserByUsername(realm, "user1");
          RedisUserSessionProvider provider =
              new RedisUserSessionProvider(s, clusterJedis, RedisMode.CLUSTER);
          List<String> found =
              provider
                  .getUserSessionsStream(realm, user)
                  .map(UserSessionModel::getId)
                  .collect(Collectors.toList());
          assertThat(found, is(empty()));
          return null;
        });

    // The dead member must have been reconciled (SREM'd) out of the index Set during the read.
    assertThat(ClusterTestSupport.members(clusterJedis, indexKey), is(empty()));
  }

  /**
   * Every index Set written in cluster mode carries a positive TTL backstop, so members can never
   * outlive the sessions that reference them even if a read never revisits the Set.
   *
   * <p>RED (before the fix): the write path issues only {@code SADD}, so {@code PTTL} is {@code -1}
   * (no expiry) and this fails. GREEN: {@code PEXPIREAT ... GT} gives the Set a positive TTL.
   */
  @Test
  public void testUserIndexHasTtlBackstopInCluster() {
    String userId = createUserSessionReturningUserId();
    String indexKey = ClusterTestSupport.userIndexKey(userId);

    assertThat(ClusterTestSupport.members(clusterJedis, indexKey), hasSize(1));

    long pttl = ClusterTestSupport.pttl(clusterJedis, indexKey);
    assertThat(
        "index Set must carry a positive TTL backstop in cluster mode (was " + pttl + ")",
        pttl > 0L,
        is(true));
  }

  /**
   * The TTL backstop is <b>grow-only</b>: because one index Set aggregates many sessions with
   * different expiries, a later write must never shorten a TTL that already covers a longer-lived
   * member. This guards the {@code GT} flag — a naive unconditional {@code PEXPIREAT} would shrink
   * the TTL to the newest session's (shorter) expiration and fail here.
   */
  @Test
  public void testIndexTtlBackstopIsGrowOnlyInCluster() {
    String userId = createUserSessionReturningUserId();
    String indexKey = ClusterTestSupport.userIndexKey(userId);

    // Force a far-future TTL (+30d), larger than any session lifespan in this realm.
    long farFutureMs = System.currentTimeMillis() + 30L * 24 * 60 * 60 * 1000;
    clusterJedis.pexpireAt(indexKey, farFutureMs);
    assertThat(ClusterTestSupport.pttl(clusterJedis, indexKey) > 0L, is(true));

    // A second session for the same user: its (much shorter) expiration must NOT shrink the TTL.
    createUserSessionReturningUserId();

    long after = ClusterTestSupport.pttl(clusterJedis, indexKey);
    long twentyFiveDaysMs = 25L * 24 * 60 * 60 * 1000;
    assertThat(
        "index TTL must not shrink below a longer-lived member (GT); was " + after,
        after > twentyFiveDaysMs,
        is(true));
  }
}
