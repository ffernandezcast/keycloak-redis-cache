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
import static org.hamcrest.Matchers.is;

import io.phasetwo.keycloak.redis.RedisHashCas;
import io.phasetwo.keycloak.redis.authSession.RedisAuthenticationSessionProvider;
import io.phasetwo.keycloak.redis.connection.RedisMode;
import io.phasetwo.keycloak.redis.testsuite.KeycloakModelTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.keycloak.models.ClientModel;
import org.keycloak.models.Constants;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.sessions.AuthenticationSessionModel;
import org.keycloak.sessions.RootAuthenticationSessionModel;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClient;
import redis.clients.jedis.UnifiedJedis;

/**
 * The child authentication-session hash ({@code auth-session:<clientId>:<tabId>}) must carry a TTL
 * backstop just like its root ({@code root-auth-session:*}) does. The adapter did not implement
 * {@link io.phasetwo.keycloak.common.ExpirableEntity}, so the write path computed a {@code null}
 * expiration and skipped {@code PEXPIREAT} — leaving every {@code auth-session:*} key with no TTL
 * and accumulating unbounded (observed as ~110&nbsp;MB of never-expiring keys on a live
 * deployment). See the discussion on issue&nbsp;#78.
 */
public class AuthSessionTtlBackstopTest extends KeycloakModelTest {

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
    RealmModel realm = createRealm(s, "test-authsession-ttl");
    s.getContext().setRealm(realm);
    realm.setDefaultRole(
        s.roles().addRealmRole(realm, Constants.DEFAULT_ROLES_ROLE_PREFIX + "-" + realm.getName()));
    realm.setAccessCodeLifespanLogin(1800);
    this.realmId = realm.getId();

    createClients(s, realm);
  }

  @Override
  public void cleanEnvironment(KeycloakSession s) {
    RealmModel realm = s.realms().getRealm(realmId);
    s.getContext().setRealm(realm);
    s.realms().removeRealm(realmId);
    jedis.flushAll();
  }

  @Test
  public void createdAuthSessionKeyCarriesTtlBackstop() {
    String[] created = new String[2]; // clientUuid, tabId
    withRealm(
        realmId,
        (s, realm) -> {
          ClientModel client = realm.getClientByClientId("test-app");
          RedisAuthenticationSessionProvider provider =
              new RedisAuthenticationSessionProvider(s, jedis, RedisMode.STANDALONE, 300);
          RootAuthenticationSessionModel root = provider.createRootAuthenticationSession(realm);
          AuthenticationSessionModel authSession = root.createAuthenticationSession(client);
          created[0] = client.getId();
          created[1] = authSession.getTabId();
          return null;
        });

    String authSessionKey = String.format("auth-session:%s:%s", created[0], created[1]);
    assertThat("the auth-session hash must be written", jedis.exists(authSessionKey), is(true));

    long pttl = jedis.pttl(authSessionKey);
    assertThat(
        "the auth-session key must carry a positive TTL backstop so it cannot leak forever; pttl was "
            + pttl,
        pttl > 0L,
        is(true));
  }
}
