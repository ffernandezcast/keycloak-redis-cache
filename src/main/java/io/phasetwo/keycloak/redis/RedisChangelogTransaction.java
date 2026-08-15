package io.phasetwo.keycloak.redis;

import static io.phasetwo.keycloak.redis.RedisMetrics.*;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.phasetwo.keycloak.common.ExpirableEntity;
import io.phasetwo.keycloak.common.ExpirationUtils;
import io.phasetwo.keycloak.redis.connection.RedisMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.jbosslog.JBossLog;
import org.keycloak.models.AbstractKeycloakTransaction;
import redis.clients.jedis.AbstractPipeline;
import redis.clients.jedis.AbstractTransaction;
import redis.clients.jedis.Response;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.args.ExpiryOption;

@JBossLog
public class RedisChangelogTransaction<K extends Key, A extends MapEntity<K>>
    extends AbstractKeycloakTransaction {

  private final Map<K, A> cache = Maps.newHashMap();
  private final Map<K, A> toDelete = Maps.newHashMap();
  private final Map<String, Set<String>> pendingIndexReaps = Maps.newLinkedHashMap();
  private final Map<String, Long> pendingIndexTtlExtensions = Maps.newLinkedHashMap();
  private final AdapterSupplier<K, A> adapterSupplier;
  private final UnifiedJedis jedis;
  private final RedisMode redisMode;
  private final String cacheName;
  private final Meter.MeterProvider<Counter> counterProvider;
  private static final int MAX_CAS_RETRIES = 3;

  /**
   * Sentinel {@link #extendIndexTtlOnCommit} horizon: a live member never expires ({@code
   * getExpiration() == null}), so no finite TTL can cover it and the index Set must be {@code
   * PERSIST}ed instead. Real horizons are epoch millis ({@code > 0}), so {@code -1} is unambiguous.
   */
  private static final long PERSIST_INDEX_TTL = -1L;

  public RedisChangelogTransaction(
      String cacheName,
      UnifiedJedis jedis,
      RedisMode redisMode,
      AdapterSupplier<K, A> adapterSupplier) {
    this.cacheName = cacheName;
    this.jedis = jedis;
    this.redisMode = redisMode;
    this.adapterSupplier = adapterSupplier;
    this.counterProvider = getCacheCounterProvider();
  }

  public RedisChangelogTransaction(
      String cacheName, UnifiedJedis jedis, AdapterSupplier<K, A> adapterSupplier) {
    this(cacheName, jedis, RedisMode.STANDALONE, adapterSupplier);
  }

  /** The Redis deployment mode this transaction writes for. */
  public RedisMode getRedisMode() {
    return redisMode;
  }

  /** Count an operation in metrics */
  void countOperation(String op) {
    List<Tag> tags = Lists.newArrayList();
    tags.add(Tag.of(CACHE_TAG, cacheName));
    tags.add(Tag.of(OPERATION_TAG, op));
    counterProvider.withTags(tags).increment();
  }

  public static final String HGETALL = "HGETALL";
  public static final String HSETEX = "HSETEX";
  public static final String HSET = "HSET";
  public static final String SADD = "SADD";
  public static final String HDEL = "HDEL";
  public static final String SREM = "SREM";
  public static final String DEL = "DEL";
  public static final String WATCH = "WATCH";

  /**
   * Gets the value if present at the key. Creates a new instance and registers it for saving using
   * the adapter supplier if none is present at the key.
   */
  public A get(K k) {
    A model = getIfPresent(k);
    if (model == null) {
      model = adapterSupplier.newInstance(k);
      cache.put(k, model);
    }
    return model;
  }

  /** Gets the value only if present at the key. Returns null otherwise. */
  public A getIfPresent(K k) {
    if (k == null) return null;
    if (toDelete.containsKey(k)) return null; // this is wrong
    A model = cache.get(k);
    if (model != null && !expired(k, model)) return model;
    String key = k.key();
    log.tracef("[redis] HGETALL %s", key);
    Map<String, String> data = jedis.hgetAll(key);
    countOperation(HGETALL);
    if (data != null && !data.isEmpty()) {
      log.tracef("found data for %s %s", key, data);
      model = adapterSupplier.newInstance(k, data);
      if (!expired(k, model)) {
        cache.put(k, model);
        return model;
      }
    }
    return null;
  }

  /** Lazy removal. Check to see if an entity is expired. return true if it was. add it toDelete. */
  private boolean expired(K k, A a) {
    if (a instanceof ExpirableEntity) {
      ExpirableEntity e = (ExpirableEntity) a;
      if (ExpirationUtils.isExpired(e, true)) {
        log.tracef("Entity at %s expired %s. Lazy removing.", k, ExpirationUtils.fromNow(e));
        addForDelete(a);
        return true;
      } else {
        log.tracef("Entity at %s active. Expires in %s.", k, ExpirationUtils.fromNow(e));
        return false;
      }
    }
    log.tracef("Entity at %s is not an expirable entity.", k);
    return false;
  }

  /**
   * Gets the a map of value if present at the given keys. Return value is a map of the key to the
   * value. May be fewer results if some keys don't have values.
   */
  public Map<K, A> getAll(Collection<K> keys) {
    if (keys == null || keys.isEmpty()) return Maps.newLinkedHashMap();
    Map<K, A> result = Maps.newLinkedHashMap();

    // try-with-resources so the pipeline's pooled connection is always returned; otherwise every
    // getAll leaks a connection and a hot read path exhausts the pool and deadlocks.
    try (AbstractPipeline pipeline = jedis.pipelined()) {
      Map<K, Response<Map<String, String>>> responses = Maps.newLinkedHashMap();

      // Queue all HGETALLs
      for (K key : keys) {
        if (toDelete.containsKey(key)) continue;
        A model = cache.get(key);
        if (model != null) {
          result.put(key, model);
        } else {
          log.tracef("[redis] HGETALL %s", key.key());
          responses.put(key, pipeline.hgetAll(key.key()));
        }
      }
      if (!responses.isEmpty()) { // only execute if some were not cached
        pipeline.sync(); // flush and read all in one round-trip
        // Build result map
        for (Map.Entry<K, Response<Map<String, String>>> entry : responses.entrySet()) {
          K key = entry.getKey();
          Map<String, String> data = entry.getValue().get();
          if (data != null && !data.isEmpty()) {
            A model = adapterSupplier.newInstance(key, data);
            if (!expired(key, model)) {
              result.put(key, model);
              cache.put(key, model);
            }
          }
        }
      }
    }
    return result;
  }

  public void addForSave(A model) {
    cache.put(model.getKey(), model);
  }

  public void addForDelete(A model) {
    toDelete.put(model.getKey(), model);
  }

  public void cachedToDelete() {
    for (A model : cache.values()) {
      addForDelete(model);
    }
  }

  /**
   * Register a secondary-index member to be {@code SREM}'d from its Set when this transaction
   * commits. Reads use this to self-heal <em>dangling</em> index members — those whose referenced
   * entity has expired/vanished — without issuing any Redis write on the read path (the "no writes
   * outside {@code commitImpl}" discipline). Works in all modes; the reap is flushed in {@link
   * #commitImpl} (issue #78).
   */
  public void reapIndexMemberOnCommit(String indexKey, String member) {
    if (indexKey == null || member == null) return;
    pendingIndexReaps.computeIfAbsent(indexKey, k -> Sets.newHashSet()).add(member);
  }

  /**
   * Register that an index Set must remain live long enough to cover a <em>live</em> member found
   * on a by-index read, applied grow-only at commit (issue #78). The TTL backstop is stamped from
   * whichever session's write last touched the Set, so a longer-lived member that is never
   * re-written can be left under a TTL that expires before it does — dropping a live session from
   * the index (reads only remove dangling members, they cannot restore a live one). Reads therefore
   * push the Set's TTL up to the longest-lived live member they resolve. A {@code null} {@code
   * coverUntilMs} means that member never expires, so no finite TTL suffices and the Set is {@code
   * PERSIST}ed. Flushed in {@link #commitImpl}; never issued on the read path (the "no Redis writes
   * outside {@code commitImpl}" discipline). Fail-open.
   */
  public void extendIndexTtlOnCommit(String indexKey, Long coverUntilMs) {
    if (indexKey == null) return;
    long horizon = (coverUntilMs == null) ? PERSIST_INDEX_TTL : coverUntilMs;
    pendingIndexTtlExtensions.merge(
        indexKey,
        horizon,
        (a, b) ->
            (a == PERSIST_INDEX_TTL || b == PERSIST_INDEX_TTL)
                ? PERSIST_INDEX_TTL
                : Math.max(a, b));
  }

  @Override
  protected void commitImpl() {
    if (cache.isEmpty()
        && toDelete.isEmpty()
        && pendingIndexReaps.isEmpty()
        && pendingIndexTtlExtensions.isEmpty()) {
      log.trace("nothing to commit. skipping transaction...");
      return;
    }

    for (A model : Lists.newArrayList(cache.values())) {
      if (model.isMarkedForDelete() || toDelete.containsKey(model.getKey())) {
        deleteEntity(model);
        toDelete.remove(model.getKey());
      } else if (model.isDirty()) {
        writeEntityWithRetries(model);
      }
    }

    for (A model : Lists.newArrayList(toDelete.values())) {
      deleteEntity(model);
      toDelete.remove(model.getKey());
    }

    reapStaleIndexMembers();
    extendIndexTtls();
  }

  /**
   * Flush the index members registered via {@link #reapIndexMemberOnCommit}: {@code SREM} each from
   * the Set it was read from. Best-effort/fail-open — a reap failure must never fail the commit.
   * Members (re)written live in this same transaction are skipped, and each remaining candidate is
   * re-checked at commit and dropped if its entity is present again — a member registered as
   * dangling on read may have been re-created by a concurrent transaction before this one commits
   * (client- session keys are deterministic, so the same member key can legitimately return), and
   * reaping it then would orphan a live session from its index. Batched in a {@code MULTI}/{@code
   * EXEC} for standalone/sentinel; issued per-key in cluster (a cross-slot MULTI is impossible
   * there).
   */
  private void reapStaleIndexMembers() {
    if (pendingIndexReaps.isEmpty()) return;

    Set<String> liveMembers =
        cache.values().stream()
            .filter(m -> !m.isMarkedForDelete() && !toDelete.containsKey(m.getKey()))
            .map(m -> m.getKey().key())
            .collect(Collectors.toSet());

    Map<String, String[]> reaps = Maps.newLinkedHashMap();
    for (Map.Entry<String, Set<String>> e : pendingIndexReaps.entrySet()) {
      String[] members =
          e.getValue().stream().filter(m -> !liveMembers.contains(m)).toArray(String[]::new);
      if (members.length > 0) reaps.put(e.getKey(), members);
    }
    pendingIndexReaps.clear();
    if (reaps.isEmpty()) return;

    reaps = dropReappearedMembers(reaps);
    if (reaps.isEmpty()) return;

    if (redisMode != RedisMode.CLUSTER) {
      try (AbstractTransaction txn = jedis.multi()) {
        for (Map.Entry<String, String[]> e : reaps.entrySet()) {
          log.tracef("[redis] SREM %s (stale index reconciliation)", e.getKey());
          txn.srem(e.getKey(), e.getValue());
          countOperation(SREM);
        }
        txn.exec();
      } catch (Exception ex) {
        log.warnf(ex, "Failed to reap stale index members");
      }
    } else {
      for (Map.Entry<String, String[]> e : reaps.entrySet()) {
        try {
          log.tracef("[redis] SREM %s (stale index reconciliation)", e.getKey());
          jedis.srem(e.getKey(), e.getValue());
          countOperation(SREM);
        } catch (Exception ex) {
          log.warnf(ex, "Failed to reap stale members from index %s", e.getKey());
        }
      }
    }
  }

  /**
   * Re-check each reap candidate against Redis in one pipelined {@code EXISTS} and drop any whose
   * entity is present again — it was re-created between the read that scheduled the reap and this
   * commit, so removing it from the index would orphan a live entity. Fails open: if the check
   * cannot be performed, no reap is issued this commit (the TTL backstop still bounds the Sets).
   */
  private Map<String, String[]> dropReappearedMembers(Map<String, String[]> reaps) {
    Set<String> candidates =
        reaps.values().stream().flatMap(Arrays::stream).collect(Collectors.toSet());
    Set<String> reappeared = Sets.newHashSet();
    try (AbstractPipeline pipeline = jedis.pipelined()) {
      Map<String, Response<Boolean>> exists = Maps.newLinkedHashMap();
      for (String member : candidates) {
        exists.put(member, pipeline.exists(member));
      }
      pipeline.sync();
      for (Map.Entry<String, Response<Boolean>> e : exists.entrySet()) {
        if (Boolean.TRUE.equals(e.getValue().get())) reappeared.add(e.getKey());
      }
    } catch (Exception ex) {
      log.warnf(ex, "Failed to re-verify stale index members; skipping reap this commit");
      return Maps.newLinkedHashMap();
    }
    if (reappeared.isEmpty()) return reaps;

    Map<String, String[]> verified = Maps.newLinkedHashMap();
    for (Map.Entry<String, String[]> e : reaps.entrySet()) {
      String[] survivors =
          Arrays.stream(e.getValue()).filter(m -> !reappeared.contains(m)).toArray(String[]::new);
      if (survivors.length > 0) verified.put(e.getKey(), survivors);
    }
    return verified;
  }

  private void deleteEntity(A model) {
    String key = model.getKey().key();
    Map<String, String> indexes = model.getSecondaryIndexes();

    if (redisMode != RedisMode.CLUSTER && !indexes.isEmpty()) {
      try (AbstractTransaction txn = jedis.multi()) {
        log.tracef("[redis] DEL %s", key);
        txn.del(key);
        countOperation(DEL);
        for (Map.Entry<String, String> index : indexes.entrySet()) {
          log.tracef("[redis] SREM %s %s", index.getKey(), index.getValue());
          txn.srem(index.getKey(), index.getValue());
          countOperation(SREM);
        }
        txn.exec();
      }
    } else {
      log.tracef("[redis] DEL %s", key);
      jedis.del(key);
      countOperation(DEL);
      for (Map.Entry<String, String> index : indexes.entrySet()) {
        log.tracef("[redis] SREM %s %s", index.getKey(), index.getValue());
        jedis.srem(index.getKey(), index.getValue());
        countOperation(SREM);
      }
    }
  }

  private void writeEntityWithRetries(A originalModel) {
    A attemptModel = originalModel;

    for (int attempt = 0; attempt <= MAX_CAS_RETRIES; attempt++) {
      RedisHashCas.CasInvocation invocation = writeEntityOnce(attemptModel);
      long code = invocation.getResponseCode();
      if (code == 1L) {
        log.tracef("[redis] CAS hsetex returned success code %s. %s", code, invocation);
        addSecondaryIndexes(attemptModel);
        return;
      }

      log.warnf("[redis] CAS hsetex returned non-success code %s. %s", code, invocation);
      if ((code != 0L && code != -1L) || attempt == MAX_CAS_RETRIES) {
        throw new IllegalStateException(
            String.format(
                "Redis CAS failed for key %s after %d attempts with code %d",
                attemptModel.getKey().key(), attempt + 1, code));
      }

      attemptModel = rebaseModel(originalModel);
    }
  }

  private RedisHashCas.CasInvocation writeEntityOnce(A model) {
    Long expireAtMs = null;
    if (model instanceof ExpirableEntity) {
      ExpirableEntity e = (ExpirableEntity) model;
      expireAtMs = e.getExpiration();
    }

    RedisHashCas cas = new RedisHashCas(jedis);
    RedisHashCas.CasInvocation invocation =
        cas.hsetex(
            model.getKey().key(),
            model.getVersion(),
            expireAtMs,
            model.getDirtyFields(),
            model.getDeletedFields());
    countOperation(HSETEX);
    if (!model.getDeletedFields().isEmpty()) {
      countOperation(HDEL);
    }
    return invocation;
  }

  private void addSecondaryIndexes(A model) {
    Map<String, String> indexes = model.getSecondaryIndexes();
    List<Map.Entry<String, String>> validIndexes =
        indexes.entrySet().stream()
            .filter(e -> e.getKey() != null && e.getValue() != null)
            .collect(Collectors.toList());

    if (validIndexes.isEmpty()) return;

    // Give every index Set a TTL backstop derived from the referencing session's expiration, so
    // dangling members cannot accumulate unbounded when a session hash TTL-expires without going
    // through the explicit delete path (issue #78). This leak is not cluster-specific — a session
    // that expires natively never runs deleteEntity in any mode — so the backstop applies in every
    // mode. redisMode only decides how the writes are issued, not whether they happen.
    Long expireAtMs = null;
    if (model instanceof ExpirableEntity) {
      expireAtMs = ((ExpirableEntity) model).getExpiration();
    }

    if (redisMode != RedisMode.CLUSTER) {
      // Standalone/sentinel keep atomic index maintenance: the SADD and its TTL backstop ride
      // inside the same MULTI/EXEC.
      try (AbstractTransaction txn = jedis.multi()) {
        for (Map.Entry<String, String> index : validIndexes) {
          log.tracef("[redis] SADD %s %s", index.getKey(), index.getValue());
          txn.sadd(index.getKey(), index.getValue());
          countOperation(SADD);
          applyIndexTtlBackstop(txn, index.getKey(), expireAtMs);
        }
        txn.exec();
      }
    } else {
      // Cluster mode: a MULTI/EXEC across the index Set and the entity key is impossible (they hash
      // to different slots), so each SADD and its TTL backstop is issued individually.
      for (Map.Entry<String, String> index : validIndexes) {
        log.tracef("[redis] SADD %s %s", index.getKey(), index.getValue());
        jedis.sadd(index.getKey(), index.getValue());
        countOperation(SADD);
        applyIndexTtlBackstop(index.getKey(), expireAtMs);
      }
    }
  }

  /**
   * Grow-only TTL backstop on an index Set, issued inside a {@code MULTI}/{@code EXEC}
   * (non-cluster). Establish a TTL if the Set has none (NX), then only ever extend it (GT): Redis
   * treats a key with no TTL as +infinity, so GT alone would never set the initial TTL on a
   * freshly-created Set; NX handles the first write, GT keeps later writes grow-only so a
   * shorter-lived session can't shrink a TTL already covering a longer-lived member of the same
   * Set.
   */
  private void applyIndexTtlBackstop(AbstractTransaction txn, String indexKey, Long expireAtMs) {
    if (expireAtMs == null || expireAtMs <= 0L) return;
    log.tracef("[redis] PEXPIREAT %s %s NX/GT", indexKey, expireAtMs);
    txn.pexpireAt(indexKey, expireAtMs, ExpiryOption.NX);
    txn.pexpireAt(indexKey, expireAtMs, ExpiryOption.GT);
  }

  /**
   * Grow-only TTL backstop on an index Set, issued directly (cluster). See the {@code txn}
   * overload.
   */
  private void applyIndexTtlBackstop(String indexKey, Long expireAtMs) {
    if (expireAtMs == null || expireAtMs <= 0L) return;
    log.tracef("[redis] PEXPIREAT %s %s NX/GT", indexKey, expireAtMs);
    jedis.pexpireAt(indexKey, expireAtMs, ExpiryOption.NX);
    jedis.pexpireAt(indexKey, expireAtMs, ExpiryOption.GT);
  }

  /**
   * Flush the index-TTL extensions registered via {@link #extendIndexTtlOnCommit}: grow each Set's
   * TTL ({@code GT}, first established with {@code NX}) to cover the longest-lived live member a
   * read resolved from it, or {@code PERSIST} the Set when a live member never expires. Batched in
   * a {@code MULTI}/{@code EXEC} for standalone/sentinel, issued per-key in cluster (a cross-slot
   * {@code MULTI} is impossible there). Best-effort/fail-open — never fails the commit.
   */
  private void extendIndexTtls() {
    if (pendingIndexTtlExtensions.isEmpty()) return;
    Map<String, Long> extensions = Maps.newLinkedHashMap(pendingIndexTtlExtensions);
    pendingIndexTtlExtensions.clear();
    try {
      if (redisMode != RedisMode.CLUSTER) {
        try (AbstractTransaction txn = jedis.multi()) {
          for (Map.Entry<String, Long> e : extensions.entrySet()) {
            applyIndexTtlExtension(txn, e.getKey(), e.getValue());
          }
          txn.exec();
        }
      } else {
        for (Map.Entry<String, Long> e : extensions.entrySet()) {
          applyIndexTtlExtension(e.getKey(), e.getValue());
        }
      }
    } catch (Exception e) {
      log.warn("Index TTL extension failed; leaving the existing TTL in place (fail-open)", e);
    }
  }

  /** Non-cluster: grow (or remove) the index Set TTL inside a {@code MULTI}/{@code EXEC}. */
  private void applyIndexTtlExtension(AbstractTransaction txn, String indexKey, long coverUntilMs) {
    if (coverUntilMs == PERSIST_INDEX_TTL) {
      log.tracef("[redis] PERSIST %s (live member never expires)", indexKey);
      txn.persist(indexKey);
    } else {
      log.tracef("[redis] PEXPIREAT %s %s NX/GT (cover live member)", indexKey, coverUntilMs);
      txn.pexpireAt(indexKey, coverUntilMs, ExpiryOption.NX);
      txn.pexpireAt(indexKey, coverUntilMs, ExpiryOption.GT);
    }
  }

  /** Cluster: grow (or remove) the index Set TTL issued directly. See the {@code txn} overload. */
  private void applyIndexTtlExtension(String indexKey, long coverUntilMs) {
    if (coverUntilMs == PERSIST_INDEX_TTL) {
      log.tracef("[redis] PERSIST %s (live member never expires)", indexKey);
      jedis.persist(indexKey);
    } else {
      log.tracef("[redis] PEXPIREAT %s %s NX/GT (cover live member)", indexKey, coverUntilMs);
      jedis.pexpireAt(indexKey, coverUntilMs, ExpiryOption.NX);
      jedis.pexpireAt(indexKey, coverUntilMs, ExpiryOption.GT);
    }
  }

  private A rebaseModel(A originalModel) {
    K key = originalModel.getKey();
    String redisKey = key.key();
    log.tracef("[redis] HGETALL %s (rebase)", redisKey);
    Map<String, String> latestData = jedis.hgetAll(redisKey);
    countOperation(HGETALL);

    A rebasedModel =
        latestData == null || latestData.isEmpty()
            ? adapterSupplier.newInstance(key)
            : adapterSupplier.newInstance(key, latestData);
    originalModel.replayPendingChangesOnto(rebasedModel);
    return rebasedModel;
  }

  @Override
  protected void rollbackImpl() {
    // No action needed on rollback for this use case
  }
}
