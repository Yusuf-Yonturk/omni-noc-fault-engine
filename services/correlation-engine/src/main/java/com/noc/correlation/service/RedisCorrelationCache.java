package com.noc.correlation.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

/** Redis-backed sliding window cache for alarm correlation (Rule 1: same siteId within TTL = same incident). */
@Service
@RequiredArgsConstructor
@Slf4j
public class RedisCorrelationCache {

    private static final String KEY_PREFIX = "correlation:";

    private final StringRedisTemplate redisTemplate;

    @Value("${noc.correlation.window-ttl-seconds:600}")
    private long windowTtlSeconds;

    /** Returns the active incident ID for the given siteId, if a window is open. */
    public Optional<UUID> getActiveIncidentId(String siteId) {
        String key   = buildKey(siteId);
        String value = redisTemplate.opsForValue().get(key);

        if (value == null) {
            log.debug("Cache MISS for siteId={}", siteId);
            return Optional.empty();
        }

        log.debug("Cache HIT for siteId={} → incidentId={}", siteId, value);
        return Optional.of(UUID.fromString(value));
    }

    /** Stores the incident ID for a site with the configured TTL. */
    public void putIncidentId(String siteId, UUID incidentId) {
        String key = buildKey(siteId);
        redisTemplate.opsForValue().set(key, incidentId.toString(), Duration.ofSeconds(windowTtlSeconds));
        log.debug("Cache SET: key={}, incidentId={}, ttl={}s", key, incidentId, windowTtlSeconds);
    }

    /** Resets the TTL for an existing site window (keeps window alive while alarms arrive). */
    public void refreshTtl(String siteId) {
        String key = buildKey(siteId);
        redisTemplate.expire(key, Duration.ofSeconds(windowTtlSeconds));
        log.debug("Cache TTL refreshed for siteId={}", siteId);
    }

    /** Removes the correlation cache entry for a site, forcing a new incident on next alarm. */
    public void evict(String siteId) {
        redisTemplate.delete(buildKey(siteId));
        log.info("Cache evicted for siteId={}", siteId);
    }

    /** Returns true if an active correlation window exists for the given siteId. */
    public boolean hasActiveWindow(String siteId) {
        return Boolean.TRUE.equals(redisTemplate.hasKey(buildKey(siteId)));
    }

    private String buildKey(String siteId) {
        return KEY_PREFIX + siteId;
    }
}
