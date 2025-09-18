"""
Performance Optimization Module

Provides intelligent caching, query optimization, and performance tuning
for the Tourism Database Update System.
"""

import os
import json
import time
import hashlib
import logging
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
import threading
from functools import wraps

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """Cache entry with metadata."""
    key: str
    value: Any
    created_at: datetime
    accessed_at: datetime
    access_count: int
    size_bytes: int
    ttl_seconds: Optional[int] = None

    @property
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        if self.ttl_seconds is None:
            return False
        return (datetime.now() - self.created_at).total_seconds() > self.ttl_seconds


@dataclass
class PerformanceStats:
    """Performance optimization statistics."""
    cache_hits: int = 0
    cache_misses: int = 0
    cache_size: int = 0
    cache_memory_mb: float = 0
    query_count: int = 0
    avg_query_time_ms: float = 0
    slow_queries: int = 0
    optimized_queries: int = 0

    @property
    def cache_hit_rate(self) -> float:
        total = self.cache_hits + self.cache_misses
        return (self.cache_hits / total * 100) if total > 0 else 0


class IntelligentCache:
    """Intelligent caching system with LRU eviction and TTL support."""

    def __init__(self, max_size_mb: int = 512, default_ttl_seconds: int = 3600):
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.default_ttl_seconds = default_ttl_seconds
        self.cache: Dict[str, CacheEntry] = {}
        self.lock = threading.RLock()
        self.stats = PerformanceStats()

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self.lock:
            if key in self.cache:
                entry = self.cache[key]

                if entry.is_expired:
                    del self.cache[key]
                    self.stats.cache_misses += 1
                    return None

                entry.accessed_at = datetime.now()
                entry.access_count += 1
                self.stats.cache_hits += 1
                return entry.value

            self.stats.cache_misses += 1
            return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> bool:
        """Set value in cache with optional TTL."""
        with self.lock:
            # Calculate size estimate
            size_bytes = self._estimate_size(value)

            # Check if we need to evict entries
            if not self._ensure_space(size_bytes):
                logger.warning(f"Failed to cache key {key}: insufficient space")
                return False

            now = datetime.now()
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=now,
                accessed_at=now,
                access_count=1,
                size_bytes=size_bytes,
                ttl_seconds=ttl_seconds or self.default_ttl_seconds
            )

            self.cache[key] = entry
            self._update_stats()
            return True

    def invalidate(self, key: str) -> bool:
        """Remove specific key from cache."""
        with self.lock:
            if key in self.cache:
                del self.cache[key]
                self._update_stats()
                return True
            return False

    def clear(self):
        """Clear all cache entries."""
        with self.lock:
            self.cache.clear()
            self._update_stats()

    def cleanup_expired(self):
        """Remove expired entries."""
        with self.lock:
            expired_keys = [
                key for key, entry in self.cache.items()
                if entry.is_expired
            ]
            for key in expired_keys:
                del self.cache[key]

            if expired_keys:
                logger.info(f"Cleaned up {len(expired_keys)} expired cache entries")
                self._update_stats()

    def _ensure_space(self, required_bytes: int) -> bool:
        """Ensure there's enough space for new entry."""
        current_size = sum(entry.size_bytes for entry in self.cache.values())

        if current_size + required_bytes <= self.max_size_bytes:
            return True

        # Need to evict entries (LRU strategy)
        sorted_entries = sorted(
            self.cache.items(),
            key=lambda x: (x[1].accessed_at, x[1].access_count)
        )

        bytes_to_free = current_size + required_bytes - self.max_size_bytes
        bytes_freed = 0

        for key, entry in sorted_entries:
            if bytes_freed >= bytes_to_free:
                break

            del self.cache[key]
            bytes_freed += entry.size_bytes
            logger.debug(f"Evicted cache entry: {key}")

        return bytes_freed >= bytes_to_free

    def _estimate_size(self, value: Any) -> int:
        """Estimate memory size of value."""
        try:
            # Simple estimation - serialize to JSON
            serialized = json.dumps(value, default=str)
            return len(serialized.encode('utf-8'))
        except:
            # Fallback estimation
            return 1024  # 1KB default

    def _update_stats(self):
        """Update cache statistics."""
        self.stats.cache_size = len(self.cache)
        self.stats.cache_memory_mb = sum(
            entry.size_bytes for entry in self.cache.values()
        ) / (1024 * 1024)

    def get_stats(self) -> PerformanceStats:
        """Get current cache statistics."""
        with self.lock:
            return self.stats


class QueryOptimizer:
    """Database query optimization and analysis."""

    def __init__(self, db_config: Dict[str, Any]):
        self.db_config = db_config
        self.query_stats: Dict[str, Dict[str, Any]] = {}
        self.slow_query_threshold_ms = 1000
        self.lock = threading.RLock()

    def analyze_query_performance(self) -> Dict[str, Any]:
        """Analyze database query performance."""
        try:
            import psycopg2

            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Get slow queries
                    cur.execute("""
                        SELECT query, calls, total_time, mean_time, rows
                        FROM pg_stat_statements
                        WHERE mean_time > %s
                        ORDER BY mean_time DESC
                        LIMIT 20
                    """, (self.slow_query_threshold_ms,))

                    slow_queries = [
                        {
                            'query': row[0][:200] + '...' if len(row[0]) > 200 else row[0],
                            'calls': row[1],
                            'total_time_ms': row[2],
                            'mean_time_ms': row[3],
                            'rows': row[4]
                        }
                        for row in cur.fetchall()
                    ]

                    # Get missing indexes
                    cur.execute("""
                        SELECT schemaname, tablename, seq_scan, seq_tup_read,
                               idx_scan, idx_tup_fetch,
                               seq_tup_read::float / seq_scan as avg_seq_read
                        FROM pg_stat_user_tables
                        WHERE seq_scan > 0 AND seq_tup_read / seq_scan > 10000
                        ORDER BY seq_tup_read DESC
                        LIMIT 10
                    """)

                    missing_indexes = [
                        {
                            'table': f"{row[0]}.{row[1]}",
                            'seq_scans': row[2],
                            'seq_rows_read': row[3],
                            'index_scans': row[4] or 0,
                            'avg_rows_per_scan': row[6]
                        }
                        for row in cur.fetchall()
                    ]

                    # Get table bloat
                    cur.execute("""
                        SELECT schemaname, tablename,
                               pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
                               n_dead_tup, n_live_tup,
                               n_dead_tup::float / (n_live_tup + n_dead_tup) * 100 as bloat_pct
                        FROM pg_stat_user_tables
                        WHERE n_dead_tup > 1000 AND n_live_tup > 0
                        ORDER BY n_dead_tup DESC
                        LIMIT 10
                    """)

                    bloated_tables = [
                        {
                            'table': f"{row[0]}.{row[1]}",
                            'size': row[2],
                            'dead_tuples': row[3],
                            'live_tuples': row[4],
                            'bloat_percent': row[5]
                        }
                        for row in cur.fetchall()
                    ]

                    return {
                        'slow_queries': slow_queries,
                        'missing_indexes': missing_indexes,
                        'bloated_tables': bloated_tables,
                        'analysis_timestamp': datetime.now().isoformat()
                    }

        except Exception as e:
            logger.error(f"Failed to analyze query performance: {e}")
            return {}

    def suggest_optimizations(self, analysis: Dict[str, Any]) -> List[str]:
        """Generate optimization suggestions based on analysis."""
        suggestions = []

        # Slow query suggestions
        slow_queries = analysis.get('slow_queries', [])
        if slow_queries:
            suggestions.append(
                f"Found {len(slow_queries)} slow queries. "
                f"Consider optimizing queries with mean time > {self.slow_query_threshold_ms}ms"
            )

        # Missing index suggestions
        missing_indexes = analysis.get('missing_indexes', [])
        for table_info in missing_indexes:
            if table_info['seq_scans'] > 100 and table_info['avg_rows_per_scan'] > 10000:
                suggestions.append(
                    f"Consider adding indexes to {table_info['table']} "
                    f"(avg {table_info['avg_rows_per_scan']:.0f} rows scanned per query)"
                )

        # Table bloat suggestions
        bloated_tables = analysis.get('bloated_tables', [])
        for table_info in bloated_tables:
            if table_info['bloat_percent'] > 20:
                suggestions.append(
                    f"Table {table_info['table']} has {table_info['bloat_percent']:.1f}% bloat. "
                    f"Consider running VACUUM FULL or REINDEX"
                )

        return suggestions

    def optimize_query_plan(self, query: str) -> Dict[str, Any]:
        """Analyze and optimize specific query."""
        try:
            import psycopg2

            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor() as cur:
                    # Get query execution plan
                    cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query}")
                    plan = cur.fetchone()[0][0]

                    # Extract key metrics
                    execution_time = plan.get('Execution Time', 0)
                    planning_time = plan.get('Planning Time', 0)
                    total_cost = plan['Plan'].get('Total Cost', 0)

                    return {
                        'execution_time_ms': execution_time,
                        'planning_time_ms': planning_time,
                        'total_cost': total_cost,
                        'plan': plan,
                        'is_slow': execution_time > self.slow_query_threshold_ms
                    }

        except Exception as e:
            logger.error(f"Failed to optimize query plan: {e}")
            return {}


class PerformanceOptimizer:
    """Main performance optimization coordinator."""

    def __init__(self, db_config: Dict[str, Any], cache_config: Dict[str, Any] = None):
        self.db_config = db_config
        cache_config = cache_config or {}

        self.cache = IntelligentCache(
            max_size_mb=cache_config.get('max_size_mb', 512),
            default_ttl_seconds=cache_config.get('default_ttl_seconds', 3600)
        )

        self.query_optimizer = QueryOptimizer(db_config)
        self.optimization_history: List[Dict[str, Any]] = []

        # Background cleanup task
        self.cleanup_thread = None
        self.running = False

    def start_background_tasks(self):
        """Start background optimization tasks."""
        if self.running:
            return

        self.running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        logger.info("Started performance optimization background tasks")

    def stop_background_tasks(self):
        """Stop background tasks."""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=10)
        logger.info("Stopped performance optimization background tasks")

    def _cleanup_loop(self):
        """Background cleanup loop."""
        while self.running:
            try:
                self.cache.cleanup_expired()
                time.sleep(300)  # Clean up every 5 minutes
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                time.sleep(60)

    def cached_query(self, cache_key: str, ttl_seconds: int = None):
        """Decorator for caching query results."""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Generate cache key with function arguments
                key_data = {
                    'function': func.__name__,
                    'args': args,
                    'kwargs': kwargs,
                    'base_key': cache_key
                }
                key = self._generate_cache_key(key_data)

                # Try cache first
                cached_result = self.cache.get(key)
                if cached_result is not None:
                    return cached_result

                # Execute function and cache result
                start_time = time.time()
                result = func(*args, **kwargs)
                execution_time_ms = (time.time() - start_time) * 1000

                # Cache successful results
                if result is not None:
                    self.cache.set(key, result, ttl_seconds)

                # Track query performance
                self._track_query_performance(func.__name__, execution_time_ms)

                return result

            return wrapper
        return decorator

    def _generate_cache_key(self, key_data: Dict[str, Any]) -> str:
        """Generate deterministic cache key from data."""
        serialized = json.dumps(key_data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode()).hexdigest()[:32]

    def _track_query_performance(self, function_name: str, execution_time_ms: float):
        """Track query performance metrics."""
        if function_name not in self.query_optimizer.query_stats:
            self.query_optimizer.query_stats[function_name] = {
                'call_count': 0,
                'total_time_ms': 0,
                'avg_time_ms': 0,
                'slow_calls': 0
            }

        stats = self.query_optimizer.query_stats[function_name]
        stats['call_count'] += 1
        stats['total_time_ms'] += execution_time_ms
        stats['avg_time_ms'] = stats['total_time_ms'] / stats['call_count']

        if execution_time_ms > self.query_optimizer.slow_query_threshold_ms:
            stats['slow_calls'] += 1

    def optimize_database(self) -> Dict[str, Any]:
        """Run comprehensive database optimization analysis."""
        logger.info("Starting database optimization analysis")

        start_time = time.time()
        analysis = self.query_optimizer.analyze_query_performance()
        suggestions = self.query_optimizer.suggest_optimizations(analysis)

        optimization_result = {
            'timestamp': datetime.now().isoformat(),
            'analysis': analysis,
            'suggestions': suggestions,
            'cache_stats': self.cache.get_stats().__dict__,
            'query_stats': self.query_optimizer.query_stats,
            'analysis_duration_ms': (time.time() - start_time) * 1000
        }

        self.optimization_history.append(optimization_result)

        # Keep only last 10 optimizations
        if len(self.optimization_history) > 10:
            self.optimization_history = self.optimization_history[-10:]

        logger.info(f"Completed optimization analysis with {len(suggestions)} suggestions")
        return optimization_result

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary."""
        cache_stats = self.cache.get_stats()

        return {
            'timestamp': datetime.now().isoformat(),
            'cache': {
                'hit_rate_percent': cache_stats.cache_hit_rate,
                'size': cache_stats.cache_size,
                'memory_mb': cache_stats.cache_memory_mb,
                'hits': cache_stats.cache_hits,
                'misses': cache_stats.cache_misses
            },
            'queries': {
                'total_tracked': len(self.query_optimizer.query_stats),
                'slow_queries': sum(
                    1 for stats in self.query_optimizer.query_stats.values()
                    if stats.get('slow_calls', 0) > 0
                ),
                'avg_response_time_ms': sum(
                    stats.get('avg_time_ms', 0)
                    for stats in self.query_optimizer.query_stats.values()
                ) / max(len(self.query_optimizer.query_stats), 1)
            },
            'optimizations': {
                'total_runs': len(self.optimization_history),
                'last_run': self.optimization_history[-1]['timestamp'] if self.optimization_history else None
            }
        }

    def clear_caches(self):
        """Clear all caches."""
        self.cache.clear()
        logger.info("Cleared all performance caches")

    def get_optimization_history(self) -> List[Dict[str, Any]]:
        """Get optimization history."""
        return self.optimization_history.copy()