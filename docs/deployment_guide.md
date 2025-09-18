# Tourism Database Deployment & Scaling Guide

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [Production Deployment](#production-deployment)
3. [Scaling Strategies](#scaling-strategies)
4. [Monitoring & Observability](#monitoring--observability)
5. [Backup & Recovery](#backup--recovery)
6. [Security Considerations](#security-considerations)
7. [Performance Optimization](#performance-optimization)
8. [Troubleshooting](#troubleshooting)

## Prerequisites

### System Requirements

#### Minimum Requirements
- **CPU**: 4 cores
- **RAM**: 8 GB
- **Storage**: 500 GB SSD
- **Network**: 1 Gbps
- **OS**: Ubuntu 20.04+ or CentOS 8+

#### Recommended Production Requirements
- **CPU**: 8+ cores
- **RAM**: 32+ GB
- **Storage**: 2+ TB NVMe SSD
- **Network**: 10 Gbps
- **OS**: Ubuntu 22.04 LTS

### Software Dependencies

```bash
# System packages
sudo apt update
sudo apt install -y postgresql-15 postgresql-contrib-15 postgresql-15-postgis-3
sudo apt install -y python3.11 python3.11-venv python3.11-dev
sudo apt install -y nginx redis-server supervisor
sudo apt install -y git curl wget unzip

# Python dependencies
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Database Setup

```bash
# PostgreSQL configuration
sudo -u postgres createuser tourism_user --createdb --pwprompt
sudo -u postgres createdb tourism_production --owner=tourism_user

# Install schemas
psql -U tourism_user -d tourism_production -f sql/corrected_tourism_schema.sql
psql -U tourism_user -d tourism_production -f sql/changelog_schema.sql
psql -U tourism_user -d tourism_production -f sql/triggers.sql

# Configure PostgreSQL for production
sudo nano /etc/postgresql/15/main/postgresql.conf
```

## Production Deployment

### 1. Environment Configuration

Create production configuration file:

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "database": "tourism_production",
    "user": "tourism_user",
    "password": "${POSTGRES_PASSWORD}",
    "pool_size": 20,
    "max_overflow": 30,
    "pool_timeout": 30,
    "pool_recycle": 3600
  },
  "source": {
    "url": "https://linked.toerismevlaanderen.be/files/02a71541-9434-11f0-b486-e14b0db176db/download?name=toeristische-attracties.ttl",
    "download_timeout": 600,
    "max_retries": 5
  },
  "processing": {
    "batch_size": 500,
    "dry_run_first": true,
    "force_update": false,
    "parallel_workers": 4
  },
  "monitoring": {
    "enable_notifications": true,
    "notification_config": {
      "email": {
        "enabled": true,
        "smtp_server": "smtp.company.com",
        "smtp_port": 587,
        "username": "${SMTP_USERNAME}",
        "password": "${SMTP_PASSWORD}",
        "to_addresses": ["admin@company.com", "monitoring@company.com"]
      },
      "webhook": {
        "enabled": true,
        "url": "https://hooks.slack.com/services/...",
        "headers": {
          "Content-Type": "application/json"
        }
      }
    },
    "thresholds": {
      "cpu_critical": 85.0,
      "memory_critical": 85.0,
      "disk_critical": 90.0,
      "error_rate_critical": 15.0
    }
  },
  "backup": {
    "backup_directory": "/data/backups",
    "retention_days": 90,
    "compression": true,
    "encryption": true,
    "auto_backup_enabled": true,
    "full_backup_schedule": "daily",
    "incremental_backup_interval_hours": 4
  },
  "logging": {
    "level": "INFO",
    "log_directory": "/var/log/tourism_db",
    "max_file_size_mb": 100,
    "backup_count": 10,
    "structured_logging": true
  }
}
```

### 2. Service Configuration

#### Systemd Service

Create `/etc/systemd/system/tourism-update.service`:

```ini
[Unit]
Description=Tourism Database Update Service
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=forking
User=tourism
Group=tourism
WorkingDirectory=/opt/tourism_db
Environment=PATH=/opt/tourism_db/venv/bin
Environment=PYTHONPATH=/opt/tourism_db
ExecStart=/opt/tourism_db/venv/bin/python tourism_update_cli.py update --config /etc/tourism_db/production.json
ExecReload=/bin/kill -HUP $MAINPID
KillMode=mixed
Restart=on-failure
RestartSec=30
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

#### Supervisor Configuration

Create `/etc/supervisor/conf.d/tourism-dashboard.conf`:

```ini
[program:tourism-dashboard]
command=/opt/tourism_db/venv/bin/python web_dashboard/app.py
directory=/opt/tourism_db
user=tourism
autostart=true
autorestart=true
redirect_stderr=true
stdout_logfile=/var/log/tourism_db/dashboard.log
environment=PYTHONPATH="/opt/tourism_db"
```

#### Nginx Configuration

Create `/etc/nginx/sites-available/tourism-dashboard`:

```nginx
upstream tourism_dashboard {
    server 127.0.0.1:5000;
}

server {
    listen 80;
    server_name tourism-dashboard.company.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name tourism-dashboard.company.com;

    ssl_certificate /etc/ssl/certs/tourism-dashboard.crt;
    ssl_certificate_key /etc/ssl/private/tourism-dashboard.key;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-RSA-AES128-GCM-SHA256:ECDHE-RSA-AES256-GCM-SHA384;

    access_log /var/log/nginx/tourism-dashboard.access.log;
    error_log /var/log/nginx/tourism-dashboard.error.log;

    location / {
        proxy_pass http://tourism_dashboard;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    # Security headers
    add_header X-Content-Type-Options nosniff;
    add_header X-Frame-Options DENY;
    add_header X-XSS-Protection "1; mode=block";
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains";
}
```

### 3. Deployment Script

Create `deploy.sh`:

```bash
#!/bin/bash
set -e

# Configuration
DEPLOY_USER="tourism"
DEPLOY_PATH="/opt/tourism_db"
BACKUP_PATH="/data/backups"
CONFIG_PATH="/etc/tourism_db"

echo "🚀 Starting Tourism Database Deployment"

# Create directories
sudo mkdir -p $DEPLOY_PATH $BACKUP_PATH $CONFIG_PATH /var/log/tourism_db
sudo chown $DEPLOY_USER:$DEPLOY_USER $DEPLOY_PATH $BACKUP_PATH /var/log/tourism_db

# Clone/update repository
if [ -d "$DEPLOY_PATH/.git" ]; then
    cd $DEPLOY_PATH
    sudo -u $DEPLOY_USER git pull
else
    sudo -u $DEPLOY_USER git clone https://github.com/your-org/tourism_db.git $DEPLOY_PATH
    cd $DEPLOY_PATH
fi

# Setup Python environment
sudo -u $DEPLOY_USER python3.11 -m venv venv
sudo -u $DEPLOY_USER venv/bin/pip install --upgrade pip
sudo -u $DEPLOY_USER venv/bin/pip install -r requirements.txt

# Copy configuration
sudo cp config/production.json $CONFIG_PATH/

# Set permissions
sudo chmod 600 $CONFIG_PATH/production.json
sudo chown $DEPLOY_USER:$DEPLOY_USER $CONFIG_PATH/production.json

# Install services
sudo systemctl daemon-reload
sudo systemctl enable tourism-update
sudo systemctl enable nginx
sudo systemctl enable supervisor

# Start services
sudo systemctl start postgresql
sudo systemctl start redis-server
sudo systemctl start supervisor
sudo systemctl start nginx

# Test deployment
echo "🧪 Testing deployment..."
sudo -u $DEPLOY_USER $DEPLOY_PATH/venv/bin/python tourism_update_cli.py status

echo "✅ Deployment completed successfully!"
echo "📊 Dashboard: https://tourism-dashboard.company.com"
```

## Scaling Strategies

### Horizontal Scaling

#### Database Read Replicas

```yaml
# docker-compose.yml for read replicas
version: '3.8'
services:
  postgres-primary:
    image: postgres:15
    environment:
      POSTGRES_REPLICATION_MODE: master
      POSTGRES_REPLICATION_USER: replicator
      POSTGRES_REPLICATION_PASSWORD: ${REPLICATION_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data

  postgres-replica-1:
    image: postgres:15
    environment:
      POSTGRES_REPLICATION_MODE: slave
      POSTGRES_REPLICATION_USER: replicator
      POSTGRES_REPLICATION_PASSWORD: ${REPLICATION_PASSWORD}
      POSTGRES_MASTER_SERVICE: postgres-primary
    depends_on:
      - postgres-primary

  postgres-replica-2:
    image: postgres:15
    environment:
      POSTGRES_REPLICATION_MODE: slave
      POSTGRES_REPLICATION_USER: replicator
      POSTGRES_REPLICATION_PASSWORD: ${REPLICATION_PASSWORD}
      POSTGRES_MASTER_SERVICE: postgres-primary
    depends_on:
      - postgres-primary
```

#### Load Balancer Configuration

```nginx
# /etc/nginx/conf.d/tourism-api-lb.conf
upstream tourism_api_backend {
    least_conn;
    server 10.0.1.10:5000 weight=3 max_fails=3 fail_timeout=30s;
    server 10.0.1.11:5000 weight=3 max_fails=3 fail_timeout=30s;
    server 10.0.1.12:5000 weight=2 max_fails=3 fail_timeout=30s;
}

server {
    listen 80;
    location / {
        proxy_pass http://tourism_api_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;

        # Health checks
        proxy_next_upstream error timeout http_502 http_503 http_504;
        proxy_connect_timeout 1s;
        proxy_send_timeout 3s;
        proxy_read_timeout 3s;
    }
}
```

### Vertical Scaling

#### PostgreSQL Optimization

```sql
-- postgresql.conf optimizations for large datasets
shared_buffers = '8GB'                    # 25% of RAM
effective_cache_size = '24GB'             # 75% of RAM
work_mem = '256MB'                        # For complex queries
maintenance_work_mem = '2GB'              # For maintenance tasks
wal_buffers = '16MB'                      # WAL buffer size
checkpoint_completion_target = 0.9        # Smooth checkpoints
random_page_cost = 1.1                    # For SSD storage
effective_io_concurrency = 200            # For SSD storage
max_connections = 200                     # Connection limit
shared_preload_libraries = 'pg_stat_statements'

-- Enable query statistics
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Create additional indexes for performance
CREATE INDEX CONCURRENTLY idx_logies_updated_at ON logies(updated_at);
CREATE INDEX CONCURRENTLY idx_addresses_municipality ON addresses(municipality);
CREATE INDEX CONCURRENTLY idx_logies_type_status ON logies(type, status);
```

#### Redis Caching

```python
# Enhanced caching configuration
REDIS_CONFIG = {
    'host': 'localhost',
    'port': 6379,
    'db': 0,
    'decode_responses': True,
    'socket_keepalive': True,
    'socket_keepalive_options': {},
    'max_connections': 50,
    'retry_on_timeout': True,
    'health_check_interval': 30
}

# Cache strategies
CACHE_STRATEGIES = {
    'frequent_queries': {'ttl': 300, 'strategy': 'LRU'},
    'static_data': {'ttl': 3600, 'strategy': 'LFU'},
    'user_sessions': {'ttl': 1800, 'strategy': 'TTL'},
    'api_responses': {'ttl': 60, 'strategy': 'LRU'}
}
```

### Container Orchestration

#### Kubernetes Deployment

```yaml
# k8s/tourism-db-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tourism-db-api
  labels:
    app: tourism-db-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: tourism-db-api
  template:
    metadata:
      labels:
        app: tourism-db-api
    spec:
      containers:
      - name: tourism-api
        image: tourism-db:latest
        ports:
        - containerPort: 5000
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: tourism-db-secret
              key: database-url
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 5000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 5000
          initialDelaySeconds: 5
          periodSeconds: 5

---
apiVersion: v1
kind: Service
metadata:
  name: tourism-db-service
spec:
  selector:
    app: tourism-db-api
  ports:
  - protocol: TCP
    port: 80
    targetPort: 5000
  type: LoadBalancer
```

## Monitoring & Observability

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "tourism_db_rules.yml"

scrape_configs:
  - job_name: 'tourism-db'
    static_configs:
      - targets: ['localhost:5000']
    metrics_path: '/metrics'
    scrape_interval: 10s

  - job_name: 'postgres-exporter'
    static_configs:
      - targets: ['localhost:9187']

  - job_name: 'node-exporter'
    static_configs:
      - targets: ['localhost:9100']

alerting:
  alertmanagers:
    - static_configs:
        - targets:
          - alertmanager:9093
```

### Grafana Dashboards

Key metrics to monitor:

1. **System Metrics**
   - CPU usage and load average
   - Memory utilization
   - Disk I/O and space usage
   - Network throughput

2. **Database Metrics**
   - Connection count and pool status
   - Query performance and slow queries
   - Cache hit rates
   - Replication lag (if using replicas)

3. **Application Metrics**
   - Update job success/failure rates
   - Processing time and throughput
   - Error rates by component
   - API response times

4. **Business Metrics**
   - Data freshness and update frequency
   - Record counts and growth trends
   - Data quality metrics

### Alert Rules

```yaml
# tourism_db_rules.yml
groups:
- name: tourism_db_alerts
  rules:
  - alert: HighCPUUsage
    expr: cpu_usage > 85
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High CPU usage detected"
      description: "CPU usage is {{ $value }}% for more than 5 minutes"

  - alert: DatabaseConnectionsHigh
    expr: db_connections > 80
    for: 2m
    labels:
      severity: critical
    annotations:
      summary: "High database connection count"
      description: "Database connections: {{ $value }}"

  - alert: UpdateJobFailed
    expr: increase(update_job_failures[1h]) > 0
    labels:
      severity: critical
    annotations:
      summary: "Tourism DB update job failed"
      description: "Update job has failed in the last hour"
```

## Backup & Recovery

### Automated Backup Strategy

```bash
#!/bin/bash
# /opt/tourism_db/scripts/backup.sh

BACKUP_DIR="/data/backups"
RETENTION_DAYS=90
DB_NAME="tourism_production"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Full backup
pg_dump -h localhost -U tourism_user -d $DB_NAME \
  --compress=9 --verbose \
  > "$BACKUP_DIR/full_backup_$TIMESTAMP.sql.gz"

# Verify backup
if [ $? -eq 0 ]; then
    echo "✅ Backup completed successfully: full_backup_$TIMESTAMP.sql.gz"

    # Upload to cloud storage (optional)
    aws s3 cp "$BACKUP_DIR/full_backup_$TIMESTAMP.sql.gz" \
      s3://tourism-db-backups/$(date +"%Y/%m/%d")/ \
      --storage-class STANDARD_IA
else
    echo "❌ Backup failed"
    exit 1
fi

# Cleanup old backups
find $BACKUP_DIR -name "full_backup_*.sql.gz" -mtime +$RETENTION_DAYS -delete

# Send notification
curl -X POST "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK" \
  -H 'Content-type: application/json' \
  --data "{\"text\":\"Tourism DB backup completed: $TIMESTAMP\"}"
```

### Point-in-Time Recovery Setup

```bash
# Enable WAL archiving in postgresql.conf
wal_level = replica
archive_mode = on
archive_command = 'cp %p /data/wal_archive/%f'
max_wal_senders = 3
```

### Disaster Recovery Procedures

1. **Database Corruption Recovery**
   ```bash
   # Stop services
   sudo systemctl stop tourism-update supervisor nginx

   # Restore from latest backup
   dropdb tourism_production
   createdb tourism_production
   gunzip -c /data/backups/latest_backup.sql.gz | psql tourism_production

   # Restart services
   sudo systemctl start postgresql tourism-update supervisor nginx
   ```

2. **Complete System Recovery**
   ```bash
   # Restore system from backup
   ./deploy.sh

   # Restore database
   psql -c "DROP DATABASE IF EXISTS tourism_production;"
   psql -c "CREATE DATABASE tourism_production;"
   pg_restore -d tourism_production /data/backups/latest_backup.sql.gz

   # Verify system health
   python tourism_update_cli.py status
   ```

## Security Considerations

### Network Security

```bash
# Firewall configuration (UFW)
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow ssh
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow from 10.0.0.0/8 to any port 5432  # PostgreSQL
sudo ufw enable
```

### Database Security

```sql
-- Create read-only user for monitoring
CREATE USER tourism_readonly WITH PASSWORD 'secure_password';
GRANT CONNECT ON DATABASE tourism_production TO tourism_readonly;
GRANT USAGE ON SCHEMA public TO tourism_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tourism_readonly;

-- Revoke unnecessary permissions
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON DATABASE tourism_production FROM PUBLIC;

-- Enable SSL
ALTER SYSTEM SET ssl = on;
ALTER SYSTEM SET ssl_cert_file = '/etc/ssl/certs/server.crt';
ALTER SYSTEM SET ssl_key_file = '/etc/ssl/private/server.key';
```

### Application Security

```python
# Environment variables for sensitive data
import os
from cryptography.fernet import Fernet

# Encryption for sensitive configuration
ENCRYPTION_KEY = os.environ.get('TOURISM_DB_ENCRYPTION_KEY')
cipher_suite = Fernet(ENCRYPTION_KEY)

# Secure password handling
DATABASE_PASSWORD = cipher_suite.decrypt(
    os.environ.get('ENCRYPTED_DB_PASSWORD').encode()
).decode()

# Input validation and sanitization
def validate_input(data):
    # Implement input validation
    pass

# Rate limiting
from flask_limiter import Limiter
limiter = Limiter(
    app,
    key_func=lambda: request.remote_addr,
    default_limits=["200 per day", "50 per hour"]
)
```

## Performance Optimization

### Database Tuning

```sql
-- Analyze query performance
SELECT query, calls, total_time, mean_time, rows
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 20;

-- Index optimization
ANALYZE;
REINDEX DATABASE tourism_production;

-- Vacuum and analyze
VACUUM ANALYZE;

-- Partition large tables
CREATE TABLE logies_partitioned (
    LIKE logies INCLUDING ALL
) PARTITION BY RANGE (created_at);

CREATE TABLE logies_2024 PARTITION OF logies_partitioned
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
```

### Application Optimization

```python
# Connection pooling
from sqlalchemy.pool import QueuePool

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=30,
    pool_timeout=30,
    pool_recycle=3600,
    pool_pre_ping=True
)

# Query optimization
@cached(cache=LRUCache(maxsize=1000), ttl=300)
def get_cached_data(query_hash):
    return execute_query(query_hash)

# Async processing
import asyncio
import aiohttp

async def process_updates_async():
    async with aiohttp.ClientSession() as session:
        tasks = [process_batch(session, batch) for batch in batches]
        await asyncio.gather(*tasks)
```

## Troubleshooting

### Common Issues

#### High Memory Usage

```bash
# Check memory usage
free -h
ps aux --sort=-%mem | head -10

# PostgreSQL memory tuning
# Reduce shared_buffers if memory is low
# Adjust work_mem and maintenance_work_mem
```

#### Slow Queries

```sql
-- Find slow queries
SELECT query, calls, total_time, mean_time
FROM pg_stat_statements
WHERE mean_time > 1000  -- More than 1 second
ORDER BY mean_time DESC;

-- Check for missing indexes
SELECT schemaname, tablename, seq_scan, seq_tup_read
FROM pg_stat_user_tables
WHERE seq_scan > 0 AND seq_tup_read > 10000;
```

#### Connection Issues

```bash
# Check PostgreSQL connections
sudo -u postgres psql -c "SELECT count(*) FROM pg_stat_activity;"

# Check connection limits
sudo -u postgres psql -c "SHOW max_connections;"

# Monitor connection pool
python -c "
from update_system.database import get_engine
engine = get_engine()
print(f'Pool size: {engine.pool.size()}')
print(f'Checked out: {engine.pool.checkedout()}')
"
```

### Health Checks

```bash
#!/bin/bash
# health_check.sh

# Check services
services=("postgresql" "nginx" "supervisor" "redis-server")
for service in "${services[@]}"; do
    if systemctl is-active --quiet $service; then
        echo "✅ $service is running"
    else
        echo "❌ $service is not running"
    fi
done

# Check disk space
df -h | awk '$5 > 80 {print "⚠️ High disk usage: " $0}'

# Check database connectivity
python -c "
import psycopg2
try:
    conn = psycopg2.connect(
        host='localhost',
        database='tourism_production',
        user='tourism_user'
    )
    print('✅ Database connection successful')
    conn.close()
except Exception as e:
    print(f'❌ Database connection failed: {e}')
"

# Check API health
curl -f http://localhost:5000/health || echo "❌ API health check failed"
```

### Log Analysis

```bash
# Analyze error patterns
grep -i error /var/log/tourism_db/*.log | \
  awk '{print $4}' | sort | uniq -c | sort -nr

# Check performance metrics
grep "duration_ms" /var/log/tourism_db/tourism_db_performance.log | \
  jq '.duration_ms' | \
  awk '{sum+=$1; count++} END {print "Avg:", sum/count, "ms"}'

# Monitor real-time logs
tail -f /var/log/tourism_db/tourism_db.log | jq .
```

This deployment guide provides comprehensive instructions for setting up, scaling, and maintaining the Tourism Database system in production environments. Follow security best practices and monitor the system continuously for optimal performance.