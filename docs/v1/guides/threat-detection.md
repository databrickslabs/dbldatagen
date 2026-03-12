---
sidebar_position: 8
title: "Threat Detection Dataset"
description: "Synthetic cybersecurity dataset for SOC analysis — credential stuffing, lateral movement, and data exfiltration scenarios"
keywords: [dbldatagen, cybersecurity, threat detection, SOC, SIEM, credential stuffing, lateral movement, data exfiltration, security alerts]
---

# Threat Detection: Multi-Stage Attack Simulation

> **TL;DR:** Generate a 5-table security dataset (hosts, users, auth_events, network_flows, security_alerts) that models a realistic enterprise under a multi-stage attack. Then use SQL/PySpark to hunt for credential stuffing, lateral movement, and data exfiltration.

## The Threat Scenario

This dataset simulates a common advanced persistent threat (APT) attack chain against a mid-sized enterprise:

```
Stage 1: Credential Stuffing
  Attacker uses stolen credential lists to brute-force user accounts.
  → High auth failure rates for a small set of targeted accounts.

Stage 2: Lateral Movement
  Compromised accounts are used to pivot across internal hosts.
  → Unusual network flows: one host contacting many destinations.

Stage 3: Data Exfiltration
  Attacker locates sensitive data and transfers it out in large chunks.
  → Anomalous high-volume outbound transfers from restricted-zone hosts.
```

The Zipf-distributed foreign keys naturally create this pattern — a small number of users and hosts generate disproportionate activity, mirroring how real compromised accounts behave.

## The Schema

```
hosts (200 rows)                    users (500 rows)
  ├── auth_events.host_id             ├── auth_events.user_id (Zipf 2.0)
  ├── network_flows.src_host_id       └── security_alerts.user_id (Zipf 2.5)
  ├── network_flows.dst_host_id
  └── security_alerts.host_id

auth_events (50K rows)    network_flows (100K rows)    security_alerts (5K rows)
```

:::info What This Demonstrates
- **Zipf distributions** on FKs to simulate compromised accounts generating heavy traffic
- **Weighted values** for alert severity (many LOW, few CRITICAL — realistic SOC volumes)
- **Multiple FK refs** to the same parent (src_host_id and dst_host_id both reference hosts)
- **Cross-table correlation** for threat hunting joins
:::

## Generate the Dataset

### YAML Plan

```yaml
seed: 1000

tables:
  - name: hosts
    rows: 200
    primary_key: {columns: [host_id]}
    columns:
      - name: host_id
        gen: {strategy: pattern, template: "HOST-{seq:4}"}
      - name: hostname
        gen: {strategy: faker, provider: user_name}
      - name: ip_address
        gen: {strategy: faker, provider: ipv4}
      - name: os
        dtype: string
        gen:
          strategy: values
          values: [Windows 11, Windows Server 2022, Ubuntu 22.04, RHEL 9, macOS 14]
      - name: zone
        dtype: string
        gen:
          strategy: values
          values: [DMZ, INTERNAL, RESTRICTED, GUEST]
      - name: criticality
        dtype: string
        gen:
          strategy: values
          values: [LOW, MEDIUM, HIGH, CRITICAL]

  - name: users
    rows: 500
    primary_key: {columns: [user_id]}
    columns:
      - name: user_id
        gen: {strategy: pattern, template: "USR-{seq:5}"}
      - name: username
        gen: {strategy: faker, provider: user_name}
      - name: email
        gen: {strategy: faker, provider: email}
      - name: department
        dtype: string
        gen:
          strategy: values
          values: [Engineering, Finance, HR, IT, Marketing, Executive, Legal, Operations]
      - name: role
        dtype: string
        gen:
          strategy: values
          values: [user, user, user, admin, service_account]
      - name: mfa_enabled
        dtype: boolean
        gen:
          strategy: values
          values: [true, true, true, false]

  - name: auth_events
    rows: 50000
    primary_key: {columns: [event_id]}
    columns:
      - name: event_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}
      - name: user_id
        gen: {strategy: constant, value: null}
        foreign_key:
          ref: users.user_id
          distribution: {type: zipf, exponent: 2.0}
      - name: host_id
        gen: {strategy: constant, value: null}
        foreign_key: {ref: hosts.host_id}
      - name: source_ip
        gen: {strategy: faker, provider: ipv4}
      - name: auth_method
        dtype: string
        gen:
          strategy: values
          values: [password, password, ssh_key, kerberos, certificate, mfa]
      - name: result
        dtype: string
        gen:
          strategy: values
          values: [SUCCESS, SUCCESS, SUCCESS, SUCCESS, FAILURE, FAILURE, LOCKED_OUT]
      - name: event_time
        dtype: timestamp
        gen: {strategy: timestamp, start: "2025-01-01", end: "2025-12-31"}

  - name: network_flows
    rows: 100000
    primary_key: {columns: [flow_id]}
    columns:
      - name: flow_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}
      - name: src_host_id
        gen: {strategy: constant, value: null}
        foreign_key: {ref: hosts.host_id}
      - name: dst_host_id
        gen: {strategy: constant, value: null}
        foreign_key: {ref: hosts.host_id}
      - name: dst_port
        dtype: int
        gen:
          strategy: values
          values: [22, 53, 80, 443, 445, 1433, 3306, 3389, 5432, 8080, 8443]
      - name: protocol
        dtype: string
        gen:
          strategy: values
          values: [TCP, TCP, TCP, UDP, ICMP]
      - name: bytes_sent
        dtype: long
        gen: {strategy: range, min: 64, max: 5000000}
      - name: bytes_received
        dtype: long
        gen: {strategy: range, min: 64, max: 5000000}
      - name: duration_ms
        dtype: int
        gen: {strategy: range, min: 1, max: 300000}
      - name: action
        dtype: string
        gen:
          strategy: values
          values: [ALLOW, ALLOW, ALLOW, DENY, DROP]
      - name: event_time
        dtype: timestamp
        gen: {strategy: timestamp, start: "2025-01-01", end: "2025-12-31"}

  - name: security_alerts
    rows: 5000
    primary_key: {columns: [alert_id]}
    columns:
      - name: alert_id
        dtype: long
        gen: {strategy: sequence, start: 1, step: 1}
      - name: host_id
        gen: {strategy: constant, value: null}
        foreign_key: {ref: hosts.host_id}
      - name: user_id
        gen: {strategy: constant, value: null}
        foreign_key:
          ref: users.user_id
          distribution: {type: zipf, exponent: 2.5}
      - name: alert_type
        dtype: string
        gen:
          strategy: values
          values:
            - BRUTE_FORCE
            - PORT_SCAN
            - DATA_EXFILTRATION
            - MALWARE_DETECTED
            - PRIVILEGE_ESCALATION
            - LATERAL_MOVEMENT
            - C2_COMMUNICATION
            - ANOMALOUS_LOGIN
      - name: severity
        dtype: string
        gen:
          strategy: values
          values: [INFO, LOW, MEDIUM, HIGH, CRITICAL]
          distribution:
            type: weighted
            weights: {INFO: 0.10, LOW: 0.30, MEDIUM: 0.35, HIGH: 0.18, CRITICAL: 0.07}
      - name: status
        dtype: string
        gen:
          strategy: values
          values: [NEW, NEW, INVESTIGATING, RESOLVED, FALSE_POSITIVE]
      - name: description
        gen: {strategy: faker, provider: sentence}
      - name: event_time
        dtype: timestamp
        gen: {strategy: timestamp, start: "2025-01-01", end: "2025-12-31"}
```

### Python API

```python
from dbldatagen.v1 import (
    generate, DataGenPlan, TableSpec, PrimaryKey, ColumnSpec,
    pk_auto, pk_pattern, fk, faker, integer, text, timestamp,
)
from dbldatagen.v1.schema import DataType, ValuesColumn, WeightedValues, Zipf

plan = DataGenPlan(
    seed=1000,
    tables=[
        TableSpec(name="hosts", rows=200,
                  primary_key=PrimaryKey(columns=["host_id"]),
                  columns=[
                      pk_pattern("host_id", "HOST-{seq:4}"),
                      faker("hostname", "user_name"),
                      faker("ip_address", "ipv4"),
                      text("os", ["Windows 11", "Windows Server 2022",
                                   "Ubuntu 22.04", "RHEL 9", "macOS 14"]),
                      text("zone", ["DMZ", "INTERNAL", "RESTRICTED", "GUEST"]),
                      text("criticality", ["LOW", "MEDIUM", "HIGH", "CRITICAL"]),
                  ]),
        TableSpec(name="users", rows=500,
                  primary_key=PrimaryKey(columns=["user_id"]),
                  columns=[
                      pk_pattern("user_id", "USR-{seq:5}"),
                      faker("username", "user_name"),
                      faker("email", "email"),
                      text("department", ["Engineering", "Finance", "HR", "IT",
                                           "Marketing", "Executive", "Legal", "Operations"]),
                      text("role", ["user", "user", "user", "admin", "service_account"]),
                      ColumnSpec(name="mfa_enabled", dtype=DataType.BOOLEAN,
                                gen=ValuesColumn(values=[True, True, True, False])),
                  ]),
        TableSpec(name="auth_events", rows=50_000,
                  primary_key=PrimaryKey(columns=["event_id"]),
                  columns=[
                      pk_auto("event_id"),
                      fk("user_id", "users.user_id", distribution=Zipf(exponent=2.0)),
                      fk("host_id", "hosts.host_id"),
                      faker("source_ip", "ipv4"),
                      text("auth_method", ["password", "password", "ssh_key",
                                            "kerberos", "certificate", "mfa"]),
                      text("result", ["SUCCESS", "SUCCESS", "SUCCESS", "SUCCESS",
                                       "FAILURE", "FAILURE", "LOCKED_OUT"]),
                      timestamp("event_time", start="2025-01-01", end="2025-12-31"),
                  ]),
        TableSpec(name="network_flows", rows=100_000,
                  primary_key=PrimaryKey(columns=["flow_id"]),
                  columns=[
                      pk_auto("flow_id"),
                      fk("src_host_id", "hosts.host_id"),
                      fk("dst_host_id", "hosts.host_id"),
                      ColumnSpec(name="dst_port", dtype=DataType.INT,
                                gen=ValuesColumn(values=[22, 53, 80, 443, 445,
                                                         1433, 3306, 3389, 5432, 8080, 8443])),
                      text("protocol", ["TCP", "TCP", "TCP", "UDP", "ICMP"]),
                      integer("bytes_sent", min=64, max=5_000_000),
                      integer("bytes_received", min=64, max=5_000_000),
                      integer("duration_ms", min=1, max=300_000),
                      text("action", ["ALLOW", "ALLOW", "ALLOW", "DENY", "DROP"]),
                      timestamp("event_time", start="2025-01-01", end="2025-12-31"),
                  ]),
        TableSpec(name="security_alerts", rows=5_000,
                  primary_key=PrimaryKey(columns=["alert_id"]),
                  columns=[
                      pk_auto("alert_id"),
                      fk("host_id", "hosts.host_id"),
                      fk("user_id", "users.user_id", distribution=Zipf(exponent=2.5)),
                      text("alert_type", [
                          "BRUTE_FORCE", "PORT_SCAN", "DATA_EXFILTRATION",
                          "MALWARE_DETECTED", "PRIVILEGE_ESCALATION",
                          "LATERAL_MOVEMENT", "C2_COMMUNICATION", "ANOMALOUS_LOGIN",
                      ]),
                      text("severity", ["INFO", "LOW", "MEDIUM", "HIGH", "CRITICAL"],
                           distribution=WeightedValues(weights={
                               "INFO": 0.10, "LOW": 0.30, "MEDIUM": 0.35,
                               "HIGH": 0.18, "CRITICAL": 0.07,
                           })),
                      text("status", ["NEW", "NEW", "INVESTIGATING",
                                       "RESOLVED", "FALSE_POSITIVE"]),
                      faker("description", "sentence"),
                      timestamp("event_time", start="2025-01-01", end="2025-12-31"),
                  ]),
    ],
)

dfs = generate(spark, plan)
```

**Output:**

```
hosts:              200
users:              500
auth_events:     50,000
network_flows:  100,000
security_alerts:  5,000
```

---

## Hunting the Attack Chain

### Stage 1: Detect Credential Stuffing

Credential stuffing attacks produce a spike in auth failures for targeted accounts. Look for users with abnormally high failure rates — especially those without MFA.

```sql
-- Users with > 40% auth failure rate
SELECT
    u.user_id,
    u.username,
    u.department,
    u.role,
    u.mfa_enabled,
    a.total_attempts,
    a.failures,
    ROUND(a.failures / a.total_attempts, 2) AS failure_rate
FROM users u
JOIN (
    SELECT
        user_id,
        COUNT(*) AS total_attempts,
        SUM(CASE WHEN result != 'SUCCESS' THEN 1 ELSE 0 END) AS failures
    FROM auth_events
    GROUP BY user_id
) a ON u.user_id = a.user_id
WHERE a.failures / a.total_attempts > 0.40
ORDER BY a.failures DESC
LIMIT 20;
```

**What to look for:**
- Accounts with failure rates well above the baseline (~28% given 3/7 non-SUCCESS values).
- Service accounts and admin accounts targeted (higher Zipf exponent = more concentrated).
- Accounts with `mfa_enabled = false` — these are the most vulnerable.

**Follow-up investigation:**
```sql
-- Timeline of failures for the top suspect account
SELECT
    DATE_TRUNC('hour', event_time) AS hour,
    COUNT(*) AS attempts,
    SUM(CASE WHEN result = 'FAILURE' THEN 1 ELSE 0 END) AS failures,
    SUM(CASE WHEN result = 'LOCKED_OUT' THEN 1 ELSE 0 END) AS lockouts
FROM auth_events
WHERE user_id = '<suspect_user_id>'
GROUP BY 1
ORDER BY 1;
```

---

### Stage 2: Detect Lateral Movement

After gaining initial access, attackers move laterally by connecting from a compromised host to many other internal hosts — often using protocols like RDP (3389), SMB (445), or SSH (22).

```sql
-- Hosts connecting to 50+ unique destinations
SELECT
    nf.src_host_id,
    h.hostname,
    h.zone AS src_zone,
    COUNT(DISTINCT nf.dst_host_id) AS unique_destinations,
    COUNT(*) AS total_flows,
    SUM(nf.bytes_sent) AS total_bytes_sent
FROM network_flows nf
JOIN hosts h ON nf.src_host_id = h.host_id
GROUP BY nf.src_host_id, h.hostname, h.zone
HAVING COUNT(DISTINCT nf.dst_host_id) > 50
ORDER BY unique_destinations DESC;
```

**What to look for:**
- Hosts in INTERNAL or GUEST zones reaching many hosts in RESTRICTED zones.
- High fan-out (many unique destinations) is unusual for normal workstations.
- Traffic on lateral-movement ports (445/SMB, 3389/RDP, 22/SSH).

**Drill into suspicious protocols:**
```sql
-- Lateral movement via RDP/SMB from suspicious hosts
SELECT
    src_host_id,
    dst_host_id,
    dst_port,
    protocol,
    bytes_sent,
    event_time
FROM network_flows
WHERE src_host_id = '<suspect_host_id>'
  AND dst_port IN (445, 3389, 22)
ORDER BY event_time;
```

---

### Stage 3: Detect Data Exfiltration

Exfiltration typically involves large outbound transfers — far above normal baseline traffic. Look for single flows with unusually high byte counts, especially from restricted-zone hosts.

```sql
-- Large outbound transfers (> 4 MB in a single flow)
SELECT
    nf.flow_id,
    h_src.hostname AS src_hostname,
    h_src.zone AS src_zone,
    h_dst.hostname AS dst_hostname,
    h_dst.zone AS dst_zone,
    nf.dst_port,
    nf.bytes_sent,
    nf.duration_ms,
    nf.event_time
FROM network_flows nf
JOIN hosts h_src ON nf.src_host_id = h_src.host_id
JOIN hosts h_dst ON nf.dst_host_id = h_dst.host_id
WHERE nf.bytes_sent > 4000000
ORDER BY nf.bytes_sent DESC
LIMIT 20;
```

**What to look for:**
- Large transfers from RESTRICTED → DMZ or RESTRICTED → GUEST (data leaving the secure perimeter).
- Unusual ports: data exfiltration often uses HTTPS (443) or DNS (53) to blend in.
- High bytes_sent with low bytes_received (one-way transfer, not normal request/response).

**Aggregate daily exfiltration volume by zone:**
```sql
-- Daily outbound volume from restricted zone
SELECT
    DATE_TRUNC('day', nf.event_time) AS day,
    SUM(nf.bytes_sent) / 1e6 AS mb_sent,
    COUNT(*) AS flow_count
FROM network_flows nf
JOIN hosts h ON nf.src_host_id = h.host_id
WHERE h.zone = 'RESTRICTED'
  AND nf.bytes_sent > 1000000
GROUP BY 1
ORDER BY 1;
```

---

### Cross-Stage Correlation: The Kill Chain

The most powerful analysis correlates signals across all three stages. A user with brute-force alerts, auth failures, AND associated high-volume network flows is a strong indicator of compromise.

```sql
-- Users appearing in both security alerts (HIGH/CRITICAL) and auth failures
WITH alert_users AS (
    SELECT DISTINCT user_id
    FROM security_alerts
    WHERE severity IN ('HIGH', 'CRITICAL')
),
failure_users AS (
    SELECT user_id, COUNT(*) AS failures
    FROM auth_events
    WHERE result != 'SUCCESS'
    GROUP BY user_id
    HAVING COUNT(*) > 20
)
SELECT
    u.user_id,
    u.username,
    u.department,
    u.role,
    f.failures AS auth_failures,
    sa.alert_count,
    sa.alert_types
FROM alert_users a
JOIN failure_users f ON a.user_id = f.user_id
JOIN users u ON u.user_id = a.user_id
JOIN (
    SELECT
        user_id,
        COUNT(*) AS alert_count,
        COLLECT_SET(alert_type) AS alert_types
    FROM security_alerts
    WHERE severity IN ('HIGH', 'CRITICAL')
    GROUP BY user_id
) sa ON sa.user_id = a.user_id
ORDER BY f.failures DESC;
```

**Building a complete incident timeline:**
```sql
-- Unified timeline for a suspect user across all event sources
SELECT event_time, 'AUTH' AS source, result AS detail
FROM auth_events WHERE user_id = '<suspect>'

UNION ALL

SELECT event_time, 'ALERT' AS source,
       CONCAT(alert_type, ' (', severity, ')') AS detail
FROM security_alerts WHERE user_id = '<suspect>'

ORDER BY event_time;
```

---

## Why Zipf Distributions Matter

The Zipf distribution is the key to making this dataset realistic for threat hunting:

| FK Relationship | Exponent | Effect |
|---|---|---|
| `auth_events.user_id` | 2.0 | A few users have thousands of events — mimics compromised accounts |
| `security_alerts.user_id` | 2.5 | Even more concentrated — a handful of users trigger most alerts |
| `network_flows.src_host_id` | 1.2 (default) | Moderate skew — some hosts more active than others |

With uniform distribution, every user/host would have roughly the same activity level — making threat detection trivially easy or impossible (no signal). Zipf creates the realistic "needle in a haystack" pattern SOC analysts actually face.

---

## Scaling Up

For a realistic enterprise SIEM workload, scale the row counts:

```yaml
tables:
  - name: hosts
    rows: 10000          # 10K hosts
  - name: users
    rows: 50000          # 50K employees
  - name: auth_events
    rows: 50000000       # 50M auth events (a week of logs)
  - name: network_flows
    rows: 500000000      # 500M network flows
  - name: security_alerts
    rows: 100000         # 100K alerts
```

On Databricks serverless, 500M rows generates in under 5 minutes.

---

## Next Steps

- **CDC for real-time alerts**: Use [CDC mode](../cdc/overview.md) to generate batches of new auth events and alerts over time, simulating a live SIEM feed.
- **Streaming**: Use [streaming mode](../data-generation/streaming.md) to continuously generate network flow data for testing streaming pipelines.
- **Custom distributions**: See [Distributions guide](../core-concepts/distributions.md) for Normal, LogNormal, and custom weighted distributions.
- **Write to Delta**: Save directly to Databricks tables for use with AI/BI dashboards.

```python
for name, df in dfs.items():
    df.write.mode("overwrite").format("delta").saveAsTable(f"security.{name}")
```
