[Back to index](./README.md)

# System Functions

---

### Function: VERSION
**Description:** Return engine version string.
**Inputs:** none
**Output:** VARCHAR
**Example:**
```sql
SELECT VERSION() AS v;
-- Result: 'sql-elasticsearch-engine 1.0.0'
```

### Function: USER
**Description:** Return current user (context-specific).
**Inputs:** none
**Output:** VARCHAR
**Example:**
```sql
SELECT USER() AS current_user;
```

[Back to index](./README.md)
