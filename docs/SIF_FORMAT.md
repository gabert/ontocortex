# SIF — Structured Intent Format

SIF is the tool surface the LLM uses to interact with a domain database.
Instead of writing SQL, the model emits structured operation dicts in
ontology vocabulary — class names, property names, relation names — and a
deterministic Python layer (`agentcore/sif.py`) translates them to
parameterized SQL against the physical schema.

**Design goals**

- The model cannot invent entities, fields, or relations — they're enum-constrained at tool-generation time.
- Every valid SIF batch is either fully applied or fully rolled back (single DB transaction).
- Errors come back as readable text the model can reason about, not exceptions.
- One SIF document maps cleanly to ontology concepts — no physical schema leaks into the prompt.

---

## 1. Tool envelope

The model calls a single tool, `submit_sif`, with one argument:

```json
{
  "operations": [ <op1>, <op2>, ... ]
}
```

All operations in a single call run inside **one database transaction**.
Any failing op rolls back the whole batch — the agent sees a "rolled back"
notice in the error text and retries cleanly.

---

## 2. Operation types

Every op is an object with an `op` field. The following values are valid:

| op | Purpose |
|---|---|
| `query`  | Read rows (with joins, filters, aggregates, sort, limit) |
| `create` | Insert one row; optional FK resolution via related lookups |
| `update` | Modify rows matching filters |
| `delete` | Remove rows matching filters |
| `link`   | Attach two existing entities across a many-to-many relation |
| `unlink` | Detach two existing entities across a many-to-many relation |
| `action` | Call a domain-registered Python function (business logic) |

---

## 3. Common fields

| Field | Applies to | Shape | Meaning |
|---|---|---|---|
| `entity`     | query / create / update / delete | string | Ontology class name (e.g. `Customer`, `Policy`) |
| `filters`    | query / update / delete           | `{property: value}` | Equality filters on the main entity |
| `fields`     | query                             | `[property, ...]`    | Columns to return; omit for `*` |
| `data`       | create / update                   | `{property: value}` | Column values to set |
| `resolve`    | create                            | `{relation: {entity, filters}}` | Resolve direct-FK related entities by lookup |
| `relations`  | query                             | `[{rel, entity, filters?}]` | JOIN chain for reads |
| `aggregate`  | query                             | `{fn, field?}`        | `count`, `sum`, `avg`, `min`, `max` |
| `sort`       | query                             | `{field, dir}`        | `asc` / `desc` |
| `limit`      | query                             | int                  | Max rows |
| `relation`   | link / unlink                     | string               | M2M relation name (must traverse a junction table) |
| `from`, `to` | link / unlink                     | `{entity, filters}`   | The two endpoints; filters must narrow to exactly one row each |
| `action`     | action                            | string               | Registered action name |
| `params`     | action                            | object               | Arbitrary params passed to the action function |

Field constraints are enforced both at tool-schema time (enum injection on
`entity`, `relation`, `from.entity`, `to.entity`, `action`) and at runtime
via `validate_operations()`.

---

## 4. JSON Schema (summary)

The authoritative schema is `agentcore/sif_schema.json`. Runtime values
for `entity`, `relation`, `action`, and related enums are injected by
`build_sif_tool(schema_map)` from the current domain's ontology.

```json
{
  "type": "object",
  "required": ["operations"],
  "properties": {
    "operations": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["op"],
        "properties": {
          "op": {"enum": ["query", "create", "update", "delete",
                          "link", "unlink", "action"]},
          "entity":   {"type": "string"},
          "filters":  {"type": "object"},
          "fields":   {"type": "array", "items": {"type": "string"}},
          "data":     {"type": "object"},
          "resolve":  {"type": "object"},
          "relations": {
            "type": "array",
            "items": {
              "type": "object",
              "required": ["rel", "entity"],
              "properties": {
                "rel":     {"type": "string"},
                "entity":  {"type": "string"},
                "filters": {"type": "object"}
              }
            }
          },
          "relation": {"type": "string"},
          "from": {
            "type": "object",
            "required": ["entity", "filters"],
            "properties": {
              "entity":  {"type": "string"},
              "filters": {"type": "object"}
            }
          },
          "to": {
            "type": "object",
            "required": ["entity", "filters"],
            "properties": {
              "entity":  {"type": "string"},
              "filters": {"type": "object"}
            }
          },
          "aggregate": {
            "type": "object",
            "required": ["fn"],
            "properties": {
              "fn":    {"enum": ["count", "sum", "avg", "min", "max"]},
              "field": {"type": "string"}
            }
          },
          "sort": {
            "type": "object",
            "required": ["field"],
            "properties": {
              "field": {"type": "string"},
              "dir":   {"enum": ["asc", "desc"]}
            }
          },
          "limit":  {"type": "integer"},
          "action": {"type": "string"},
          "params": {"type": "object"}
        }
      }
    }
  }
}
```

---

## 5. Examples

### 5.1 Simple query with filter

> "Show me John Smith's active policies."

```json
{
  "operations": [
    {
      "op": "query",
      "entity": "Policy",
      "filters": {"status": "active"},
      "relations": [
        {"rel": "hasPolicy", "entity": "Customer",
         "filters": {"first_name": "John", "last_name": "Smith"}}
      ]
    }
  ]
}
```

### 5.2 Query with specific fields, sort, limit

```json
{
  "operations": [
    {
      "op": "query",
      "entity": "Claim",
      "fields": ["claim_number", "claim_date", "claim_amount", "status"],
      "filters": {"status": "claim_pending"},
      "sort": {"field": "claim_date", "dir": "desc"},
      "limit": 10
    }
  ]
}
```

### 5.3 Aggregation

> "How many active policies does John have?"

```json
{
  "operations": [
    {
      "op": "query",
      "entity": "Policy",
      "filters": {"status": "active"},
      "relations": [
        {"rel": "hasPolicy", "entity": "Customer",
         "filters": {"email": "john.smith@example.org"}}
      ],
      "aggregate": {"fn": "count"}
    }
  ]
}
```

### 5.4 Create with direct-FK resolve

> "File a $5,000 accident claim on policy POL-2024-00001."

```json
{
  "operations": [
    {
      "op": "create",
      "entity": "Claim",
      "data": {
        "claim_date": "2026-04-15",
        "claim_amount": 5000,
        "claim_type": "accident",
        "status": "claim_pending"
      },
      "resolve": {
        "hasClaim": {
          "entity": "Policy",
          "filters": {"policy_number": "POL-2024-00001"}
        }
      }
    }
  ]
}
```

`resolve` only works for direct-FK relations (1-to-many). For M2M, use
`link` instead.

### 5.5 Update

```json
{
  "operations": [
    {
      "op": "update",
      "entity": "Claim",
      "filters": {"claim_number": "CLM-2026-0042"},
      "data": {"status": "claim_approved", "approved_amount": 4500}
    }
  ]
}
```

### 5.6 Delete

```json
{
  "operations": [
    {
      "op": "delete",
      "entity": "Appointment",
      "filters": {"appointment_id": 17}
    }
  ]
}
```

### 5.7 Link an M2M relation (add cosigner to an existing loan)

```json
{
  "operations": [
    {
      "op": "link",
      "relation": "hasCosigner",
      "from": {"entity": "Loan",     "filters": {"loan_number": "SLN-2026-00001"}},
      "to":   {"entity": "Cosigner", "filters": {"email": "jane@example.org"}}
    }
  ]
}
```

Endpoint order is irrelevant — the validator accepts `{Loan, Cosigner}`
either way. Both filter sets must narrow to exactly one row.

If the link already exists, the op succeeds with an "already linked"
message — it is idempotent.

### 5.8 Unlink

```json
{
  "operations": [
    {
      "op": "unlink",
      "relation": "hasCosigner",
      "from": {"entity": "Loan",     "filters": {"loan_number": "SLN-2026-00001"}},
      "to":   {"entity": "Cosigner", "filters": {"email": "jane@example.org"}}
    }
  ]
}
```

Also idempotent — unlinking a non-existent link returns "not linked".

### 5.9 Transactional batch: create + link atomically

> "Add Jane Doe as a new cosigner on loan SLN-2026-00001."

```json
{
  "operations": [
    {
      "op": "create",
      "entity": "Cosigner",
      "data": {
        "first_name": "Jane",
        "last_name": "Doe",
        "email": "jane@example.org",
        "credit_score": 720
      }
    },
    {
      "op": "link",
      "relation": "hasCosigner",
      "from": {"entity": "Loan",     "filters": {"loan_number": "SLN-2026-00001"}},
      "to":   {"entity": "Cosigner", "filters": {"email": "jane@example.org"}}
    }
  ]
}
```

Both ops run in one transaction. If the link fails (e.g. the loan doesn't
exist), the Cosigner insert is rolled back — no orphan row is left
behind.

### 5.10 Action (domain business logic)

```json
{
  "operations": [
    {
      "op": "action",
      "action": "quote_premium",
      "params": {
        "customer_email": "john@example.org",
        "vehicle_vin": "1HGCM82633A004352",
        "coverage_type": "comprehensive"
      }
    }
  ]
}
```

Actions are Python functions registered per-domain in
`domains/<name>/actions/`. They receive `(params, db_config, schema_map)`
and return a string the agent includes in its response.

---

## 6. Validation & error handling

`validate_operations()` runs on every batch **before** any SQL executes.
It checks:

- Op type is one of the valid set
- `entity` is a known ontology class
- Field references (`fields`, `filters`, `data`, `sort.field`,
  `aggregate.field`) exist on the target class
- Relations are known; their endpoint entities match
- `resolve` targets direct-FK relations only
- `link`/`unlink` targets M2M relations only; from/to entity pair matches
  the relation's domain/range; filter columns exist on both sides
- Actions are registered

On failure, the whole batch is rejected with a message listing every
error. Nothing is written. The agent reads the error, fixes the op, and
resubmits.

At execution time, database errors (constraint violations, etc.) are
caught per-op, the transaction is rolled back, and the agent receives:

```
Operation N FAILED:
<error detail>

All M earlier op(s) in this batch were rolled back —
the database is unchanged.

Decide how to proceed: correct the failing op, ask the user for missing
information, or try a different approach...
```

This is what keeps the self-correction loop productive: the agent always
knows exactly which op failed, what the error was, and that the database
is in a clean state.

---

## 7. What SIF deliberately does NOT support

Kept out by design to preserve the deterministic translation layer:

- **Arbitrary SQL** — no raw SQL escape hatch. Edge cases go through `action`.
- **OR / NOT / IN / range filters** — only AND-of-equalities. Extend the
  schema when a real use case demands it.
- **Subqueries or CTEs** — use multiple ops in one transactional batch.
- **GROUP BY with HAVING** — aggregates are single-value; multi-group
  aggregations are not yet expressible.
- **Joins beyond the ontology graph** — every join comes from a declared
  `ObjectProperty`, never from ad-hoc table relationships.

These limits are part of the value: they're what guarantee that every SIF
document the model emits is safe, scoped, and semantically anchored to
the ontology.
