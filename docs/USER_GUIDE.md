# OntoCortex User Guide

## Table of contents

1. [Introduction](#1-introduction)
2. [Setup](#2-setup)
3. [Customization](#3-customization)

---

## 1. Introduction

### What is OntoCortex?

OntoCortex is a framework for building domain-specific data applications driven by an OWL ontology. You define your domain once — as an ontology, a set of business rules, and an agent persona — and the framework generates everything else: database schema, test data, and a conversational agent that understands your domain.

### Two layers, one ontology

The framework produces two independent layers that share the same ontology as their source of truth:

**Layer 1: SIF Engine** — a deterministic ontology-to-SQL pipeline. It takes an OWL ontology, maps it to a physical database, and executes structured data operations (query, create, update, delete, link, unlink) with validation, column-level mapping, and identity scoping. No LLM is involved at runtime. The SIF Engine can be consumed by any frontend — a REST API, a rule engine, a CLI tool, or a batch process.

**Layer 2: LLM Agent** — a conversational layer built on top of the SIF Engine. It reads the ontology and business rules, understands natural language, and emits SIF operations. The agent is one consumer of the engine, not the engine itself. Swapping the LLM for a different model (or removing it entirely for programmatic use) does not affect the data pipeline.

This separation means:
- The SIF Engine is **testable, auditable, and deterministic** — every operation can be validated and logged without LLM involvement.
- The LLM Agent is **replaceable** — it only produces SIF, never SQL. Any system that can produce valid SIF operations can drive the engine.
- Identity scoping, validation, and constraint enforcement happen **below the LLM** — the agent cannot bypass them.

### Architecture overview

The system has two phases: **design time** (one-time setup) and **runtime** (serving users).

**Design time** — the Scaled Architect Pipeline:

```
ontology.ttl
     │
     ▼
  Planner  ──► schema_plan.yaml     (deterministic, no LLM)
     │
     ▼
  Builder  ──► module builds         (deterministic, no LLM)
     │
     ▼
 Reconciler ──► schema.json          (deterministic merge)
     │
     ├──► mapping.yaml               (ontology-to-physical-schema bridge)
     ├──► schema.sql                  (DDL for inspection)
     └──► seed_data.sql              (optional demo data, LLM-generated)
```

1. The **Planner** reads the ontology and produces a topology-only plan: tables, relationships, modules. Pure Python. Ambiguous decisions (FK direction, junction tables) can be resolved with `schema_overrides.yaml`.
2. The **Builder** maps each ontology datatype property to a column definition: XSD type to logical type (string, integer, decimal, boolean, date, datetime), with convention-based flags (not_null, required, unique). Pure Python.
3. The **Reconciler** merges module builds into a single `schema.json`, validates consistency, and generates a mapping file.

**Runtime** — the SIF Engine (deterministic, LLM-free):

```
SIF operation (ontology vocabulary)
     │
     ▼
  SIF Validator
     │  checks entity names, property names, required fields
     ▼
  SIF Translator
     │  converts ontology names to physical SQL using mapping
     ▼
  Identity Scoping
     │  injects user ownership filters (post-translation)
     ▼
  PostgreSQL
     │
     ▼
  Structured results
```

**Runtime** — the LLM Agent (one consumer of the SIF Engine):

```
User message
     │
     ▼
  ConversationAgent (LLM)
     │  reads ontology + business rules
     │  emits SIF operations
     │         │
     │         ▼
     │    SIF Engine (as above)
     │         │
     │         ▼
     │  receives structured results
     ▼
  Natural language answer
```

The LLM never generates SQL, never sees table names, and never handles user identity. It only speaks ontology vocabulary. Everything below the LLM is deterministic and independently usable.

### Key concepts

**Ontology** — A standard OWL/Turtle file that defines your domain's classes (entities), properties (fields), relationships, and allowed value sets. This is the single source of truth for both layers.

**SIF (Structured Intent Format)** — A JSON structure that expresses data operations (query, create, update, delete, link, unlink) in ontology vocabulary. The LLM agent emits SIF, but any system can produce it. The deterministic translator converts SIF to SQL.

**Mapping** — A YAML file (`data_sources/primary/mapping.yaml`) that bridges ontology names to physical database names. Generated automatically from the schema, but can be hand-edited to map onto an existing database.

**Data source** — A named connection to a database, defined in `domain.json` and resolved to connection credentials in `config.ini`.

**Identity scoping** — An application-boundary mechanism that restricts data access per user. Operates within the SIF Engine, below and independent of the LLM. The pipeline injects ownership filters into every SQL statement after translation.

---

## 2. Setup

### 2.1 Prerequisites

- Python 3.10+
- Docker (for PostgreSQL) — or a local PostgreSQL 15+ instance
- An Anthropic API key ([console.anthropic.com](https://console.anthropic.com/)) — needed for the LLM Agent and optional seed data generation; schema generation is fully deterministic and does not require an API key

### 2.2 Install

```bash
git clone https://github.com/gabert/ontocortex.git
cd ontocortex
pip install -r requirements.txt
docker-compose up -d
```

### 2.3 Configuration

Copy the template and edit:

```bash
cp config.ini.template config.ini
```

`config.ini` has five section types. All sections and all keys within them are required — there are no defaults.

#### `[anthropic]` — API key

```ini
[anthropic]
api_key = sk-ant-...your-key-here...
```

Alternatively, set the `ANTHROPIC_API_KEY` environment variable (takes precedence over the file).

#### `[models]` — LLM model selection

```ini
[models]
chat = claude-sonnet-4-6
seed_data = claude-sonnet-4-6
analyzer = claude-sonnet-4-6
```

| Key | Used by | Purpose |
|-----|---------|---------|
| `chat` | ConversationAgent | The model that talks to users |
| `seed_data` | Seed generator (`--seed`) | Produces realistic demo INSERT statements |
| `analyzer` | Error analyzer | Post-mortem analysis when the pipeline fails |

#### `[chat]` — Conversation agent settings

```ini
[chat]
max_tokens = 8192
max_retries = 3
retry_delay = 5
max_iterations = 20
```

| Key | Purpose |
|-----|---------|
| `max_tokens` | Maximum response tokens per LLM call |
| `max_retries` | Retry count on transient API errors (429/529) |
| `retry_delay` | Base delay between retries in seconds (multiplied by attempt number) |
| `max_iterations` | Maximum tool-use loop iterations per user message |

#### `[architect]` — Schema generation settings

```ini
[architect]
max_tokens = 4096
max_concurrency = 5
sdk_max_retries = 5
max_validation_attempts = 3
rows_per_table = 10
junction_rows = 5
```

| Key | Purpose |
|-----|---------|
| `max_tokens` | Maximum response tokens per seed data LLM call |
| `max_concurrency` | Maximum parallel LLM calls during seed data generation |
| `sdk_max_retries` | Anthropic SDK retry count for transient errors |
| `max_validation_attempts` | Validation feedback loop attempts (bad output triggers re-prompt) |
| `rows_per_table` | Number of seed data rows per entity table |
| `junction_rows` | Number of seed data rows per junction (many-to-many) table |

These settings are used only by the seed data generator (`--seed`). The schema builder itself is fully deterministic and ignores them.

#### `[data_source:NAME]` — Database connections

One section per database. The `NAME` must match the `store` value in a domain's `domain.json`.

```ini
[data_source:student_loans_demo]
dbname = student_loans_demo
user = postgres
password = postgres
host = localhost
port = 5432
```

Every section must contain all five keys. There is no inheritance or fallback between sections.

### 2.4 Generate a domain schema

This is a one-time step per domain. It sends the ontology through the architect pipeline and produces everything the runtime needs. The builder is fully deterministic by default — no LLM call, no API key needed.

```bash
# Phase 1: deterministic plan (no LLM)
python scripts/design_plan.py student_loans

# Phase 2+3: deterministic build + reconcile (no LLM)
python scripts/build_schema.py student_loans

# Optional: also generate demo seed data (requires API key)
python scripts/build_schema.py student_loans --seed
```

CLI options for `build_schema.py`:

| Flag | Purpose |
|------|---------|
| `--force` | Ignore cached builds, rebuild everything |
| `--seed` | Generate demo seed data (requires API key) |
| `--seed-rows N` | Override rows per table from config |
| `--source NAME` | Data source name for the mapping (default: `primary`) |

### 2.5 Set up the database and run

```bash
# Option A: auto-install on first run
python main.py student_loans

# Option B: explicit setup
python scripts/setup_database.py student_loans
python main.py student_loans

# Option C: web UI
streamlit run streamlit_app.py
```

On first launch, the app detects that the database doesn't exist and installs it automatically (creates the database, applies the schema, loads seed data if available).

### 2.6 Interactive CLI commands

| Command | Description |
|---------|-------------|
| `/list` | Show available domains |
| `/load <domain>` | Switch domain; installs DB if needed |
| `/reset` | Drop and recreate the current domain's database |
| `/new` | Clear conversation history (keep domain loaded) |
| `/help` | Show available commands |
| `/quit` | Exit |

---

## 3. Customization

### 3.1 Creating a new domain

A domain is a directory under `domains/` containing four files you write and a set of files the architect generates. Here is the minimal structure:

```
domains/my_domain/
    domain.json                  # Domain descriptor
    my_domain_ontology.ttl       # OWL ontology (source of truth)
    my_domain.rules              # Business rules (plain text)
    my_domain_prompt.txt         # Agent persona
```

After running the architect pipeline, the directory gains a per-data-source subdirectory:

```
domains/my_domain/
    data_sources/primary/
        overrides.yaml           # Optional planner hints (FK direction)
        mapping.yaml             # Ontology-to-physical mapping (generated or hand-written)
        _generated/
            my_domain_schema_plan.yaml
            my_domain_schema.json
            my_domain_schema.sql
            my_domain_seed_data.sql
            _builds/             # Per-module builder outputs
```

#### 3.1.1 domain.json

The domain descriptor. Minimal example:

```json
{
  "name": "My Domain Agent",
  "description": "One-line description of what this agent does",
  "ontology": "my_domain_ontology.ttl",
  "business_rules": "my_domain.rules",
  "system_prompt": "my_domain_prompt.txt"
}
```

Full example with all fields:

```json
{
  "name": "Student Loans Advisor",
  "description": "Student-loan portfolio management",
  "ontology": "student_loans_ontology.ttl",
  "business_rules": "student_loans.rules",
  "system_prompt": "student_loans_prompt.txt",
  "identity_entity": "Student",
  "data_sources": {
    "primary": {
      "store": "student_loans_demo",
      "source_dir": "data_sources/primary",
      "mapping": "mapping.yaml"
    }
  },
  "generated": {
    "schema_plan": "student_loans_schema_plan.yaml",
    "schema": "student_loans_schema.json",
    "seed_data": "student_loans_seed_data.sql"
  }
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `name` | yes | Display name for the agent |
| `description` | yes | Short description shown in domain listings |
| `ontology` | yes | Filename of the OWL/Turtle ontology |
| `business_rules` | yes | Filename of the business rules file (`.rules`) |
| `system_prompt` | yes | Filename of the agent persona text |
| `identity_entity` | no | Ontology class name for user scoping (e.g. `"Student"`, `"Owner"`) |
| `data_sources` | no | Named data sources (auto-populated by the architect) |
| `generated` | no | Pointers to generated artifacts (auto-populated by the architect) |

The `data_sources` and `generated` blocks are written automatically by `build_schema.py`. You only need to create the first five fields by hand.

Each data source entry has:

| Key | Description |
|-----|-------------|
| `store` | Pointer to a `[data_source:NAME]` section in `config.ini` |
| `source_dir` | Relative path to the data-source directory (default: `data_sources/<name>`) |
| `mapping` | Mapping filename within `source_dir` (default: `mapping.yaml`) |

#### 3.1.2 Ontology (.ttl)

A standard OWL/Turtle file. No custom annotations needed — the architect infers everything from standard OWL semantics.

**Structure:**

```turtle
@prefix rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix owl:  <http://www.w3.org/2002/07/owl#> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .
@prefix ex:   <https://example.org/ontology#> .

<https://example.org/ontology>
    a owl:Ontology ;
    rdfs:label "My Ontology" .
```

**Classes** — define your entities:

```turtle
ex:Customer a owl:Class ;
    rdfs:label "Customer" ;
    rdfs:comment "A customer of the business." .
```

**Datatype properties** — fields on entities:

```turtle
ex:firstName a owl:DatatypeProperty ;
    rdfs:label "firstName" ;
    rdfs:domain ex:Customer ;
    rdfs:range xsd:string .
```

Supported XSD types: `xsd:string`, `xsd:integer`, `xsd:decimal`, `xsd:boolean`, `xsd:date`, `xsd:dateTime`. The builder maps these to logical column types (string, integer, decimal, boolean, date, datetime). Additional XSD subtypes (`xsd:int`, `xsd:long`, `xsd:float`, `xsd:double`, `xsd:normalizedString`, etc.) are also supported.

A property can have multiple domains (shared across classes):

```turtle
ex:email a owl:DatatypeProperty ;
    rdfs:domain [ a owl:Class ; owl:unionOf ( ex:Customer ex:Vendor ) ] ;
    rdfs:range xsd:string .
```

**Object properties** — relationships between classes:

```turtle
ex:hasOrder a owl:ObjectProperty ;
    rdfs:label "hasOrder" ;
    rdfs:comment "A customer places orders." ;
    rdfs:domain ex:Order ;
    rdfs:range ex:Customer .
```

The planner infers FK direction from domain/range. For `hasOrder` above: Order is the domain (FK holder), Customer is the range (referenced). If the default direction is wrong, override it in `overrides.yaml` (see [3.4](#34-schema-overrides)).

**Value sets** — constrained enumerations:

```turtle
ex:OrderStatusValue a owl:Class ;
    rdfs:label "OrderStatusValue" ;
    owl:oneOf ( ex:pending ex:shipped ex:delivered ex:cancelled ) .
```

Reference a value set as the range of a datatype property:

```turtle
ex:orderStatus a owl:DatatypeProperty ;
    rdfs:domain ex:Order ;
    rdfs:range ex:OrderStatusValue .
```

The architect generates a lookup table with these values and a FK constraint.

**Many-to-many relationships** — use `rdfs:comment` to indicate that a junction table is needed. The planner also uses heuristics (if both sides can have multiple of each other), but an explicit comment removes ambiguity:

```turtle
ex:hasTag a owl:ObjectProperty ;
    rdfs:comment "Many-to-many: a product can have multiple tags, and a tag applies to multiple products." ;
    rdfs:domain ex:Product ;
    rdfs:range ex:Tag .
```

#### 3.1.3 Business rules (.rules)

A plain text file injected into the agent's system prompt. The agent reads it to make decisions. Use `=== SECTION ===` headers to organize rules into groups. No schema, no parsing — it is consumed only by the LLM.

Example:

```text
=== PRICING ===
Base consultation fee is $50.
Customers with 3+ years of history receive a 10% loyalty discount.
Apply automatically — do not ask the customer.

=== CANCELLATION POLICY ===
Full refund within 30 days of service.
After 30 days, a $25 late cancellation fee applies.
Waive the fee once per customer per year at your discretion if they ask politely.

=== CONSTRAINTS ===
Never waive fees for the same customer twice in the same calendar year.
Always confirm the action you took — do not silently modify records.
```

The agent uses these rules as instructions. Write them as you would brief a new employee — clear, direct, no ambiguity.

#### 3.1.4 Agent persona (.txt)

A plain text file that defines who the agent is and how it behaves. Injected as the first section of the system prompt.

Example:

```text
You are a friendly customer service representative for Acme Corp.
You have full access to the customer database.

=== WHAT YOU CAN DO ===
- Look up orders, returns, and account details
- Process returns within the 30-day window
- Apply loyalty discounts per the business rules

=== HOW TO BEHAVE ===
- Be concise and helpful
- Don't ask for information you can look up
- Never show SQL, table names, or internal details
- When you take an action, confirm what you did
```

### 3.2 Identity scoping

Identity scoping restricts the agent to only see and modify data belonging to the authenticated user. It is optional — omit `identity_entity` from `domain.json` to disable it.

**How it works:**

1. You declare `"identity_entity": "Customer"` in `domain.json`. This tells the framework which ontology class represents the user.
2. At startup, the framework builds an `OwnershipMap` by scanning the schema for tables that have a direct FK to the user entity's table.
3. The application boundary (CLI login prompt, Streamlit user picker, web session) establishes who the user is: `IdentityContext(user_id=42)`.
4. After every SIF-to-SQL translation, the pipeline injects `AND customer_id = 42` into queries, adds the FK to inserts, and scopes updates/deletes.

The LLM is completely unaware of this. It cannot bypass, omit, or modify the scoping — it is injected deterministically after translation.

**Tables that have no FK to the user entity are unscoped** (e.g. lookup tables, reference data). They pass through without filtering.

**In a multi-user deployment**, the identity comes from the request context (JWT, session cookie, API gateway header), not from the pipeline instance. The architecture supports this — `IdentityContext` is just a value passed per call.

### 3.3 Data sources

Each domain can have multiple data sources — for example, a primary database and a read replica, or a production database and a legacy migration source. Each data source has its own directory under `data_sources/` containing its mapping, overrides, and generated artifacts.

Data sources are defined in two places:

**In `domain.json`** — the logical definition:

```json
"data_sources": {
  "primary": {
    "store": "my_database_name",
    "source_dir": "data_sources/primary",
    "mapping": "mapping.yaml"
  }
}
```

- The key (`primary`) is the data source name.
- `store` is a pointer to a `[data_source:NAME]` section in `config.ini`.
- `source_dir` is the directory holding all artifacts for this data source (overrides, mapping, generated files).
- `mapping` points to the mapping file within `source_dir`.

**In `config.ini`** — the connection credentials:

```ini
[data_source:my_database_name]
dbname = my_database_name
user = postgres
password = postgres
host = localhost
port = 5432
```

**On disk** — the data source directory:

```
domains/my_domain/
    data_sources/primary/
        overrides.yaml          # Optional planner hints
        mapping.yaml            # Ontology-to-physical mapping
        _generated/             # All generated artifacts for this data source
```

Each mapping file starts with a back-pointer to its data source:

```yaml
data_source: primary
tables:
  Customer:
    table: customers
    primary_key: customer_id
    columns:
      first_name:
        column: first_name
      ...
```

### 3.4 Schema overrides

When the deterministic planner cannot resolve an ambiguous decision from ontology semantics alone, you can provide hints in `overrides.yaml` inside the data source directory (e.g. `data_sources/primary/overrides.yaml`).

Currently supported:

**FK direction** — By default, the FK lives on the domain (subject) side. For composition-style relationships where the child should carry the FK:

```yaml
fk_parent:
  hasPayment: domain    # FK lives on the range (Payment), pointing at the domain (Loan)
```

This tells the planner: "Payment is a child of Loan. Put the FK `loan_id` on the payments table, not the other way around."

### 3.5 Mapping to an existing database

If you have an existing database and want to connect it to an OntoCortex agent, you can hand-write a mapping file instead of running the architect pipeline. The mapping bridges your ontology vocabulary to whatever table and column names already exist.

```yaml
data_source: primary
tables:
  Owner:
    iri: https://example.org/ontology#Owner
    table: existing_customers        # Your actual table name
    primary_key: cust_id             # Your actual PK column
    columns:
      first_name:                    # Ontology property name
        iri: https://example.org/ontology#firstName
        column: fname                # Your actual column name
      last_name:
        iri: https://example.org/ontology#lastName
        column: lname

relations:
  hasOrder:
    type: direct
    fk_table: orders
    fk_column: cust_id
    ref_table: existing_customers
    ref_column: cust_id
```

The agent and SIF always use ontology names (`first_name`, `Owner`). The translator resolves them to physical names (`fname`, `existing_customers`) through the mapping. When `column` matches the ontology property name, you still declare it — explicit is better than implicit.

### 3.6 Domain actions

For business logic that cannot be expressed as data operations — premium calculations, appointment scheduling, PDF generation — you can register domain actions.

Create an `actions.py` in your domain directory:

```python
from agentcore.actions import register_action

def calculate_premium(params: dict, db_config, schema_map) -> str:
    coverage = params.get("coverage_amount", 0)
    rate = 0.02
    premium = coverage * rate
    return f"Calculated premium: ${premium:.2f} for ${coverage:,.0f} coverage"

register_action(
    name="calculate_premium",
    fn=calculate_premium,
    description="Calculate insurance premium based on coverage amount",
    params_schema={
        "coverage_amount": {"type": "number", "description": "Coverage amount in dollars"},
    },
)
```

The agent can invoke this action via SIF when a user asks for a premium calculation. Actions are loaded at pipeline startup and appear in the agent's tool schema automatically.

### 3.7 File layout reference

```
domains/<name>/
    domain.json                    # Domain descriptor (you write this)
    <name>_ontology.ttl            # OWL ontology (you write this)
    <name>.rules                   # Business rules (you write this)
    <name>_prompt.txt              # Agent persona (you write this)
    actions.py                     # Optional domain actions (you write this)
    data_sources/
        primary/                   # Per-data-source directory
            overrides.yaml         # Optional planner hints (you write this)
            mapping.yaml           # Ontology-to-physical mapping (generated or hand-written)
            _generated/
                <name>_schema_plan.yaml
                <name>_schema.json
                <name>_schema.sql
                <name>_seed_data.sql
                _builds/           # Per-module builder outputs

config.ini                         # Runtime configuration (gitignored)
config.ini.template                # Configuration reference
```

### 3.8 Workflow summary

```bash
# 1. Create your domain files
mkdir domains/my_domain
# ... write domain.json, ontology.ttl, rules.yaml, prompt.txt

# 2. Generate the schema
python scripts/design_plan.py my_domain
python scripts/build_schema.py my_domain --seed

# 3. Add database connection to config.ini
# [data_source:my_domain_db]
# dbname = my_domain_db
# user = postgres
# password = postgres
# host = localhost
# port = 5432

# 4. Run
python main.py my_domain
# or
streamlit run streamlit_app.py
```

### 3.9 Troubleshooting

**"No schema plan found"**
Run: `python scripts/design_plan.py <domain>`

**"No mapping file found"**
Run: `python scripts/build_schema.py <domain>`

**"No [data_source:NAME] section in config.ini"**
Add a `[data_source:...]` section with the `store` value from your `domain.json`.

**"Missing [models] section in config.ini"**
Your `config.ini` is missing required sections. Copy from `config.ini.template`.

**"Cannot connect to the database"**
Check that PostgreSQL is running: `docker-compose ps`

**"API key not found"**
Set `ANTHROPIC_API_KEY` environment variable, or fill in `api_key` in the `[anthropic]` section.

**Schema generation fails partway through**
Module builds are cached in `_generated/_builds/`. Re-run `build_schema.py` — only failed modules will be rebuilt. Use `--force` to rebuild everything from scratch. The schema builder is fully deterministic, so build failures indicate an ontology issue.
