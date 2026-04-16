# OntoCortex

> **Experimental research project** — exploring the boundaries of what LLMs can do as a business logic layer in ontology-driven architectures.

**An ontology-driven, domain-agnostic framework with two independent layers:**

1. **SIF Engine** — a deterministic ontology-to-SQL pipeline. Takes an OWL ontology, maps it to a physical database, and executes structured data operations (query, create, update, delete, link) with validation, column-level mapping, and identity scoping. No LLM required at runtime.

2. **LLM Agent** — a conversational layer on top of the SIF Engine. The agent reads the ontology and business rules, understands natural language, and emits SIF operations. It is one consumer of the engine — the same SIF pipeline can be driven by a REST API, a rule engine, or any other frontend.

A pure OWL/Turtle ontology is the single source of truth for each domain. Three domains ship as proof-of-concept: **student loans**, **vet clinic**, and **dragon breeding**.

---

## How it works

```
                        ┌──────────────────────────────────────┐
  ontology.ttl ──►      │  Scaled Architect Pipeline           │
                        │  1. Planner (deterministic)          │
                        │  2. Builder (deterministic)          │
                        │  3. Reconciler (deterministic)       │
                        └──────────┬───────────────────────────┘
                                   │
                    schema.json + mapping.yaml + seed_data.sql
                                   │
                         SQLAlchemy ──► PostgreSQL
```

**SIF Engine (deterministic, LLM-free at runtime):**

```
  SIF operation ──► Validator ──► Translator ──► Identity Scoping ──► SQL ──► PostgreSQL
  (ontology names)                (mapping.yaml)  (post-translation)
```

**LLM Agent (one consumer of the SIF Engine):**

```
  User question ──► Agent (Claude)
                        ├── reads ontology + business rules (system prompt)
                        ├── emits SIF operations (ontology vocabulary)
                        ├── SIF Engine executes them
                        └── returns reasoned answer
```

The entire schema design pipeline is deterministic — no LLM is needed. The Planner infers table topology from OWL semantics, the Builder maps XSD types to column definitions, and the Reconciler merges everything into a single schema. At runtime, the agent speaks ontology names and the SIF Engine handles everything below.

---

## Prerequisites

- Python 3.10+
- Docker (for PostgreSQL) — or a local PostgreSQL 15+ instance
- An Anthropic API key — [console.anthropic.com](https://console.anthropic.com/) *(only needed for the LLM Agent and optional seed data generation; schema generation is fully deterministic)*

---

## Quickstart

```bash
git clone https://github.com/gabert/ontocortex.git
cd ontocortex
pip install -r requirements.txt
```

**Start PostgreSQL**
```bash
docker-compose up -d
```

**Configure**
```bash
cp config.ini.template config.ini
```
Edit `config.ini` — set your API key (or use the `ANTHROPIC_API_KEY` environment variable), configure models, and add a `[data_source:...]` section for each domain database. See the [User Guide](docs/USER_GUIDE.md) for details.

**Generate schema from ontology** *(one-time per domain, no LLM needed)*
```bash
python scripts/design_plan.py student_loans
python scripts/build_schema.py student_loans
```

**Optionally generate demo seed data** *(requires API key)*
```bash
python scripts/build_schema.py student_loans --seed
```

**Run the web UI**
```bash
streamlit run streamlit_app.py
```

Opens at `http://localhost:8501` with a chat interface and an under-the-hood panel showing SIF operations, SQL queries, and results.

**Or run the CLI agent**
```bash
python main.py student_loans
```

---

## Documentation

- **[User Guide](docs/USER_GUIDE.md)** — setup, configuration, creating domains, customization
- **[config.ini.template](config.ini.template)** — annotated configuration reference

---

## Adding a new domain

1. Create `domains/<name>/` with:
   - `domain.json` — descriptor (name, data sources, file pointers)
   - `<name>_ontology.ttl` — OWL/Turtle ontology (pure OWL, no custom annotations)
   - `<name>.rules` — business rules (plain text with `=== SECTION ===` headers)
   - `<name>_prompt.txt` — agent persona / system prompt fragment

2. Generate the schema:
   ```bash
   python scripts/design_plan.py <name>
   python scripts/build_schema.py <name>
   # Optional: generate demo seed data (requires API key)
   python scripts/build_schema.py <name> --seed
   ```

3. Add a `[data_source:<dbname>]` section to `config.ini`

4. Run:
   ```bash
   python main.py <name>
   ```

The framework detects the new domain automatically — no code changes needed. See the [User Guide](docs/USER_GUIDE.md) for the full walkthrough.

---

## Interactive CLI commands

| Command | Description |
|---|---|
| `/list` | Show available domains |
| `/load <domain>` | Switch to a domain; installs DB if needed |
| `/reset` | Drop and recreate the current domain's database |
| `/new` | Clear conversation history (keep domain) |
| `/quit` | Exit |

---

## Tech stack

| Component | Technology |
|---|---|
| LLM | Claude (Anthropic) |
| Ontology | OWL / Turtle (RDFLib) |
| Database | PostgreSQL 15 |
| ORM / DDL | SQLAlchemy |
| Web UI | Streamlit |
| Container | Docker |

---

## License

MIT
