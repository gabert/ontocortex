# OntoCortex

> **Experimental research project** — exploring the boundaries of what LLMs can do as a business logic layer in ontology-driven architectures.

**An ontology-driven, domain-agnostic LLM agent framework.**

A pure OWL/Turtle ontology is the single source of truth for each domain — it drives database schema generation, test data loading, and agent context injection. Claude acts as the business logic layer: it receives the schema and business rules in its system prompt, generates SQL, executes it against PostgreSQL, and returns reasoned answers. There is no traditional service layer.

Three domains ship as proof-of-concept: **insurance**, **vet clinic**, and **dragon breeding**.

> **Note:** Claude (Anthropic) is used as the LLM backend for prototyping. The framework will be migrated to LangChain to support multiple model backends.

---

## How it works

```
ontology.ttl ──► LLM architect ──► schema.json (logical)
                                        │
                                        ├──► SQLAlchemy ──► PostgreSQL tables
                                        ├──► schema description ──► agent prompt
                                        └──► seed_data.sql ──► PostgreSQL rows

User question ──► Agent (Claude)
                      ├── reads schema + ontology + business rules
                      ├── generates SQL
                      ├── executes against PostgreSQL
                      └── returns reasoned answer
```

The LLM architect infers all schema decisions (FK placement, constraints, defaults) from pure OWL semantics — no custom annotations needed.

---

## Prerequisites

- Python 3.10+
- Docker (for PostgreSQL) — or a local PostgreSQL 15+ instance
- An Anthropic API key — [console.anthropic.com](https://console.anthropic.com/)

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
Edit `config.ini` and set your database credentials. For the API key, either set it as an environment variable (preferred):
```bash
export ANTHROPIC_API_KEY=sk-ant-...        # Linux / macOS
$env:ANTHROPIC_API_KEY="sk-ant-..."        # Windows PowerShell
```
Or fill it in directly in `config.ini` — see the comments in the template.

**Generate schema from ontology** *(one-time per domain)*
```bash
python scripts/design_schema.py insurance
```

**Run the web UI**
```bash
streamlit run streamlit_app.py
```

Opens at `http://localhost:8501` with a chat interface and an under-the-hood panel showing SQL queries, tool calls, and reasoning.

**Or run the CLI agent**
```bash
python main.py insurance
```

---

## Adding a new domain

1. Create `domains/<name>/` with:
   - `domain.json` — descriptor (name, db name, file pointers)
   - `<name>_ontology.ttl` — OWL/Turtle ontology
   - `<name>_rules.yaml` — business rules
   - `<name>_prompt.txt` — agent persona / system prompt fragment

2. Generate the schema:
   ```bash
   python scripts/design_schema.py <name>
   ```

3. Run:
   ```bash
   python main.py <name>
   ```

The framework detects the new domain automatically — no code changes needed.

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
| LLM | Claude (Anthropic) — prototype; LangChain migration planned |
| Ontology | OWL / Turtle (RDFLib) |
| Database | PostgreSQL 15 |
| ORM / DDL | SQLAlchemy |
| Web UI | Streamlit |
| Container | Docker |

---

## License

MIT