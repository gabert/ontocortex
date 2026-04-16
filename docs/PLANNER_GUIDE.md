# Schema Planner — User Guide

The Planner turns an OWL/Turtle ontology into a **schema plan**: a topology-only
blueprint listing every entity table, every foreign-key relationship, every
junction table, and the module grouping that drives downstream parallel work.

It is **pure, deterministic Python** — no LLM call — which means it is fast,
reproducible, free to run, and auditable. The tradeoff is that a handful of
design decisions can't be made from ontology semantics alone. For those, the
Planner has a clearly scoped override file: `schema_overrides.yaml`. This is
where your human judgment lives.

Even with advanced AI in the loop, those judgment calls are where a reviewer
is expected to intervene. This guide tells you where to look, what to check,
and how to fix what needs fixing.

---

## 1. What the Planner does

```
ontology.ttl  ──►  agentcore/planner.py  ──►  _generated/<domain>_schema_plan.yaml
                        ▲
                        │ optional
              domains/<domain>/schema_overrides.yaml
```

**In:** `domains/<domain>/<domain>_ontology.ttl` plus an optional
`schema_overrides.yaml` in the same directory.

**Out:** `domains/<domain>/_generated/<domain>_schema_plan.yaml` — the plan
you review. This file is input to Phase 2 (Builder), which designs columns
per module, and Phase 3 (Reconciler), which merges the result and renders DDL.

**The Planner decides three things, in order:**

1. **Tables.** Every `owl:Class` that is not a value set becomes a table.
   Naming is mechanical: `{prefix}_{plural_snake_case}`, with a surrogate
   integer primary key `{prefix}_{singular}_id`.
2. **Relationships.** Every `owl:ObjectProperty` becomes either a foreign
   key (1:N) or a junction table (M:N). Direction is pinned in the plan.
3. **Modules.** Tables are grouped into modules — one per namespace prefix
   by default. Modules are the unit of parallelism for the Builder phase.

**The Planner does NOT decide:**

- Data columns (datatype properties). That's the Builder's job.
- Data types, constraints, defaults, indexes.
- Lookup tables for value sets. Those are injected deterministically later.
- Creation order — the Reconciler topologically sorts FKs.

---

## 2. Quick start

```bash
# Design a plan for a domain
python scripts/design_plan.py student_loans

# If the domain has only one entry in domains/, you can omit the name:
python scripts/design_plan.py
```

The script prints a short summary:

```
Modules      : 1
Tables       : 7
Relationships: 6  (5 FKs, 1 junctions)

module sl               7 tables
```

Then open `domains/<domain>/_generated/<domain>_schema_plan.yaml` and
review it. The guide below explains what to look for.

---

## 3. Reading the plan

A generated plan has three top-level sections:

```yaml
modules:            # how tables are grouped (parallelism unit)
  - name: sl
    tables: [sl_cosigners, sl_lenders, sl_loans, ...]

tables:             # every entity table
  - name: sl_loans
    ontology_iri: https://studentloans.example.org/ontology#Loan
    primary_key: sl_loan_id
    comment: A specific loan agreement...

relationships:      # FKs and junctions
  - iri: https://studentloans.example.org/ontology#hasBorrower
    kind: fk
    child_table: sl_loans
    parent_table: sl_students
    fk_column: sl_student_id

  - iri: https://studentloans.example.org/ontology#hasCosigner
    kind: junction
    junction_table: sl_loan_cosigners
    endpoint_a: sl_loans
    endpoint_b: sl_cosigners
```

**FK direction** is read as: `child_table` carries the `fk_column`,
which points at `parent_table`'s primary key. The parent can exist
without the child; the child cannot exist without the parent.

---

## 4. Where the Planner needs your judgment

Three categories of decisions have **no clean signal in OWL alone**. Review
each pass of the plan for these, and use the override file to fix anything
that looks wrong.

### 4.1 FK direction for ambiguous `has*` properties

The default is: **FK lives on the domain (subject) side**. This is correct
for *reference-style* properties where the range exists independently:

- `Loan hasBorrower Student` → FK `sl_student_id` on `sl_loans` ✓
- `Loan hasLoanType LoanType` → FK `sl_loan_type_id` on `sl_loans` ✓
- `Loan forSchool School` → FK `sl_school_id` on `sl_loans` ✓

It is **wrong** for *composition-style* properties where the range is a
dependent child record:

- `Loan hasPayment Payment` → FK should be `sl_loan_id` on `sl_payments`,
  not the other way around. The Planner won't figure this out from the name.

**How to tell which is which:** ask yourself which entity would be orphaned
if the other were deleted. A payment without a loan is nonsense → payment
is the child, FK goes on the payments table. A loan without a loan type is
also nonsense, but the loan type *exists independently* as a catalog entry
→ loan type is the parent, FK goes on the loans table.

**Fix:** add an entry to `fk_parent` in `schema_overrides.yaml`:

```yaml
fk_parent:
  sl:hasPayment: domain   # FK lives on the range side (sl_payments)
```

The value `domain` means "FK lives on the range side pointing back to the
domain". The value `range` is the default.

### 4.2 Junction vs. foreign key

The Planner marks a property as a junction table if its `rdfs:comment`
says so (`"many-to-many"`, `"junction table"`, `"m:n"`, etc.). Otherwise
it defaults to an FK. If your ontology has an M:N relationship without
the magic words, the Planner will get it wrong.

**Fix:** force the junction explicitly.

```yaml
junction_properties:
  - hasCosigner
  - treatedBy
```

Or by (domain, range) IRI pairs, useful for multi-namespace ontologies:

```yaml
junction_relations:
  - ["https://example.org/ontology#Loan", "https://example.org/ontology#Cosigner"]
```

### 4.3 Module grouping

The default is one module per namespace prefix. That works for small
single-namespace domains (student_loans, insurance, vet) but will
produce a single oversized module for a 300-table domain that happens
to share one prefix.

Modules are the **grouping unit** for Phase 2. The deterministic
builder processes them sequentially (instant). Target module size:
**5–20 tables**.

**Fix:** force groupings explicitly. Each entry can match by physical
table name, class local_name, qname, or full IRI.

```yaml
modules:
  borrowers:
    - Student
    - Cosigner
  loans:
    - Loan
    - LoanType
    - Lender
    - School
  billing:
    - Payment
```

Any class not listed falls back to the namespace-prefix default, so you
can override only the parts you care about.

### 4.4 Ignoring classes

Legacy, draft, or abstract classes that shouldn't become tables:

```yaml
ignore_classes:
  - LegacyThing
  - DraftConcept
  - "https://example.org/ontology#AbstractBase"
```

Any IRI / qname / local_name listed here is dropped from tables and
any relationship pointing at it is skipped.

---

## 5. `schema_overrides.yaml` reference

Full example showing every section. Every section is optional — include
only what you need.

```yaml
# ─── FK direction ───────────────────────────────────────────────
# Default: 'range' (FK on the domain/subject side, pointing at the range).
# Use 'domain' when the range is a dependent child that should carry the FK.
#
# Keys may be bare local_name, qname (prefix:LocalName), or full IRI.
# Resolved in that order; most-specific wins.
fk_parent:
  sl:hasPayment: domain
  hasInvoice: domain          # bare name, fine for single-namespace domains
  reportsTo: range            # explicit, matches default

# ─── Junction tables ────────────────────────────────────────────
junction_properties:          # by property name/qname/IRI
  - hasCosigner
  - treatedBy

junction_relations:           # by (domain IRI, range IRI) pairs
  - ["https://example.org/ontology#Loan", "https://example.org/ontology#Cosigner"]

# ─── Ignored classes ────────────────────────────────────────────
ignore_classes:
  - LegacyThing
  - DraftConcept

# ─── Module groupings ───────────────────────────────────────────
# Keys are module names; values are lists of class local_names (or
# table names, qnames, or IRIs). Unlisted classes fall back to the
# namespace-prefix default.
modules:
  borrowers: [Student, Cosigner]
  loans:     [Loan, LoanType, Lender, School]
  billing:   [Payment]
```

---

## 6. Review workflow

A practical checklist when reviewing a freshly generated plan:

**Step 1 — table count sanity check.**
The CLI prints `Tables: N`. Does N match the number of non-value-set
classes in your ontology? If it's lower, something is being ignored
(check `ignore_classes`) or the ontology has domain/range holes.

**Step 2 — walk the relationships section.**
For each `kind: fk`, ask the orphan test from §4.1: *"if I delete the
parent, does the child still make sense?"* If yes, the direction is
wrong — add an entry to `fk_parent` and re-run.

**Step 3 — check every junction.**
Junctions should only exist where both sides are independent and the
relationship is truly M:N. Look for false positives (an `rdfs:comment`
that happens to contain the word "many") and false negatives (a real
M:N where the comment didn't use the keywords).

**Step 4 — check module sizes.**
Any module above ~25 tables should be split. Any module below ~3 tables
should probably be merged or the grouping rethought. Use the `modules`
override to draw better boundaries.

**Step 5 — re-run until the plan is clean.**
`python scripts/design_plan.py <domain>` — the Planner is fast enough
to re-run on every override tweak. Iterate until the plan survives
your review without edits.

**Step 6 — commit the override file.**
`schema_overrides.yaml` is your judgment captured as code. Commit it
alongside the ontology. Someone (possibly future-you) will need to
understand why `hasPayment` is `domain` two years from now.

---

## 7. Common pitfalls

- **"Why is my Payment table missing?"** Check that `Payment` has an
  `rdfs:domain`/`rdfs:range` declared on every property that touches it.
  The Planner silently skips object properties where either endpoint is
  an ignored or missing class.

- **"The plan says junction but I meant FK."** Check the `rdfs:comment`
  on the property. The Planner treats `"many-to-many"`, `"junction"`,
  `"m:n"`, and a few variants as junction signals. Rewrite the comment
  or add an `fk_parent` override.

- **"PyYAML not installed."** `pip install pyyaml`. The planner requires
  PyYAML unconditionally now that plans are YAML.

- **"My override key isn't being picked up."** Override keys for properties
  must match the `local_name` (e.g. `hasPayment`), the qname (`sl:hasPayment`),
  or the full IRI. No leading underscore, no trailing colon. When in doubt,
  use the full IRI — it's the escape hatch that can't be ambiguous.

- **"Validation failed: Plan missing relationships for..."** The Planner
  emits a relationship for every object property whose domain *and* range
  resolve to a table in the plan. If you ignored a class that's used as a
  domain or range, relationships pointing at it are silently dropped —
  which shows up as a validation error if they're referenced elsewhere.
  Either un-ignore the class or add the related property to the ignore set.

---

## 8. What comes after the plan

The plan is the input to two downstream phases (not yet implemented as of
this writing — see `NOTES_scaled_architect.md`):

- **Phase 2 — Builder.** Deterministic. Maps each ontology datatype
  property to a column definition using XSD type → logical type mapping
  and convention-based flags. No LLM call.
- **Phase 3 — Reconciler.** Pure Python. Merges the Builder outputs,
  injects lookup tables from value sets, topologically sorts table
  creation order, validates global referential integrity, renders DDL.

Your override file is read by all three phases, so a fix made here once
propagates downstream on every rebuild.

---

## 9. Why this is a human-supervised step

The Planner is deterministic, which means every call with the same
ontology + overrides produces byte-identical output. That's a feature:
it makes the plan *reviewable*. You can diff two plan runs and see
exactly what changed.

But the determinism also means the Planner cannot guess. Composition vs.
reference, junction vs. FK, module boundaries — these are **modeling
decisions**, not lookups. Someone who understands the business has to
make them once, encode them in `schema_overrides.yaml`, and commit that
file. From then on, the Planner will apply them consistently every time
the ontology is rebuilt.

In practice, a large domain needs ~5–15 override entries — roughly one
for every judgment call in the ontology. That's small enough to review
by hand on every schema rebuild, and that's precisely the point.
