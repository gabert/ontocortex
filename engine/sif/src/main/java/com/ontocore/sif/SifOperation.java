package com.ontocore.sif;

/**
 * A single SIF (Structured Intent Format) operation.
 *
 * <p>SIF is the contract between the LLM agent and the engine. The agent
 * expresses intent in ontology vocabulary; the engine validates, translates,
 * and executes it against the physical backend.</p>
 *
 * <p>Sealed to exactly 7 variants — exhaustive {@code switch} expressions
 * are enforced at compile time.</p>
 */
public sealed interface SifOperation
        permits Query, Create, Update, Delete, Action, Link, Unlink {

    /** The operation type string as it appears in JSON ({@code "query"}, {@code "create"}, etc.). */
    String op();
}
