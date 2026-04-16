package com.ontocore.sif;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SifParserTest {

    @Test
    void parseQuery() {
        var raw = Map.<String, Object>of(
                "op", "query",
                "entity", "Borrower",
                "fields", List.of("first_name", "email"),
                "filters", Map.<String, Object>of("status", "active")
        );

        SifOperation op = SifParser.parseOne(raw);
        assertThat(op).isInstanceOf(Query.class);

        Query q = (Query) op;
        assertThat(q.op()).isEqualTo("query");
        assertThat(q.entity()).isEqualTo("Borrower");
        assertThat(q.fields()).containsExactly("first_name", "email");
        assertThat(q.filters()).containsEntry("status", "active");
        assertThat(q.relations()).isNull();
        assertThat(q.limit()).isNull();
    }

    @Test
    void parseQueryWithRelations() {
        var raw = Map.<String, Object>of(
                "op", "query",
                "entity", "Loan",
                "relations", List.of(
                        Map.<String, Object>of("rel", "hasBorrower", "entity", "Borrower")
                )
        );

        Query q = (Query) SifParser.parseOne(raw);
        assertThat(q.relations()).hasSize(1);
        assertThat(q.relations().getFirst()).containsEntry("rel", "hasBorrower");
    }

    @Test
    void parseQueryWithAggregateAndSort() {
        var raw = Map.<String, Object>of(
                "op", "query",
                "entity", "Loan",
                "aggregate", Map.<String, Object>of("fn", "sum", "field", "amount"),
                "sort", Map.<String, Object>of("field", "amount", "dir", "desc"),
                "limit", 10
        );

        Query q = (Query) SifParser.parseOne(raw);
        assertThat(q.aggregate()).containsEntry("fn", "sum");
        assertThat(q.sort()).containsEntry("dir", "desc");
        assertThat(q.limit()).isEqualTo(10);
    }

    @Test
    void parseCreate() {
        var raw = Map.<String, Object>of(
                "op", "create",
                "entity", "Borrower",
                "data", Map.<String, Object>of("first_name", "Alice", "email", "alice@example.com")
        );

        SifOperation op = SifParser.parseOne(raw);
        assertThat(op).isInstanceOf(Create.class);

        Create c = (Create) op;
        assertThat(c.entity()).isEqualTo("Borrower");
        assertThat(c.data()).containsEntry("first_name", "Alice");
        assertThat(c.resolve()).isNull();
    }

    @Test
    void parseCreateWithResolve() {
        var raw = Map.<String, Object>of(
                "op", "create",
                "entity", "Loan",
                "data", Map.<String, Object>of("amount", 5000),
                "resolve", Map.<String, Object>of("hasBorrower",
                        Map.of("entity", "Borrower", "filters", Map.of("email", "alice@example.com")))
        );

        Create c = (Create) SifParser.parseOne(raw);
        assertThat(c.resolve()).containsKey("hasBorrower");
    }

    @Test
    void parseUpdate() {
        var raw = Map.<String, Object>of(
                "op", "update",
                "entity", "Borrower",
                "data", Map.<String, Object>of("email", "new@example.com"),
                "filters", Map.<String, Object>of("borrower_id", 1)
        );

        SifOperation op = SifParser.parseOne(raw);
        assertThat(op).isInstanceOf(Update.class);

        Update u = (Update) op;
        assertThat(u.entity()).isEqualTo("Borrower");
        assertThat(u.data()).containsEntry("email", "new@example.com");
        assertThat(u.filters()).containsEntry("borrower_id", 1);
    }

    @Test
    void parseDelete() {
        var raw = Map.<String, Object>of(
                "op", "delete",
                "entity", "Borrower",
                "filters", Map.<String, Object>of("borrower_id", 1)
        );

        SifOperation op = SifParser.parseOne(raw);
        assertThat(op).isInstanceOf(Delete.class);

        Delete d = (Delete) op;
        assertThat(d.filters()).containsEntry("borrower_id", 1);
    }

    @Test
    void parseAction() {
        var raw = Map.<String, Object>of(
                "op", "action",
                "action", "calculate_premium",
                "params", Map.<String, Object>of("policy_id", 42)
        );

        SifOperation op = SifParser.parseOne(raw);
        assertThat(op).isInstanceOf(Action.class);

        Action a = (Action) op;
        assertThat(a.action()).isEqualTo("calculate_premium");
        assertThat(a.params()).containsEntry("policy_id", 42);
    }

    @Test
    void parseLink() {
        var raw = Map.<String, Object>of(
                "op", "link",
                "relation", "ownsPet",
                "from", Map.<String, Object>of("entity", "Owner", "filters", Map.of("owner_id", 1)),
                "to", Map.<String, Object>of("entity", "Pet", "filters", Map.of("pet_id", 5))
        );

        SifOperation op = SifParser.parseOne(raw);
        assertThat(op).isInstanceOf(Link.class);

        Link l = (Link) op;
        assertThat(l.relation()).isEqualTo("ownsPet");
        assertThat(l.from()).containsKey("entity");
        assertThat(l.to()).containsKey("entity");
    }

    @Test
    void parseUnlink() {
        var raw = Map.<String, Object>of(
                "op", "unlink",
                "relation", "ownsPet",
                "from", Map.<String, Object>of("entity", "Owner", "filters", Map.of("owner_id", 1)),
                "to", Map.<String, Object>of("entity", "Pet", "filters", Map.of("pet_id", 5))
        );

        SifOperation op = SifParser.parseOne(raw);
        assertThat(op).isInstanceOf(Unlink.class);
    }

    @Test
    void parseAllReturnsList() {
        var ops = List.of(
                Map.<String, Object>of("op", "query", "entity", "Borrower"),
                Map.<String, Object>of("op", "create", "entity", "Loan", "data", Map.of("amount", 1000))
        );

        List<SifOperation> result = SifParser.parseAll(ops);
        assertThat(result).hasSize(2);
        assertThat(result.get(0)).isInstanceOf(Query.class);
        assertThat(result.get(1)).isInstanceOf(Create.class);
    }

    @Test
    void missingOpThrows() {
        var raw = Map.<String, Object>of("entity", "Borrower");

        assertThatThrownBy(() -> SifParser.parseOne(raw))
                .isInstanceOf(SifParseError.class)
                .hasMessageContaining("Missing 'op'");
    }

    @Test
    void unknownOpThrows() {
        var raw = Map.<String, Object>of("op", "drop_table");

        assertThatThrownBy(() -> SifParser.parseOne(raw))
                .isInstanceOf(SifParseError.class)
                .hasMessageContaining("Unknown op 'drop_table'");
    }

    @Test
    void exhaustiveSwitchCoversAllTypes() {
        var ops = List.of(
                Map.<String, Object>of("op", "query", "entity", "X"),
                Map.<String, Object>of("op", "create", "entity", "X"),
                Map.<String, Object>of("op", "update", "entity", "X"),
                Map.<String, Object>of("op", "delete", "entity", "X"),
                Map.<String, Object>of("op", "action", "action", "test"),
                Map.<String, Object>of("op", "link", "relation", "r"),
                Map.<String, Object>of("op", "unlink", "relation", "r")
        );

        List<SifOperation> parsed = SifParser.parseAll(ops);

        for (SifOperation op : parsed) {
            String label = switch (op) {
                case Query q -> q.op();
                case Create c -> c.op();
                case Update u -> u.op();
                case Delete d -> d.op();
                case Action a -> a.op();
                case Link l -> l.op();
                case Unlink ul -> ul.op();
            };
            assertThat(label).isNotBlank();
        }
    }
}
