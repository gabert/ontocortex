package com.ontocore.engine.ontology;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ontocore.engine.architect.NamingConventions;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFList;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.RDFS;
import org.apache.jena.vocabulary.XSD;

/**
 * Parses an OWL/Turtle ontology into an {@link OntologyModel}.
 *
 * <p>Uses Apache Jena to replace the Python rdflib-based parser. The output
 * structure is identical to Python's {@code build_ontology_model()} so that
 * downstream consumers (planner, schema map, validation) produce the same
 * results.</p>
 */
public final class OntologyParser {

    /** Infrastructure prefixes — anything NOT in this set is a domain namespace. */
    private static final List<String> INFRA_PREFIXES = List.of(
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "http://www.w3.org/2000/01/rdf-schema#",
            "http://www.w3.org/2002/07/owl#",
            "http://www.w3.org/2001/XMLSchema#",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns",
            "http://www.w3.org/2000/01/rdf-schema",
            "http://www.w3.org/2002/07/owl",
            "http://www.w3.org/2001/XMLSchema"
    );

    private OntologyParser() {}

    /**
     * Parse a Turtle ontology string into a structured {@link OntologyModel}.
     *
     * @param turtleText the raw .ttl content
     * @param sourcePath optional path for error messages (may be null)
     * @return the parsed ontology model
     * @throws OntologyParseError if parsing fails
     */
    public static OntologyModel parse(String turtleText, String sourcePath) {
        Model model = ModelFactory.createDefaultModel();
        try {
            model.read(new StringReader(turtleText), null, "TURTLE");
        } catch (Exception e) {
            String where = sourcePath != null ? " (" + sourcePath + ")" : "";
            throw new OntologyParseError("Failed to parse ontology" + where + ": " + e.getMessage(), e);
        }

        // Build namespace map: domain prefixes only
        Map<String, String> candidateNs = new LinkedHashMap<>();
        model.getNsPrefixMap().forEach((prefix, uri) -> {
            if (!prefix.isEmpty() && !isInfraUri(uri)) {
                candidateNs.put(uri, prefix);
            }
        });

        // Determine which namespaces are actually used
        Set<String> usedNs = new HashSet<>();
        StmtIterator stmts = model.listStatements();
        while (stmts.hasNext()) {
            Statement stmt = stmts.next();
            checkNodeNamespace(stmt.getSubject(), candidateNs, usedNs);
            if (stmt.getObject().isResource()) {
                checkNodeNamespace(stmt.getObject().asResource(), candidateNs, usedNs);
            }
        }

        Map<String, String> nsMap = new LinkedHashMap<>();
        candidateNs.forEach((uri, prefix) -> {
            if (usedNs.contains(uri)) {
                nsMap.put(uri, prefix);
            }
        });

        // Collect classes and value sets
        List<OntologyClass> classes = new ArrayList<>();
        List<ValueSet> valueSets = new ArrayList<>();
        List<Resource> owlClasses = new ArrayList<>();
        model.listResourcesWithProperty(RDF.type, OWL.Class).forEachRemaining(owlClasses::add);
        owlClasses.sort(Comparator.comparing(Resource::getURI, Comparator.nullsLast(Comparator.naturalOrder())));

        for (Resource cls : owlClasses) {
            if (cls.isAnon() || !isDomainUri(cls.getURI())) continue;

            String comment = getComment(model, cls);

            if (isValueSet(model, cls)) {
                List<String> members = collectOneOfMembers(model, cls);
                valueSets.add(new ValueSet(
                        cls.getURI(),
                        qname(cls.getURI(), nsMap),
                        NamingConventions.localName(cls.getURI()),
                        comment,
                        members
                ));
            } else {
                classes.add(new OntologyClass(
                        cls.getURI(),
                        qname(cls.getURI(), nsMap),
                        NamingConventions.localName(cls.getURI()),
                        comment
                ));
            }
        }

        // Collect object properties
        List<ObjectProperty> objectProperties = new ArrayList<>();
        List<Resource> owlObjProps = new ArrayList<>();
        model.listResourcesWithProperty(RDF.type, OWL.ObjectProperty).forEachRemaining(owlObjProps::add);
        owlObjProps.sort(Comparator.comparing(Resource::getURI, Comparator.nullsLast(Comparator.naturalOrder())));

        for (Resource prop : owlObjProps) {
            if (prop.isAnon() || !isDomainUri(prop.getURI())) continue;

            RDFNode domainNode = getPropertyValue(model, prop, RDFS.domain);
            RDFNode rangeNode = getPropertyValue(model, prop, RDFS.range);

            List<String> domainIris = resolveClassList(model, domainNode);
            List<String> rangeIris = resolveClassList(model, rangeNode);

            objectProperties.add(new ObjectProperty(
                    prop.getURI(),
                    qname(prop.getURI(), nsMap),
                    NamingConventions.localName(prop.getURI()),
                    domainIris.size() == 1 ? domainIris.getFirst() : null,
                    rangeIris.size() == 1 ? rangeIris.getFirst() : null,
                    domainIris,
                    rangeIris,
                    getComment(model, prop)
            ));
        }

        // Collect datatype properties
        List<DatatypeProperty> datatypeProperties = new ArrayList<>();
        List<Resource> owlDtProps = new ArrayList<>();
        model.listResourcesWithProperty(RDF.type, OWL.DatatypeProperty).forEachRemaining(owlDtProps::add);
        owlDtProps.sort(Comparator.comparing(Resource::getURI, Comparator.nullsLast(Comparator.naturalOrder())));

        for (Resource prop : owlDtProps) {
            if (prop.isAnon() || !isDomainUri(prop.getURI())) continue;

            RDFNode domainNode = getPropertyValue(model, prop, RDFS.domain);
            List<String> domainIris = resolveClassList(model, domainNode);
            RDFNode rangeNode = getPropertyValue(model, prop, RDFS.range);
            String rangeIri = rangeNode != null && rangeNode.isURIResource() ? rangeNode.asResource().getURI() : null;

            String localName = NamingConventions.localName(prop.getURI());
            datatypeProperties.add(new DatatypeProperty(
                    prop.getURI(),
                    qname(prop.getURI(), nsMap),
                    localName,
                    NamingConventions.toSnakeCase(localName),
                    domainIris,
                    rangeIri,
                    getComment(model, prop)
            ));
        }

        // Build the namespace map in Python's format: {prefix: uri}
        Map<String, String> namespacesOut = new LinkedHashMap<>();
        nsMap.forEach((uri, prefix) -> namespacesOut.put(prefix, uri));

        return new OntologyModel(namespacesOut, classes, objectProperties, datatypeProperties, valueSets);
    }

    public static OntologyModel parse(String turtleText) {
        return parse(turtleText, null);
    }

    // ── Private helpers ────────────────────────────────────────────────

    private static boolean isInfraUri(String uri) {
        for (String prefix : INFRA_PREFIXES) {
            if (uri.startsWith(prefix)) return true;
        }
        return false;
    }

    private static boolean isDomainUri(String uri) {
        return uri != null && !isInfraUri(uri);
    }

    private static void checkNodeNamespace(Resource resource, Map<String, String> candidateNs, Set<String> usedNs) {
        if (resource.isAnon()) return;
        String uri = resource.getURI();
        if (uri == null || !isDomainUri(uri)) return;
        for (String nsUri : candidateNs.keySet()) {
            if (uri.startsWith(nsUri)) {
                usedNs.add(nsUri);
                break;
            }
        }
    }

    private static String qname(String uri, Map<String, String> nsMap) {
        for (Map.Entry<String, String> entry : nsMap.entrySet()) {
            if (uri.startsWith(entry.getKey())) {
                return entry.getValue() + ":" + uri.substring(entry.getKey().length());
            }
        }
        return NamingConventions.localName(uri);
    }

    private static String getComment(Model model, Resource resource) {
        Statement stmt = resource.getProperty(RDFS.comment);
        return stmt != null ? stmt.getString() : "";
    }

    private static RDFNode getPropertyValue(Model model, Resource subject, org.apache.jena.rdf.model.Property predicate) {
        Statement stmt = subject.getProperty(predicate);
        return stmt != null ? stmt.getObject() : null;
    }

    private static boolean isValueSet(Model model, Resource cls) {
        return cls.hasProperty(OWL.oneOf);
    }

    private static List<String> collectOneOfMembers(Model model, Resource cls) {
        Statement stmt = cls.getProperty(OWL.oneOf);
        if (stmt == null) return List.of();

        RDFNode listNode = stmt.getObject();
        if (!listNode.isResource()) return List.of();

        List<String> members = new ArrayList<>();
        RDFList rdfList = listNode.as(RDFList.class);
        rdfList.iterator().forEachRemaining(node -> {
            if (node.isURIResource()) {
                members.add(node.asResource().getURI());
            }
        });
        return members;
    }

    /**
     * Resolve a domain/range node to a list of class IRIs.
     * Handles both simple URI resources and owl:unionOf blank nodes.
     */
    private static List<String> resolveClassList(Model model, RDFNode node) {
        if (node == null) return List.of();

        if (node.isURIResource()) {
            return List.of(node.asResource().getURI());
        }

        if (node.isAnon()) {
            Resource bnode = node.asResource();
            Statement unionStmt = bnode.getProperty(OWL.unionOf);
            if (unionStmt != null && unionStmt.getObject().isResource()) {
                List<String> result = new ArrayList<>();
                RDFList rdfList = unionStmt.getObject().as(RDFList.class);
                rdfList.iterator().forEachRemaining(item -> {
                    if (item.isURIResource()) {
                        result.add(item.asResource().getURI());
                    }
                });
                return result;
            }
        }

        return List.of();
    }
}
