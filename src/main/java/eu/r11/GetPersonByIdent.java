package eu.r11;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

/**
 * This is an example showing how you could expose Neo4j's full text indexes as
 * two procedures - one for updating indexes, and one for querying by label and
 * the lucene query language.
 */
public class GetPersonByIdent {
    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;

    @Context
    public GraphDatabaseService db;

    // Our constants
    private static final RelationshipType ev2p = RelationshipType.withName("crm_P140_assigned_attribute_to");
    private static final RelationshipType ev2id = RelationshipType.withName("crm_P37_assigned");
    private static final RelationshipType ev2ag = RelationshipType.withName("crm_P14_carried_out_by");
    private static final String personClass = "crm_E21_Person";
    private static final String assignment = "crm_E15_Identifier_Assignment";


    /**
     * This procedure takes a Node and gets the relationships going in and out of it
     *
     * @param ident  The identifier string of the person sought
     * @param authority The authority who assigned the given identifier string
     * @return  A RelationshipTypes instance with the relations (incoming and outgoing) for a given node.
     */
    @Procedure(value = "eu.r11.getPersonByIdent")
    @Description("Get an E21_Person node based on identifier assignment and agent.")
    public Stream<PersonRecord> getPersonByIdent(@Name("ident") String ident, @Name("authority") String authority) {
        List<PersonRecord> result = new ArrayList<>();
        try (Transaction tx = db.beginTx()) {
            Set<Node> matchedIdents = new HashSet<>();
            tx.findNodes(Label.label("crm_E42_Identifier"), "value", ident).forEachRemaining(n ->
                    n.getRelationships(Direction.INCOMING, ev2id).forEach(m -> matchedIdents.add(m.getStartNode())));
            Set<Node> matchedAuths = new HashSet<>();
            tx.findNodes(Label.label("crm_E42_Identifier"), "value", ident).forEachRemaining(n ->
                    n.getRelationships(Direction.INCOMING, ev2id).forEach(m -> matchedAuths.add(m.getStartNode())));
            matchedIdents.retainAll(matchedAuths);
            Set<Node> matchedPersons = matchedIdents.stream()
                    .map(x -> x.getSingleRelationship(ev2p, Direction.OUTGOING).getEndNode())
                    .collect(Collectors.toSet());
            matchedPersons.forEach(p -> result.add(new PersonRecord(p)));
        }
        return result.stream();
    }

    public static class PersonRecord {
        // These records contain two lists of distinct relationship types going in and out of a Node.
        public Node personNode;
        public Map<String,String> personIdents;

        public PersonRecord(Node person) {
            this.personNode = person;
            this.personIdents = new HashMap<>();
            personNode.getRelationships(Direction.INCOMING, ev2p)
                    .forEach(id -> {
                        Node identifier = id.getStartNode().getSingleRelationship(ev2id, Direction.OUTGOING).getEndNode();
                        Node agent = id.getStartNode().getSingleRelationship(ev2ag, Direction.OUTGOING).getEndNode();
                        personIdents.put(identifier.getProperty("value").toString(), agent.getProperty("identifier").toString());
                    });
        }
    }
}
