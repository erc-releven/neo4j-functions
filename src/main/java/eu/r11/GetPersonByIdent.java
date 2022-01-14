package eu.r11;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    /**
     * This procedure takes a Node and gets the relationships going in and out of it
     *
     * @param ident  The identifier string of the person sought
     * @param authority The authority who assigned the given identifier string
     * @return  A RelationshipTypes instance with the relations (incoming and outgoing) for a given node.
     */
    @Procedure(value = "eu.r11.getPersonByIdent")
    @Description("Get an E21_Person node based on identifier assignment and agent.")
    public Stream<Node> getPersonByIdent(@Name("ident") String ident, @Name("authority") String authority) {
        String q = String.format("MATCH (ass:crm_E15_Identifier_Assignment)-[:crm_P37_assigned]->(idlabel:crm_E42_Identifier {value:'%s'}), " +
                "(ass)-[:crm_P14_carried_out_by]->(agent:crm_E39_Actor {identifier:'%s'), " +
                "(ass)-[:crm_p140_assigned_attribute_to]->(p:crm_E21_Person) RETURN p", ident, authority);
        Result r;
        try (Transaction tx = db.beginTx()) {
            r = tx.execute(q);
            tx.commit();
        }
        ArrayList<Node> result = new ArrayList<>();
        if (r.hasNext())
            r.forEachRemaining(x -> result.add((Node) x.get("p")));

        return result.stream();
    }

    /**
     * Adds the distinct type of a relationship to the given List<String>
     *
     * @param list  the list to add the distinct relationship type to
     * @param relationship  the relationship to get the name() from
     */
    private void AddDistinct(List<String> list, Relationship relationship){
        AddDistinct(list, relationship.getType().name());
    }

    /**
     * Adds an item to a List only if the item is not already in the List
     *
     * @param list  the list to add the distinct item to
     * @param item  the item to add to the list
     */
    private <T> void AddDistinct(List<T> list, T item){
        if(!list.contains(item))
            list.add(item);
    }

    /**
     * This is the output record for our search procedure. All procedures
     * that return results return them as a Stream of Records, where the
     * records are defined like this one - customized to fit what the procedure
     * is returning.
     * <p>
     * These classes can only have public non-final fields, and the fields must
     * be one of the following types:
     *
     * <ul>
     *     <li>{@link String}</li>
     *     <li>{@link Long} or {@code long}</li>
     *     <li>{@link Double} or {@code double}</li>
     *     <li>{@link Number}</li>
     *     <li>{@link Boolean} or {@code boolean}</li>
     *     <li>{@link Node}</li>
     *     <li>{@link org.neo4j.graphdb.Relationship}</li>
     *     <li>{@link org.neo4j.graphdb.Path}</li>
     *     <li>{@link Map} with key {@link String} and value {@link Object}</li>
     *     <li>{@link List} of elements of any valid field type, including {@link List}</li>
     *     <li>{@link Object}, meaning any of the valid field types</li>
     * </ul>
     */
    public static class RelationshipTypes {
        // These records contain two lists of distinct relationship types going in and out of a Node.
        public List<String> outgoing;
        public List<String> incoming;

        public RelationshipTypes(List<String> incoming, List<String> outgoing) {
            this.outgoing = outgoing;
            this.incoming = incoming;
        }
    }
}
