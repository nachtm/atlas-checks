package org.openstreetmap.atlas.checks.validation.linear.edges;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import org.openstreetmap.atlas.checks.atlas.predicates.TagPredicates;
import org.openstreetmap.atlas.checks.base.BaseCheck;
import org.openstreetmap.atlas.checks.flag.CheckFlag;
import org.openstreetmap.atlas.geography.atlas.items.AtlasObject;
import org.openstreetmap.atlas.geography.atlas.items.Edge;
import org.openstreetmap.atlas.geography.atlas.items.Node;
import org.openstreetmap.atlas.tags.AerowayTag;
import org.openstreetmap.atlas.tags.AmenityTag;
import org.openstreetmap.atlas.tags.HighwayTag;
import org.openstreetmap.atlas.tags.RouteTag;
import org.openstreetmap.atlas.tags.SyntheticBoundaryNodeTag;
import org.openstreetmap.atlas.tags.annotations.validation.Validators;
import org.openstreetmap.atlas.utilities.configuration.Configuration;

/**
 * This check flags islands of roads where it is impossible to get out. The simplest is a one-way
 * that dead-ends; that would be a one-edge island.
 *
 * @author matthieun
 * @author cuthbertm
 * @author gpogulsky
 * @author savannahostrowski
 * @author nachtm
 */
public class SinkIslandCheck extends BaseCheck<Long>
{
    private static final float LOAD_FACTOR = 0.8f;
    private static final long TREE_SIZE_DEFAULT = 50;
    private static final List<String> FALLBACK_INSTRUCTIONS = Collections
            .singletonList("Road is impossible to get out of.");
    private static final String MOTORCYCLE_PARKING_AMENITY = "MOTORCYCLE_PARKING";
    private static final String PARKING_ENTRANCE_AMENITY = "PARKING_ENTRANCE";
    private static final List<String> amenityValuesToExclude = Arrays.asList(
            AmenityTag.PARKING.toString(), AmenityTag.PARKING_SPACE.toString(),
            MOTORCYCLE_PARKING_AMENITY, PARKING_ENTRANCE_AMENITY);
    private static final HighwayTag UNIMPORTANT_HIGHWAY = HighwayTag.NO;
    private static final HighwayTag MINIMUM_IMPORTANCE_HIGHWAY = HighwayTag.SERVICE;
    private static final long serialVersionUID = -1432150496331502258L;
    private final int storeSize;
    private final int treeSize;

    /**
     * Default constructor
     *
     * @param configuration
     *            the JSON configuration for this check
     */
    public SinkIslandCheck(final Configuration configuration)
    {
        super(configuration);
        this.treeSize = configurationValue(configuration, "tree.size", TREE_SIZE_DEFAULT,
                Math::toIntExact);

        // LOAD_FACTOR 0.8 gives us default initial capacity 50 / 0.8 = 62.5
        // map & queue will allocate 64 (the nearest power of 2) for that initial capacity
        // Our algorithm does not allow neither explored set nor candidates queue exceed
        // this.treeSize
        // Therefore underlying map/queue we will never re-double the capacity
        this.storeSize = (int) (this.treeSize / LOAD_FACTOR);
    }

    @Override
    public boolean validCheckForObject(final AtlasObject object)
    {
        // This is a valid edge
        return this.validEdge(object)
                // We haven't flagged it before
                && !this.isFlagged(object.getIdentifier())
                // Only look at edges with a highway tag at least as significant as
                // MINIMUM_IMPORTANCE_HIGHWAY
                && HighwayTag.highwayTag(object).orElse(UNIMPORTANT_HIGHWAY)
                        .isMoreImportantThanOrEqualTo(MINIMUM_IMPORTANCE_HIGHWAY);
    }

    @Override
    protected Optional<CheckFlag> flag(final AtlasObject object)
    {
        // The current edge to be explored
        final Edge candidate = (Edge) object;

        // Analyze candidate's neighbors to determine whether it is part of a sink node
        final Optional<Set<AtlasObject>> explored = buildNetwork(candidate);

        // Set every explored edge as flagged for any other processes to know that we have already
        // process all those edges
        explored.ifPresent(objs -> objs.forEach(marked -> this.markAsFlagged(marked.getIdentifier())));

        // If we found any sink islands, flag them here.
        return explored.map(toTag -> createFlag(toTag, this.getLocalizedInstruction(0)));
    }

    @Override
    protected List<String> getFallbackInstructions()
    {
        return FALLBACK_INSTRUCTIONS;
    }

    /**
     * This function will check various elements of the edge to make sure that we should be looking
     * at it.
     *
     * @param object
     *            the edge to check whether we want to continue looking at it
     * @return {@code true} if is a valid object to look at
     */
    private boolean validEdge(final AtlasObject object)
    {
        return object instanceof Edge
                // Ignore any airport taxiways and runways, as these often create a sink island
                && !Validators.isOfType(object, AerowayTag.class, AerowayTag.TAXIWAY,
                        AerowayTag.RUNWAY)
                // Only allow car navigable highways and ignore ferries
                && HighwayTag.isCarNavigableHighway(object) && !RouteTag.isFerry(object)
                // Ignore any highways tagged as areas
                && !TagPredicates.IS_AREA.test(object);
    }

    /**
     * This function will check edges and will determine whether they are outside the scope of this
     * check or not. Currently, edges which contain nodes with a synthetic boundary node or edges
     * that contain end nodes with certain parking tags are disqualified from this check.
     *
     * @param edge The edge we want to look at
     * @return {@code true} if we should stop examining this component of the network because it
     *         contains this node. {@code false} if we should continue looking at this component.
     */
    private boolean outsideOfScope(final Edge edge)
    {
        // If we've way-sectioned at the border, don't consider this component of the network
        return SyntheticBoundaryNodeTag.isBoundaryNode(edge.end())
                || SyntheticBoundaryNodeTag.isBoundaryNode(edge.start())
                // Don't consider components that end in nodes with certain amenity tags
                || endNodeHasAmenityTypeToExclude(edge);
    }

    /**
     * This function checks to see if the end node of an Edge AtlasObject has an amenity tag with
     * one of the amenityValuesToExclude.
     * 
     * @param object
     *            An AtlasObject (known to be an Edge)
     * @return {@code true} if the end node of the end has one of the amenityValuesToExclude, and
     *         {@code false} otherwise
     */
    private boolean endNodeHasAmenityTypeToExclude(final AtlasObject object)
    {
        final Edge edge = (Edge) object;
        final Node endNode = edge.end();

        return Validators.isOfType(endNode, AmenityTag.class, AmenityTag.PARKING,
                AmenityTag.PARKING_SPACE, AmenityTag.MOTORCYCLE_PARKING,
                AmenityTag.PARKING_ENTRANCE);
    }

    /**
     * Analyze the edge network starting at start, and return the sink island, if one exists. If we
     * find any nodes that have already been flagged, then we quit immediately, assuming that the
     * other process correctly labeled this connected component. Similarly, if we find any edges
     * deemed to be outside the scope of this check, or the network grows past the maximum tree
     * size, quit immediately.
     * @param start - the initial edge in the potential sink island.
     * @return an Optional<Set<AtlasObject>> containing a sink island, if one exists.
     * Otherwise, Optional.empty().
     */
    private Optional<Set<AtlasObject>> buildNetwork(final Edge start)
    {
        final Set<AtlasObject> explored = new HashSet<>(this.storeSize, LOAD_FACTOR);
        final Set<AtlasObject> terminal = new HashSet<>();
        final Queue<Edge> candidates = new ArrayDeque<>();
        Edge candidate = start;

        // Start edge always explored
        explored.add(candidate);

        // Keep looping while we still have a valid candidate to explore
        while (candidate != null)
        {
            // If the edge has already been flagged by another process then we can break out of the
            // loop and assume that whether the check was a flag or not was handled by the other
            // process
            if (this.isFlagged(candidate.getIdentifier()))
            {
                return Optional.empty();
            }

            // Retrieve all the valid outgoing edges to explore
            final Set<Edge> outEdges = candidate.outEdges();

            if (outEdges.isEmpty())
            {
                // Sink edge. Don't mark the edge explored until we know how big the tree is
                terminal.add(candidate);
            }
            else
            {
                // Add the current candidate to the set of already explored edges
                explored.add(candidate);

                // From the list of outgoing edges from the current candidate filter out any edges
                // that have already been explored and add all the rest to the queue of possible
                // candidates
                for (final Edge edge : outEdges)
                {
                    if (this.validEdge(edge) && !explored.contains(edge))
                    {
                        // Don't tag this network if we run into an edge outside the scope of this check.
                        if (this.outsideOfScope(edge))
                        {
                            return Optional.empty();
                        }
                        candidates.add(edge);
                    }
                }

                // If the number of available candidates and the size of the currently explored
                // items is larger then the configurable tree size, then we can break out of the
                // loop and assume that this is not a SinkIsland
                if (candidates.size() + explored.size() > this.treeSize)
                {
                    return Optional.empty();
                }
            }

            // Get the next candidate
            candidate = candidates.poll();
        }

        // If we exit due to tree size (emptyFlag == true) and there are terminal edges we could
        // cache them and check on entry to this method. However it seems to happen too rare in
        // practice. So these edges (if any) will be processed as all others. Even though they would
        // not generate any candidates. Otherwise if we covered the whole tree, there is no need to
        // delay processing of terminal edges. We should add them to the geometry we are going to
        // flag.
        // Include all touched edges
        explored.addAll(terminal);

        return Optional.of(explored);
    }
}
