"""Build network graphs from hydrographic data.

Creates directed graphs representing upstream/downstream relationships
for hydrological analysis.
"""

import json
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import geopandas as gpd
import networkx as nx
from shapely.geometry import LineString, Point
import structlog

logger = structlog.get_logger(__name__)


class GraphBuilder:
    """Build network graphs from hydrographic segments."""

    def __init__(self, task_id: str):
        """Initialize graph builder.

        Args:
            task_id: Task identifier
        """
        self.task_id = task_id
        self.logger = logger.bind(task_id=task_id)

    def build_hydro_graph(
        self,
        troncons_file: Path,
        output_file: Path,
        connectivity_tolerance: float = 1.0
    ) -> nx.DiGraph:
        """Build directed graph from hydrographic segments.

        Args:
            troncons_file: GeoJSON file with line segments (BD TOPAGE)
            output_file: Output JSON file for graph
            connectivity_tolerance: Tolerance in meters for connecting segments

        Returns:
            NetworkX directed graph
        """
        self.logger.info(
            "Building hydrographic graph",
            troncons=str(troncons_file),
            tolerance=connectivity_tolerance
        )

        # Load segments
        troncons = gpd.read_file(troncons_file)

        # Convert to projected CRS for accurate distance
        troncons = troncons.to_crs('EPSG:2154')  # Lambert 93

        # Create directed graph
        G = nx.DiGraph()

        # Add nodes (segments) with attributes
        for idx, row in troncons.iterrows():
            segment_id = row.get('id_troncon', f'segment_{idx}')

            # Get start and end points
            geom = row.geometry
            if isinstance(geom, LineString):
                start_point = Point(geom.coords[0])
                end_point = Point(geom.coords[-1])

                G.add_node(
                    segment_id,
                    geometry=geom.wkt,
                    start_point=start_point.wkt,
                    end_point=end_point.wkt,
                    classification=row.get('classif', ''),
                    length_m=geom.length
                )

        self.logger.info(f"Added {len(G.nodes)} nodes to graph")

        # Build spatial index for efficient connectivity search
        # For each segment, find segments whose start point is near this segment's end point
        edges_added = 0

        for node_id in G.nodes:
            node_data = G.nodes[node_id]
            end_point_wkt = node_data['end_point']
            end_point = Point(*map(float, end_point_wkt.replace('POINT (', '').replace(')', '').split()))

            # Find potential downstream segments
            for other_id in G.nodes:
                if node_id == other_id:
                    continue

                other_data = G.nodes[other_id]
                other_start_wkt = other_data['start_point']
                other_start = Point(*map(float, other_start_wkt.replace('POINT (', '').replace(')', '').split()))

                # Check if end of this segment connects to start of other
                distance = end_point.distance(other_start)

                if distance <= connectivity_tolerance:
                    # Add directed edge (this -> other means water flows from this to other)
                    G.add_edge(
                        node_id,
                        other_id,
                        distance=distance
                    )
                    edges_added += 1

        self.logger.info(f"Added {edges_added} edges to graph")

        # Calculate some graph metrics
        metrics = {
            'nodes': len(G.nodes),
            'edges': len(G.edges),
            'connected_components': nx.number_weakly_connected_components(G),
            'density': nx.density(G)
        }

        self.logger.info("Graph metrics", **metrics)

        # Export graph to JSON
        graph_data = nx.node_link_data(G)
        graph_data['metrics'] = metrics

        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(graph_data, f, indent=2)

        self.logger.info("Graph saved", output=str(output_file))

        return G

    def find_upstream_segments(
        self,
        graph: nx.DiGraph,
        segment_id: str,
        max_depth: Optional[int] = None
    ) -> Set[str]:
        """Find all upstream segments from a given segment.

        Args:
            graph: NetworkX directed graph
            segment_id: Starting segment ID
            max_depth: Maximum depth to search (None = unlimited)

        Returns:
            Set of upstream segment IDs
        """
        upstream = set()

        # Reverse graph to find predecessors (upstream)
        G_reversed = graph.reverse()

        # BFS from segment
        if max_depth is None:
            upstream = nx.descendants(G_reversed, segment_id)
        else:
            # Limited depth search
            visited = {segment_id}
            queue = [(segment_id, 0)]

            while queue:
                current, depth = queue.pop(0)

                if max_depth is not None and depth >= max_depth:
                    continue

                for predecessor in G_reversed.predecessors(current):
                    if predecessor not in visited:
                        visited.add(predecessor)
                        upstream.add(predecessor)
                        queue.append((predecessor, depth + 1))

        return upstream

    def find_downstream_segments(
        self,
        graph: nx.DiGraph,
        segment_id: str,
        max_depth: Optional[int] = None
    ) -> Set[str]:
        """Find all downstream segments from a given segment.

        Args:
            graph: NetworkX directed graph
            segment_id: Starting segment ID
            max_depth: Maximum depth to search (None = unlimited)

        Returns:
            Set of downstream segment IDs
        """
        if max_depth is None:
            return nx.descendants(graph, segment_id)
        else:
            # Limited depth search
            visited = {segment_id}
            downstream = set()
            queue = [(segment_id, 0)]

            while queue:
                current, depth = queue.pop(0)

                if max_depth is not None and depth >= max_depth:
                    continue

                for successor in graph.successors(current):
                    if successor not in visited:
                        visited.add(successor)
                        downstream.add(successor)
                        queue.append((successor, depth + 1))

            return downstream

    def calculate_upstream_area(
        self,
        graph: nx.DiGraph,
        segment_id: str
    ) -> float:
        """Calculate total upstream length (proxy for drainage area).

        Args:
            graph: NetworkX directed graph
            segment_id: Segment ID

        Returns:
            Total upstream length in meters
        """
        upstream_segments = self.find_upstream_segments(graph, segment_id)

        total_length = 0.0
        for seg_id in upstream_segments:
            seg_data = graph.nodes[seg_id]
            total_length += seg_data.get('length_m', 0.0)

        # Include the segment itself
        total_length += graph.nodes[segment_id].get('length_m', 0.0)

        return total_length

    def find_main_stem(
        self,
        graph: nx.DiGraph,
        outlet_id: str
    ) -> List[str]:
        """Find main stem (longest path) from outlet to headwaters.

        Args:
            graph: NetworkX directed graph
            outlet_id: Outlet segment ID

        Returns:
            List of segment IDs forming main stem
        """
        # Reverse graph
        G_reversed = graph.reverse()

        # Find all simple paths from outlet to sources (nodes with no predecessors)
        sources = [node for node in G_reversed.nodes if G_reversed.out_degree(node) == 0]

        longest_path = []
        max_length = 0.0

        for source in sources:
            try:
                paths = nx.all_simple_paths(G_reversed, outlet_id, source)

                for path in paths:
                    # Calculate path length
                    path_length = sum(
                        graph.nodes[seg_id].get('length_m', 0.0)
                        for seg_id in path
                    )

                    if path_length > max_length:
                        max_length = path_length
                        longest_path = path

            except nx.NetworkXNoPath:
                continue

        self.logger.info(
            "Main stem found",
            outlet=outlet_id,
            length_m=max_length,
            segments=len(longest_path)
        )

        return longest_path

    def load_graph(self, graph_file: Path) -> nx.DiGraph:
        """Load graph from JSON file.

        Args:
            graph_file: Path to graph JSON file

        Returns:
            NetworkX directed graph
        """
        with open(graph_file, 'r', encoding='utf-8') as f:
            graph_data = json.load(f)

        G = nx.node_link_graph(graph_data, directed=True)

        self.logger.info(
            "Graph loaded",
            file=str(graph_file),
            nodes=len(G.nodes),
            edges=len(G.edges)
        )

        return G
