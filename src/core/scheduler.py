from collections import defaultdict
import logging
from ..db.connections import get_redshift_connection
from ..utils.config import load_config

# Configure logging
logging.basicConfig(
    filename='logs/scheduler.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def build_dependency_graph(mvs_config):
    """
    Build a complete dependency graph for the given MVs.
    Ensure all nodes are included, even those without dependencies.
    """
    dependency_graph = defaultdict(list)

    for mv_name in mvs_config.keys():
        mv_name = mv_name.strip()
        dependencies = collect_all_dependencies(mv_name)
        dependency_graph[mv_name] = [dep.strip() for dep in dependencies]

        for dep in dependencies:
            if dep.strip() not in dependency_graph:
                dependency_graph[dep.strip()] = []

    # Debug: Print the dependency graph
    logging.info(f"Dependency Graph: {dependency_graph}")
    return dependency_graph

def fetch_dependencies_from_redshift(mv_name):
    """
    Fetch direct dependencies of a Materialized View from Redshift.
    :param mv_name: The name of the MV whose dependencies are to be fetched.
    :return: A list of dependent MV names with whitespace trimmed.
    """
    conn = None  # Initialize conn to None
    try:
        conn = get_redshift_connection()
        cur = conn.cursor()

        # Query the Redshift system table for dependencies
        query = f"SELECT ref_name FROM STV_MV_DEPS WHERE name = '{mv_name.strip()}'"
        cur.execute(query)
        result = cur.fetchall()

        # Trim whitespace from all dependency names
        dependencies = [row[0].strip() for row in result]
        logging.info(f"Fetched dependencies for {mv_name.strip()}: {dependencies}")
        return dependencies
    except Exception as e:
        logging.error(f"Error fetching dependencies for {mv_name.strip()}: {e}")
        return []
    finally:
        if conn:
            cur.close()
            conn.close()

def collect_all_dependencies(mv_name, visited=None):
    """
    Recursively collect all dependencies for a given MV.
    """
    if visited is None:
        visited = set()

    if mv_name in visited:
        return []

    visited.add(mv_name)
    dependencies = fetch_dependencies_from_redshift(mv_name)
    
    all_dependencies = set(dependencies)
    for dep in dependencies:
        sub_dependencies = collect_all_dependencies(dep, visited)
        all_dependencies.update(sub_dependencies)

    return list(all_dependencies)

if __name__ == "__main__":
    # Load configuration
    config = load_config()
    if config:
        # Build dependency graph
        dependency_graph = build_dependency_graph(config.get('mvs', {}))
        logging.info("Scheduler started successfully") 