"""
Manifest Resolver - Builds dependency graph between assets and content.

This layer analyzes the manifest to:
1. Identify all asset dependencies
2. Detect broken references
3. Enable parallel processing with proper ordering
"""

from pathlib import Path
from typing import Dict, List, Set, Tuple
from dataclasses import dataclass, field
import xml.etree.ElementTree as ET

from models.canonical_models import CanonicalCourse, CanonicalAsset, CanonicalAssessment
from observability.logger import get_logger

logger = get_logger(__name__)


@dataclass
class DependencyNode:
    """A node in the dependency graph."""
    identifier: str
    type: str  # 'module', 'assessment', 'asset', 'page'
    depends_on: List[str] = field(default_factory=list)
    depended_by: List[str] = field(default_factory=list)
    status: str = "pending"  # pending, processing, complete, failed
    source_path: Optional[str] = None


class ManifestResolver:
    """
    Analyzes course structure and builds a dependency graph.
    
    This enables:
    - Parallel asset processing with proper ordering
    - Broken link detection before processing
    - Resume capability (checkpoint at failed nodes)
    """
    
    def __init__(self, course_dir: Path):
        self.course_dir = Path(course_dir)
        self.nodes: Dict[str, DependencyNode] = {}
        self.asset_nodes: Dict[str, DependencyNode] = {}
        self.content_nodes: Dict[str, DependencyNode] = {}
    
    def resolve(self) -> Dict[str, DependencyNode]:
        """
        Build the complete dependency graph for the course.
        
        Returns:
            Dictionary of identifier -> DependencyNode
        """
        manifest_path = self.course_dir / "imsmanifest.xml"
        
        if not manifest_path.exists():
            logger.warning("No imsmanifest.xml found")
            return {}
        
        # Parse manifest
        try:
            tree = ET.parse(str(manifest_path))
            root = tree.getroot()
        except Exception as e:
            logger.error(f"Failed to parse manifest: {e}")
            return {}
        
        # Build resource nodes
        self._process_resources(root)
        
        # Build organization dependencies
        self._process_organizations(root)
        
        return self.nodes
    
    def _process_resources(self, root):
        """Process <resources> section of manifest."""
        resources_elem = root.find("resources")
        if resources_elem is None:
            return
        
        for resource in resources_elem.findall("resource"):
            resid = resource.get("identifier", "")
            if not resid:
                continue
            
            href = resource.get("href", "")
            resource_type = resource.get("type", "")
            
            node = DependencyNode(
                identifier=resid,
                type="asset" if self._is_asset_type(resource_type) else "content",
                source_path=href
            )
            self.nodes[resid] = node
            self.asset_nodes[resid] = node
            
            # Process file references within resource
            for file_elem in resource.findall(".//file"):
                file_href = file_elem.get("href", "")
                if file_href:
                    file_node = DependencyNode(
                        identifier=f"{resid}_file_{file_href}",
                        type="file",
                        source_path=file_href,
                        depends_on=[resid]
                    )
                    self.nodes[file_node.identifier] = file_node
    
    def _process_organizations(self, root):
        """Process <organizations> section to build module dependencies."""
        orgs = root.find("organizations")
        if orgs is None:
            return
        
        for org in orgs.findall("organization"):
            for item in org.findall(".//item"):
                self._process_item(item, None)
    
    def _process_item(self, item, parent_id):
        """Process a TOC item and its children."""
        item_id = item.get("identifier", "")
        identifierref = item.get("identifierref", "")
        
        node = DependencyNode(
            identifier=item_id,
            type="content"
        )
        self.nodes[item_id] = node
        self.content_nodes[item_id] = node
        
        # Link to resource
        if identifierref and identifierref in self.nodes:
            node.depends_on.append(identifierref)
            self.nodes[identifierref].depended_by.append(item_id)
        
        # Process children
        for child in item.findall("item"):
            self._process_item(child, item_id)
    
    def _is_asset_type(self, resource_type: str) -> bool:
        """Check if resource type represents an asset files."""
        asset_types = [
            "webcontent",
            "resource",
            "/files",
            ".pdf",
            ".pptx",
            ".docx",
            ".zip",
        ]
        return any(at in resource_type.lower() for at in asset_types)
    
    def get_processing_order(self) -> List[List[str]]:
        """
        Get topological sort of nodes for parallel processing.
        
        Returns:
            List of batches, where each batch can be processed in parallel.
        """
        in_degree = {node_id: 0 for node_id in self.nodes}
        
        # Calculate in-degrees
        for node in self.nodes.values():
            for dep in node.depends_on:
                if dep in in_degree:
                    in_degree[node.identifier] += 1
        
        # BFS to find levels
        batches = []
        remaining = set(self.nodes.keys())
        
        while remaining:
            # Find all nodes with no remaining dependencies
            batch = [
                node_id for node_id in remaining
                if all(d not in remaining for d in self.nodes[node_id].depends_on)
            ]
            
            if not batch:
                # Circular dependency - break one
                batch = [remaining.pop()]
                logger.warning(f"Breaking circular dependency at {batch[0]}")
                continue
            
            batches.append(batch)
            remaining -= set(batch)
        
        return batches
    
    def find_orphaned_files(self) -> List[str]:
        """Find files in the course directory not referenced in manifest."""
        referenced = set()
        for node in self.nodes.values():
            if node.source_path:
                referenced.add(node.source_path)
        
        orphaned = []
        for file_path in self.course_dir.rglob("*"):
            if file_path.is_file():
                rel_path = str(file_path.relative_to(self.course_dir))
                if rel_path not in referenced and "tutor_lms_output" not in rel_path:
                    orphaned.append(rel_path)
        
        return orphaned