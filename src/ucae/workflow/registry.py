from abc import ABC, abstractmethod
from typing import Dict, List
from src.ucae.workflow.context import PipelineContext


class PipelineStep(ABC):
    """
    Abstract base class representing a single discrete execution step in the pipeline.
    Stages (e.g. parse, validate, upload, export) inherit from this.
    """
    @property
    @abstractmethod
    def name(self) -> str:
        """Returns the unique name of this pipeline step."""
        pass

    @abstractmethod
    def execute(self, context: PipelineContext) -> None:
        """
        Executes the logic for this step, reading from and writing to the PipelineContext.
        """
        pass


class PipelineRegistry:
    """
    Registry for mapping provider formats to their corresponding list of execution steps.
    This separates the responsibility of execution/orchestration from provider parsing.
    """
    def __init__(self):
        self._pipelines: Dict[str, List[PipelineStep]] = {}

    def register_pipeline(self, provider_id: str, steps: List[PipelineStep]) -> None:
        """Registers a list of pipeline steps for a specific provider format."""
        self._pipelines[provider_id] = list(steps)

    def get_pipeline_steps(self, provider_id: str) -> List[PipelineStep]:
        """Gets the pipeline steps registered for the given provider."""
        if provider_id not in self._pipelines:
            raise KeyError(f"No pipeline registered for provider '{provider_id}'")
        return list(self._pipelines[provider_id])
