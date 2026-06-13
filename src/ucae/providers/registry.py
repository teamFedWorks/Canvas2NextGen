from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict
from src.ucae.providers.base import BaseLmsProvider, MatchResult
from src.ucae.workflow.workspace import ExtractedWorkspace


@dataclass(frozen=True)
class MatchCandidate:
    """
    Represents a candidate provider resolved during format detection,
    storing the provider instance and the calculated match result metrics.
    """
    provider: BaseLmsProvider
    result: MatchResult


class ProviderRegistry:
    """
    Registry for available LMS providers.
    Responsible for registering providers and detecting the correct provider
    for a given extracted workspace based on structural format checks.
    """
    def __init__(self):
        self._providers: List[BaseLmsProvider] = []

    def register(self, provider: BaseLmsProvider) -> None:
        """Registers a new provider instance, keeping registry sorted by priority."""
        if any(p.metadata.id == provider.metadata.id for p in self._providers):
            raise ValueError(f"Provider with ID '{provider.metadata.id}' is already registered.")
        
        self._providers.append(provider)
        # Sort by priority descending, so higher priority providers detect first
        self._providers.sort(key=lambda p: p.metadata.priority, reverse=True)

    def get_providers(self) -> List[BaseLmsProvider]:
        """Returns the list of registered providers."""
        return list(self._providers)

    def detect_provider(self, workspace: ExtractedWorkspace) -> List[MatchCandidate]:
        """
        Evaluates all registered providers against the extracted workspace.
        Returns a list of MatchCandidate objects sorted by match confidence descending.
        """
        candidates: List[MatchCandidate] = []

        for provider in self._providers:
            try:
                result = provider.detect(workspace)
                candidates.append(MatchCandidate(provider=provider, result=result))
            except Exception:
                # Shield registry from detection crashes, report match as failed
                candidates.append(
                    MatchCandidate(
                        provider=provider, 
                        result=MatchResult(matched=False, confidence=0.0)
                    )
                )

        # Sort candidates: matched first, then by confidence descending, then priority descending
        candidates.sort(
            key=lambda c: (
                1 if c.result.matched else 0,
                c.result.confidence,
                c.provider.metadata.priority
            ),
            reverse=True
        )

        return candidates
