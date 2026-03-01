"""Optimization helpers (Bregman projection / Frank-Wolfe)."""

from .bregman_fw import (
    FrankWolfeProjector,
    LinearOracle,
    ProjectionResult,
    SimplexOracle,
    kl_divergence,
)

__all__ = [
    "FrankWolfeProjector",
    "LinearOracle",
    "ProjectionResult",
    "SimplexOracle",
    "kl_divergence",
]
