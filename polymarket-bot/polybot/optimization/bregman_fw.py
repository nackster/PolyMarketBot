"""Bregman projection via Frank-Wolfe (LMSR/KL divergence).

This is a generic optimizer that needs a LinearOracle to define the feasible set
(e.g., a marginal polytope). It does NOT perform dependency detection on its own.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Protocol, Sequence, Tuple

import math

_EPS = 1e-12


class LinearOracle(Protocol):
    """Return a feasible vertex z minimizing c·z."""

    def solve(self, c: Sequence[float]) -> Sequence[float]:
        ...


@dataclass(slots=True)
class ProjectionResult:
    mu: List[float]
    gap: float
    iters: int
    history: List[float]


class SimplexOracle:
    """Oracle for the probability simplex (one-hot vertices)."""

    def __init__(self, n: int):
        if n <= 0:
            raise ValueError("n must be > 0")
        self.n = int(n)

    def solve(self, c: Sequence[float]) -> Sequence[float]:
        if len(c) != self.n:
            raise ValueError("c length must equal n")
        # minimize c·z where z is one-hot -> pick argmin c
        k = min(range(self.n), key=lambda i: float(c[i]))
        return [1.0 if i == k else 0.0 for i in range(self.n)]


def _clip_positive(x: float) -> float:
    return float(x) if x > _EPS else _EPS


def _normalize(x: Sequence[float]) -> List[float]:
    s = float(sum(x))
    if s <= 0:
        return [1.0 / len(x) for _ in x]
    return [float(v) / s for v in x]


def kl_divergence(mu: Sequence[float], theta: Sequence[float]) -> float:
    """KL(mu || theta) with safe clipping (both positive).

    KL = sum mu_i * log(mu_i / theta_i)
    """
    if len(mu) != len(theta):
        raise ValueError("mu and theta must have same length")
    total = 0.0
    for m, t in zip(mu, theta):
        m = _clip_positive(float(m))
        t = _clip_positive(float(t))
        total += m * math.log(m / t)
    return float(total)


def _grad_kl(mu: Sequence[float], theta: Sequence[float]) -> List[float]:
    if len(mu) != len(theta):
        raise ValueError("mu and theta must have same length")
    grad: List[float] = []
    for m, t in zip(mu, theta):
        m = _clip_positive(float(m))
        t = _clip_positive(float(t))
        grad.append(math.log(m / t))
    return grad


def _dot(a: Sequence[float], b: Sequence[float]) -> float:
    return float(sum(float(x) * float(y) for x, y in zip(a, b)))


class FrankWolfeProjector:
    """Frank-Wolfe for KL-based Bregman projection.

    Objective: minimize KL(mu || theta) over mu in feasible polytope.
    Requires LinearOracle to provide min_c c·z over vertices z.
    """

    def __init__(self, max_iter: int = 150, tol: float = 1e-6, use_line_search: bool = False):
        self.max_iter = int(max_iter)
        self.tol = float(tol)
        self.use_line_search = bool(use_line_search)

    def project(
        self,
        theta: Sequence[float],
        oracle: LinearOracle,
        init: Optional[Sequence[float]] = None,
    ) -> ProjectionResult:
        if len(theta) == 0:
            raise ValueError("theta must be non-empty")

        theta_clipped = [_clip_positive(float(t)) for t in theta]

        if init is None:
            # Any feasible vertex is a valid starting point
            init = oracle.solve([0.0 for _ in theta_clipped])

        mu = [float(x) for x in init]
        history: List[float] = []

        for t in range(self.max_iter):
            grad = _grad_kl(mu, theta_clipped)
            s = list(oracle.solve(grad))
            gap = _dot(grad, [m - si for m, si in zip(mu, s)])
            history.append(gap)

            if gap <= self.tol:
                return ProjectionResult(mu=list(mu), gap=float(gap), iters=t + 1, history=history)

            if self.use_line_search:
                gamma = self._line_search(mu, s, theta_clipped)
            else:
                gamma = 2.0 / float(t + 2)

            mu = [m + gamma * (si - m) for m, si in zip(mu, s)]

        final_gap = history[-1] if history else float("inf")
        return ProjectionResult(mu=list(mu), gap=float(final_gap), iters=self.max_iter, history=history)

    def _line_search(self, mu: Sequence[float], s: Sequence[float], theta: Sequence[float]) -> float:
        """Simple backtracking line search on [0,1] for KL objective."""
        lo, hi = 0.0, 1.0
        best_gamma = 0.0
        best_val = kl_divergence(mu, theta)

        # Ternary-like search (fixed iterations)
        for _ in range(25):
            g1 = lo + (hi - lo) / 3.0
            g2 = hi - (hi - lo) / 3.0
            mu1 = [m + g1 * (si - m) for m, si in zip(mu, s)]
            mu2 = [m + g2 * (si - m) for m, si in zip(mu, s)]
            v1 = kl_divergence(mu1, theta)
            v2 = kl_divergence(mu2, theta)
            if v1 <= v2:
                hi = g2
                if v1 < best_val:
                    best_val = v1
                    best_gamma = g1
            else:
                lo = g1
                if v2 < best_val:
                    best_val = v2
                    best_gamma = g2

        return float(best_gamma)
