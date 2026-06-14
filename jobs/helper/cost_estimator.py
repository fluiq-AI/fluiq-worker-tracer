"""Estimate the dollar cost of a single trace event using PostgreSQL prices.

The price table (``model_prices``) stores costs per million tokens. Given a
trace event with provider/model/usage metadata, this module produces a cost
breakdown suitable for insertion into ``fluiq.trace_costs``.
"""
from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Optional

from db.postgresql import postgres_client

logger = logging.getLogger(__name__)

_MILLION = Decimal(1_000_000)

_PROVIDER_ALIASES = {
    "OPENAI": "OpenAI",
    "ANTHROPIC": "Anthropic",
    "GEMINI": "Google",
    "GOOGLEADK": "Google",
}

# Some providers are reached through the OpenAI SDK (OpenAI-compatible API),
# so the trace's ``integration`` is always "OpenAI" even though another vendor
# actually served — and priced — the call. Kimi/Moonshot is one such case:
# clients point the OpenAI SDK at ``https://api.moonshot.ai/v1`` and call
# models like ``kimi-k2.6`` or ``moonshot-v1-8k``. The model id is the only
# signal we have here, so map those prefixes to the real provider whose rows
# live in ``model_prices``. Keys are matched against the lower-cased model id.
_MODEL_PREFIX_PROVIDERS = (
    ("kimi", "Kimi"),
    ("moonshot", "Kimi"),
)


def _provider_from_model(model: Any) -> Optional[str]:
    """Resolve the real provider from the model id for OpenAI-compatible vendors."""
    if not model:
        return None
    name = str(model).strip().lower()
    for prefix, provider in _MODEL_PREFIX_PROVIDERS:
        if name.startswith(prefix):
            return provider
    return None


def _resolve_provider(integration: Any, model: Any = None) -> Optional[str]:
    # A vendor served via the OpenAI-compatible API wins over the SDK-reported
    # integration so the price lookup targets the right rows (e.g. Kimi rather
    # than OpenAI for ``kimi-k2.6``).
    by_model = _provider_from_model(model)
    if by_model:
        return by_model
    if not integration:
        return None
    key = str(integration).strip().upper()
    return _PROVIDER_ALIASES.get(key)


def _coerce_int(value: Any) -> int:
    if value in (None, ""):
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _extract_tokens(event: dict[str, Any]) -> dict[str, int]:
    """Return {prompt, completion, cached} normalised across SDK shapes."""
    tokens = event.get("tokens") or {}
    if not isinstance(tokens, dict):
        return {"prompt": 0, "completion": 0, "cached": 0}

    prompt = (
        tokens.get("prompt")
        or tokens.get("input_tokens")
        or tokens.get("prompt_tokens")
        or 0
    )
    completion = (
        tokens.get("completion")
        or tokens.get("output_tokens")
        or tokens.get("completion_tokens")
        or 0
    )

    cached = 0
    details = tokens.get("input_tokens_details") or tokens.get("prompt_tokens_details")
    if isinstance(details, dict):
        cached = details.get("cached_tokens") or 0
    cached = cached or tokens.get("cached_tokens") or tokens.get("cached") or 0

    return {
        "prompt": _coerce_int(prompt),
        "completion": _coerce_int(completion),
        "cached": _coerce_int(cached),
    }


def _to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal(0)
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _compute(price: dict[str, Any], tokens: dict[str, int]) -> dict[str, Any]:
    prompt = tokens["prompt"]
    completion = tokens["completion"]
    cached = min(tokens["cached"], prompt) if prompt else tokens["cached"]
    billable_input = max(prompt - cached, 0)

    threshold = price.get("long_context_consider_token_greater_than")
    long_context = bool(threshold) and prompt > int(threshold)

    if long_context:
        in_rate = _to_decimal(price.get("long_context_input_per_million"))
        cached_rate = _to_decimal(price.get("long_context_cached_input_per_million"))
        out_rate = _to_decimal(price.get("long_context_output_per_million"))
    else:
        in_rate = _to_decimal(price.get("input_token_cost_per_million"))
        cached_rate = _to_decimal(price.get("cached_input_token_cost_per_million"))
        out_rate = _to_decimal(price.get("output_token_cost_per_million"))

    input_cost = (Decimal(billable_input) * in_rate) / _MILLION
    cached_cost = (Decimal(cached) * cached_rate) / _MILLION
    output_cost = (Decimal(completion) * out_rate) / _MILLION
    total = input_cost + cached_cost + output_cost

    return {
        "input_tokens": billable_input,
        "cached_input_tokens": cached,
        "output_tokens": completion,
        "input_cost": input_cost,
        "cached_input_cost": cached_cost,
        "output_cost": output_cost,
        "total_cost": total,
        "long_context": long_context,
    }


async def estimate_trace_cost(event: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Resolve provider/model/tokens from ``event`` and compute the cost.

    Returns ``None`` when the event is not a billable LLM call (missing
    provider, model, or token usage) or when no matching price row is found.
    """
    if not isinstance(event, dict):
        return None

    model = event.get("model")
    provider = _resolve_provider(event.get("integration"), model)
    if not provider or not model:
        return None

    tokens = _extract_tokens(event)
    if tokens["prompt"] == 0 and tokens["completion"] == 0:
        return None

    modality = event.get("modality") or "Text"
    price = await postgres_client.fetch_price(provider, str(model), modality)
    if price is None:
        logger.info(
            "[COST] No price row for provider=%s model=%s modality=%s",
            provider, model, modality,
        )
        return None

    breakdown = _compute(price, tokens)
    breakdown.update({
        "provider": provider,
        "model": str(model),
        "modality": modality,
        "currency": "USD",
    })
    return breakdown
