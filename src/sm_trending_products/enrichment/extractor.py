"""Free, conservative phrase labels over supplied text; no audio or model download."""

from __future__ import annotations

import re
import unicodedata

from .contracts import InsightOutput, ProviderSpec, SegmentLabel, TextSegment

PHRASES: dict[SegmentLabel, tuple[str, ...]] = {
    "problem": ("problem", "struggling", "pain", "tired of", "probleme", "fatigue", "marre de"),
    "benefit": ("helps", "save time", "easier", "permet", "gagner du temps", "plus facile"),
    "claimed_proof": (
        "tested",
        "results",
        "before and after",
        "teste",
        "resultat",
        "avant et apres",
    ),
    "objection": ("expensive", "doesn't work", "too much", "trop cher", "ne marche pas"),
    "call_to_action": ("buy", "shop", "link in", "order", "acheter", "commander", "lien dans"),
    "offer": ("discount", "free shipping", "sale", "reduction", "livraison offerte", "promo"),
}


class LocalTranscriptAdapter:
    spec = ProviderSpec(provider_id="local-rules", version="quoted-phrases-v1", enabled=True)

    def analyze(self, transcript: dict) -> InsightOutput:
        text = transcript["text"]
        segments: list[TextSegment] = []
        for match in re.finditer(r"[^.!?\n]+[.!?]?", text):
            start = match.start() + len(match[0]) - len(match[0].lstrip())
            end = min(match.end(), start + 500)
            if start >= end:
                continue
            value = "".join(
                char
                for char in unicodedata.normalize("NFKD", text[start:end].casefold())
                if not unicodedata.combining(char)
            )
            labels: list[SegmentLabel] = ["hook"] if not segments else []
            labels.extend(
                label
                for label, phrases in PHRASES.items()
                if any(re.search(r"\b" + re.escape(phrase) + r"\b", value) for phrase in phrases)
            )
            for label in labels:
                segments.append(TextSegment(start=start, end=end, label=label))
                if len(segments) == 24:
                    return InsightOutput(method="lexical", segments=tuple(segments))
        return InsightOutput(method="lexical", segments=tuple(segments))
