"""Free bounded lexical clues, never buyer identity, verified demand or sales."""

from __future__ import annotations

import unicodedata

from .contracts import Comment

RULES = {
    "purchase_question_candidate": ("where can i buy", "where to buy", "ou acheter", "quel site"),
    "price_question_candidate": ("how much", "what price", "quel prix", "combien"),
    "delivery_question_candidate": ("shipping", "delivery", "livraison", "livrez"),
    "negative_experience_candidate": ("scam", "broken", "broke after", "arnaque", "casse"),
}


def describe_comments(comments: tuple[Comment, ...]) -> dict:
    counts = dict.fromkeys(RULES, 0)
    quotes: dict[str, list[dict]] = {key: [] for key in RULES}
    for comment in comments:
        text = "".join(
            c
            for c in unicodedata.normalize("NFKD", comment.text.casefold())
            if not unicodedata.combining(c)
        )
        for label, phrases in RULES.items():
            if any(phrase in text for phrase in phrases):
                counts[label] += 1
                if len(quotes[label]) < 5:
                    quotes[label].append(
                        {"comment_id": comment.comment_id, "text": comment.text[:160]}
                    )
    return {
        "method": "comment-phrase-candidates-en-fr-v1",
        "counts": counts,
        "quotes": quotes,
        "interpretation_validated": False,
        "independent_buyers": None,
        "measured_purchases": None,
        "scope": "captured_comment_sample_only",
    }
