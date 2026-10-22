# Product identity and evidence

Status: proposed.

## Problem Statement

The same product appears in many videos and seller offers, while similar-looking products can be different. Incorrect grouping can manufacture a trend and contaminate historical evaluation.

## Solution

Maintain a reviewed product catalog with evidence-backed video and offer links, confidence, product families, and availability history.

## User Stories

1. As an operator, I want multiple offers linked to a product, so that evidence is not fragmented by seller.
2. As an operator, I want uncertain matches explained, so that I can approve or reject them.
3. As an operator, I want product families distinct from exact matches, so that substitutes are not treated as identical items.
4. As an operator, I want merges and splits reversible, so that catalog errors can be corrected.
5. As an operator, I want prior links reconstructible, so that backtests use only knowledge available then.
6. As an operator, I want repeated creatives identified, so that apparent independent spread is not exaggerated.

## Implementation Decisions

- Prefer explicit source product identifiers, documented product tags, and offer links where available. Text similarity proposes candidates; it does not establish identity automatically.
- Store evidence references, confidence, matching method/version, review decision, effective period when known, and availability time for every link. Preserve rejected hypotheses and supersession history.
- Support manual linking first. Add bounded, deterministic text candidate generation before considering paid embeddings, image downloads, or language-model calls.
- Product-family membership is versioned separately from exact product identity. Ambiguous links are excluded from high-confidence aggregation or reported as a separate sensitivity scenario.
- Creative fingerprints indicate likely duplication only; creator independence and common ownership are unknown unless supported by evidence.

## Testing Decisions

Through the catalog review boundary, link two videos to a product, reject a similar item, merge then split a mistaken grouping, and inspect the assessment at earlier availability times. Verify no future link leaks into historical aggregates. Use a small labeled corpus to measure candidate precision and review load rather than tests that repeat matching implementation. No existing approved tests exist.

## Out of Scope

Guaranteed universal product recognition, unreviewed identity claims from visual resemblance, or mandatory media retention.

## Further Notes

Done means identity decisions are inspectable, reversible, and reconstructible at past decision times. Product and product-family identifiers are internal stable identifiers, not source-specific listing identifiers.
