"""Chapter content classification and cross-source alignment for aggregate processing."""

from __future__ import annotations

import re
import unicodedata
from bisect import bisect_left
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from itertools import combinations
from typing import Any

from app.services.text_convert import to_simplified


# ── thresholds (plan §7.4) ───────────────────────────────────────────────────

TITLE_SIMILARITY_THRESHOLD = 0.80
PREVIEW_SIMILARITY_THRESHOLD = 0.70
PREVIEW_HIGH_CONFIDENCE = 0.88
PREVIEW_MIN_LENGTH = 40
PREVIEW_PREFERRED_MIN = 80
PREVIEW_PREFERRED_MAX = 200
CANDIDATE_WINDOW_SIZE = 1000
FULL_CONTENT_MIN_LENGTH = 200
HEAD_PREVIEW_SIMILARITY_THRESHOLD = 0.35
RELAXED_PREVIEW_SIMILARITY_THRESHOLD = 0.50
RELAXED_HEAD_PREVIEW_SIMILARITY_THRESHOLD = 0.50
CROSS_SOURCE_CONTENT_SIMILARITY_THRESHOLD = 0.90
CROSS_SOURCE_COMPARE_MAX = 3000
SENTENCE_MATCH_THRESHOLD = 0.80
SENTENCE_MATCH_WINDOW = 3
SENTENCE_MIN_LENGTH = 6
SENTENCE_ORDER_MIN_MATCHES = 10
SENTENCE_ORDER_MIN_COVERAGE = 0.65
SENTENCE_ORDER_CONSISTENT_RATIO = 0.92
SENTENCE_ORDER_MISMATCH_RATIO = 0.80


# ── helpers ──────────────────────────────────────────────────────────────────


def _normalize_for_compare(text: str) -> str:
    """Strip whitespace, punctuation and line breaks for fuzzy comparison."""
    text = re.sub(r"\s+", "", text)
    text = re.sub(r"""[，。！？、；："'（）《》【】\.,!\?;:\(\)<>\[\]]""", "", text)
    return text


def _title_similarity(a: str, b: str) -> float:
    """Character-level similarity between two chapter titles."""
    na = _normalize_chapter_title(a)
    nb = _normalize_chapter_title(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb, autojunk=False).ratio()


def _sequence_similarity(a: str, b: str) -> float:
    """Character-level LCS-based similarity (code similarity)."""
    na = _normalize_for_compare(a)
    nb = _normalize_for_compare(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb, autojunk=False).ratio()


_CHINESE_DIGITS = {
    "零": 0,
    "〇": 0,
    "一": 1,
    "二": 2,
    "两": 2,
    "三": 3,
    "四": 4,
    "五": 5,
    "六": 6,
    "七": 7,
    "八": 8,
    "九": 9,
}
_CHINESE_UNITS = {"十": 10, "百": 100, "千": 1000, "万": 10000}


def _parse_chinese_number(token: str) -> int | None:
    token = str(token or "").strip()
    if not token:
        return None
    if token.isdigit():
        return int(token)
    total = 0
    section = 0
    number = 0
    seen = False
    for char in token:
        if char in _CHINESE_DIGITS:
            number = _CHINESE_DIGITS[char]
            seen = True
            continue
        unit = _CHINESE_UNITS.get(char)
        if unit is None:
            return None
        seen = True
        if unit == 10000:
            section = (section + (number or 0)) * unit
            total += section
            section = 0
        else:
            section += (number or 1) * unit
        number = 0
    if not seen:
        return None
    return total + section + number


def _normalize_chapter_title(text: str) -> str:
    text = str(text or "")

    def replace_ordinal(match: re.Match[str]) -> str:
        number = _parse_chinese_number(match.group(1))
        return str(number) if number is not None else match.group(1)

    text = re.sub(r"第([零〇一二两三四五六七八九十百千万\d]+)([章节回卷篇部集])", replace_ordinal, text)
    text = text.replace("正文", "")
    return _normalize_for_compare(text)


def chapter_title_similarity(expected_title: str, candidate_title: str) -> float:
    return _title_similarity(expected_title, candidate_title)


_CHAPTER_ORDINAL_PREFIX = re.compile(
    r"^\s*第\s*[零〇一二两三四五六七八九十百千万\d]+\s*[章节回卷篇部集]\s*[:：、.．\-_·]*"
)


def chapter_name_similarity(expected_title: str, candidate_title: str) -> float:
    """Compare descriptive names when both titles have chapter ordinals."""
    expected = str(expected_title or "")
    candidate = str(candidate_title or "")
    if not _CHAPTER_ORDINAL_PREFIX.match(expected) or not _CHAPTER_ORDINAL_PREFIX.match(candidate):
        return 0.0
    expected_name = _CHAPTER_ORDINAL_PREFIX.sub("", expected).strip()
    candidate_name = _CHAPTER_ORDINAL_PREFIX.sub("", candidate).strip()
    if not expected_name or not candidate_name:
        return 0.0
    return _title_similarity(expected_name, candidate_name)


def _candidate_body_head(candidate_title: str, candidate_content: str) -> str:
    stripped = str(candidate_content or "").strip()
    if not stripped:
        return stripped
    lines = stripped.splitlines()
    if not lines:
        return stripped
    if _title_similarity(candidate_title, lines[0]) >= 0.95:
        return "\n".join(lines[1:]).strip()
    return stripped


def _sliding_preview_similarity(preview: str, candidate_content: str) -> float:
    """Sliding window match: find where *preview* best matches inside *candidate_content*.

    Searches the first ``CANDIDATE_WINDOW_SIZE`` characters of candidate_content.
    Returns the best similarity score found.
    """
    norm_preview = _normalize_for_compare(preview)
    if not norm_preview:
        return 0.0

    search_text = _normalize_for_compare(candidate_content[:CANDIDATE_WINDOW_SIZE])
    if not search_text:
        return 0.0

    # If candidate is shorter than preview, just compare directly.
    if len(search_text) <= len(norm_preview):
        return _sequence_similarity(norm_preview, search_text)

    best = 0.0
    step = max(1, len(norm_preview) // 4)
    for i in range(0, len(search_text) - len(norm_preview) + 1, step):
        window = search_text[i : i + len(norm_preview)]
        sim = SequenceMatcher(None, norm_preview, window, autojunk=False).ratio()
        if sim > best:
            best = sim
            if best >= 1.0:
                break
    # Also check the last window position.
    if len(search_text) > len(norm_preview):
        window = search_text[-len(norm_preview):]
        sim = SequenceMatcher(None, norm_preview, window, autojunk=False).ratio()
        best = max(best, sim)
    return best


def cross_source_content_similarity(left: str, right: str) -> float:
    """Compare candidate bodies while preserving sentence order."""
    return _sequence_similarity(
        str(left or "")[:CROSS_SOURCE_COMPARE_MAX],
        str(right or "")[:CROSS_SOURCE_COMPARE_MAX],
    )


def _sentence_fingerprint(value: str) -> str:
    simplified = str(to_simplified(unicodedata.normalize("NFKC", value)) or "")
    return re.sub(r"[\W_]+", "", simplified.casefold())


def _sentence_fingerprints(content: str) -> list[str]:
    """Split content for comparison without changing the stored body."""
    compact = re.sub(r"\s+", "", str(content or "").replace("\r", "\n"))
    fragments = re.findall(r".*?(?:[。！？!?；;]+[”’\"'」』】）)]*|$)", compact)
    sentences: list[str] = []
    pending = ""
    for fragment in fragments:
        if not fragment:
            continue
        pending += fragment
        fingerprint = _sentence_fingerprint(pending)
        if len(fingerprint) >= SENTENCE_MIN_LENGTH:
            sentences.append(fingerprint)
            pending = ""
    if pending:
        fingerprint = _sentence_fingerprint(pending)
        if sentences:
            sentences[-1] += fingerprint
        elif len(fingerprint) >= SENTENCE_MIN_LENGTH:
            sentences.append(fingerprint)
    return sentences


def _sentence_similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    shorter, longer = sorted((len(left), len(right)))
    if shorter * 3 < longer * 2:
        return 0.0
    return SequenceMatcher(None, left, right, autojunk=False).ratio()


def _lis_length(values: list[int]) -> int:
    tails: list[int] = []
    for value in values:
        index = bisect_left(tails, value)
        if index == len(tails):
            tails.append(value)
        else:
            tails[index] = value
    return len(tails)


def _map_sentence_positions(reference: list[str], peer: list[str]) -> dict[str, Any]:
    if not reference or not peer:
        return {
            "matchedCount": 0,
            "coverage": 0.0,
            "orderRatio": 0.0,
            "misplacedCount": 0,
            "backwardCount": 0,
        }

    reference_counts = Counter(reference)
    peer_counts = Counter(peer)
    exact_positions: dict[str, list[int]] = defaultdict(list)
    for index, fingerprint in enumerate(peer):
        exact_positions[fingerprint].append(index)

    used_peer_indexes: set[int] = set()
    mapped_positions: list[int] = []
    for reference_index, fingerprint in enumerate(reference):
        if reference_counts[fingerprint] > 1 or peer_counts[fingerprint] > 1:
            continue
        expected = round(reference_index * (len(peer) - 1) / max(1, len(reference) - 1))
        match_index: int | None = None
        if fingerprint in exact_positions:
            exact_index = exact_positions[fingerprint][0]
            if exact_index not in used_peer_indexes:
                match_index = exact_index
        else:
            lower = max(0, expected - SENTENCE_MATCH_WINDOW)
            upper = min(len(peer), expected + SENTENCE_MATCH_WINDOW + 1)
            search_indexes = [index for index in range(lower, upper) if index not in used_peer_indexes]
            best_score = 0.0
            for peer_index in search_indexes:
                score = _sentence_similarity(fingerprint, peer[peer_index])
                if score > best_score:
                    best_score = score
                    match_index = peer_index
            if best_score < SENTENCE_MATCH_THRESHOLD:
                match_index = None
                for peer_index, peer_fingerprint in enumerate(peer):
                    if peer_index in used_peer_indexes:
                        continue
                    score = _sentence_similarity(fingerprint, peer_fingerprint)
                    if score > best_score:
                        best_score = score
                        match_index = peer_index
                if best_score < SENTENCE_MATCH_THRESHOLD:
                    match_index = None
        if match_index is None:
            continue
        used_peer_indexes.add(match_index)
        mapped_positions.append(match_index)

    matched_count = len(mapped_positions)
    lis_length = _lis_length(mapped_positions)
    backward_count = 0
    highest_seen = -1
    for position in mapped_positions:
        if position < highest_seen:
            backward_count += 1
        else:
            highest_seen = position
    return {
        "matchedCount": matched_count,
        "coverage": round(matched_count / max(1, min(len(reference), len(peer))), 4),
        "orderRatio": round(lis_length / max(1, matched_count), 4),
        "misplacedCount": matched_count - lis_length,
        "backwardCount": backward_count,
    }


def _compare_sentence_order(left: list[str], right: list[str]) -> dict[str, Any]:
    forward = _map_sentence_positions(left, right)
    reverse = _map_sentence_positions(right, left)
    matched_count = min(forward["matchedCount"], reverse["matchedCount"])
    coverage = min(forward["coverage"], reverse["coverage"])
    order_ratio = min(forward["orderRatio"], reverse["orderRatio"])
    misplaced_count = max(forward["misplacedCount"], reverse["misplacedCount"])
    backward_count = max(forward["backwardCount"], reverse["backwardCount"])
    has_evidence = (
        matched_count >= SENTENCE_ORDER_MIN_MATCHES
        and coverage >= SENTENCE_ORDER_MIN_COVERAGE
    )
    return {
        "matchedCount": matched_count,
        "coverage": round(coverage, 4),
        "orderRatio": round(order_ratio, 4),
        "misplacedCount": misplaced_count,
        "backwardCount": backward_count,
        "consistent": bool(has_evidence and order_ratio >= SENTENCE_ORDER_CONSISTENT_RATIO),
        "orderMismatch": bool(
            has_evidence
            and order_ratio <= SENTENCE_ORDER_MISMATCH_RATIO
            and misplaced_count >= 3
            and backward_count >= 3
        ),
    }


def analyze_sentence_order_consensus(
    candidates: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Reject only an outlier that conflicts with two order-consistent references."""
    sources = [
        (str(candidate.get("source_id", "") or ""), _sentence_fingerprints(candidate.get("content", "")))
        for candidate in candidates
        if str(candidate.get("source_id", "") or "")
    ]
    comparisons: dict[frozenset[str], dict[str, Any]] = {}
    for (left_id, left), (right_id, right) in combinations(sources, 2):
        comparisons[frozenset((left_id, right_id))] = _compare_sentence_order(left, right)

    consistent_degrees = {source_id: 0 for source_id, _ in sources}
    for pair, comparison in comparisons.items():
        if comparison["consistent"]:
            for source_id in pair:
                consistent_degrees[source_id] += 1

    audits: dict[str, dict[str, Any]] = {}
    source_ids = [source_id for source_id, _ in sources]
    sentence_counts = {source_id: len(sentences) for source_id, sentences in sources}
    for source_id in source_ids:
        peer_ids = [peer_id for peer_id in source_ids if peer_id != source_id]
        reference_ids: list[str] = []
        if len(source_ids) >= 3:
            for left_id, right_id in combinations(peer_ids, 2):
                reference_pair = comparisons[frozenset((left_id, right_id))]
                left_comparison = comparisons[frozenset((source_id, left_id))]
                right_comparison = comparisons[frozenset((source_id, right_id))]
                if (
                    reference_pair["consistent"]
                    and left_comparison["orderMismatch"]
                    and right_comparison["orderMismatch"]
                    and consistent_degrees[source_id] < consistent_degrees[left_id]
                    and consistent_degrees[source_id] < consistent_degrees[right_id]
                ):
                    reference_ids = [left_id, right_id]
                    break

        rejected = bool(reference_ids)
        peer_comparisons = []
        for peer_id in peer_ids:
            comparison = comparisons[frozenset((source_id, peer_id))]
            peer_comparisons.append({
                "sourceId": peer_id,
                "matchedCount": comparison["matchedCount"],
                "coverage": comparison["coverage"],
                "orderRatio": comparison["orderRatio"],
                "misplacedCount": comparison["misplacedCount"],
                "backwardCount": comparison["backwardCount"],
                "relation": (
                    "consistent" if comparison["consistent"]
                    else "order_mismatch" if comparison["orderMismatch"]
                    else "insufficient_evidence"
                ),
            })
        audits[source_id] = {
            "status": "rejected_order_mismatch" if rejected else (
                "order_consistent" if consistent_degrees[source_id] else "insufficient_evidence"
            ),
            "rejected": rejected,
            "sentenceCount": sentence_counts[source_id],
            "consistentPeerCount": consistent_degrees[source_id],
            "referenceSourceIds": reference_ids,
            "comparisons": peer_comparisons,
        }
    return audits


# ── classification ───────────────────────────────────────────────────────────


def classify_source_content(
    content: str | None,
    source_id: str = "",
    is_official: bool = False,
    *,
    source_word_count: int = 0,
    preview_only_hint: bool = False,
    extra: dict[str, Any] | None = None,
    is_paid: bool = False,
    is_vip: bool = False,
) -> dict[str, Any]:
    """Classify fetched chapter content as ``full`` / ``preview`` / ``empty``.

    基于 is_vip + previewOnly 判断，而非硬编码字数阈值。
    Returns a dict with keys: classification, contentLength, previewText,
    isOfficial, reason.
    """
    if not content or not content.strip():
        return {
            "classification": "empty",
            "contentLength": 0,
            "previewText": "",
            "isOfficial": is_official,
            "sourceId": source_id,
            "reason": "no_content",
        }

    stripped = content.strip()
    length = len(stripped)
    preview_text = stripped[:PREVIEW_PREFERRED_MAX]
    extra = extra if isinstance(extra, dict) else {}

    explicit_preview = bool(preview_only_hint)
    for key in ("previewOnly", "isPreview", "preview_only", "isLocked"):
        if key in extra:
            explicit_preview = explicit_preview or bool(extra.get(key))
    if is_paid:
        explicit_preview = True

    try:
        source_word_count = int(source_word_count or 0)
    except (TypeError, ValueError):
        source_word_count = 0

    # The official TOC is authoritative for free chapters. Some Qidian App
    # fallback responses label short announcements as paid previews even when
    # the TOC marks them free; treating that transport hint as stronger than
    # TOC metadata can permanently block publication from chapter one.
    known_official_free = source_id == "qidian_com_app" and is_official and not is_vip

    # 1. 显式预览信号 → preview
    if explicit_preview and not known_official_free:
        return {
            "classification": "preview",
            "contentLength": length,
            "previewText": preview_text,
            "isOfficial": is_official,
            "sourceId": source_id,
            "reason": "explicit_preview_signal",
            "sourceWordCount": source_word_count,
        }

    # 2. VIP 章且未付费 → preview（官方 VIP 预览）
    if is_vip and not is_paid:
        return {
            "classification": "preview",
            "contentLength": length,
            "previewText": preview_text,
            "isOfficial": is_official,
            "sourceId": source_id,
            "reason": "vip_chapter_not_paid",
            "sourceWordCount": source_word_count,
        }

    # 3. 内容明显短于官方字数 → preview
    if source_word_count > 0 and length > 0 and length + 80 < source_word_count:
        return {
            "classification": "preview",
            "contentLength": length,
            "previewText": preview_text,
            "isOfficial": is_official,
            "sourceId": source_id,
            "reason": "visible_content_shorter_than_source_word_count",
            "sourceWordCount": source_word_count,
        }

    # 4. 极短内容且无任何元数据 → preview（安全网，<40 字几乎不可能是完整章节）
    if length < PREVIEW_MIN_LENGTH and not source_word_count and not known_official_free:
        return {
            "classification": "preview",
            "contentLength": length,
            "previewText": preview_text,
            "isOfficial": is_official,
            "sourceId": source_id,
            "reason": "content_too_short_no_metadata",
            "sourceWordCount": source_word_count,
        }

    # 5. 有内容 → full（免费章、已付费 VIP、第三方补全的完整正文）
    return {
        "classification": "full",
        "contentLength": length,
        "previewText": preview_text,
        "isOfficial": is_official,
        "sourceId": source_id,
        "reason": "content_present",
        "sourceWordCount": source_word_count,
    }


# ── alignment ────────────────────────────────────────────────────────────────


def align_candidate_chapter(
    *,
    official_preview: str,
    candidate_title: str,
    candidate_content: str,
    expected_title: str,
) -> dict[str, Any]:
    """Check whether a candidate chapter matches the official preview.

    Uses title similarity + sliding-window preview similarity per plan §7.4.

    Returns a dict with: alignmentPassed, titleSimilarity, previewSimilarity,
    alignmentReason.
    """
    if not official_preview or len(official_preview.strip()) < PREVIEW_MIN_LENGTH:
        return {
            "alignmentPassed": False,
            "titleSimilarity": 0.0,
            "previewSimilarity": 0.0,
            "headPreviewSimilarity": 0.0,
            "alignmentReason": "no_preview_available",
        }

    title_sim = max(
        _title_similarity(expected_title, candidate_title),
        chapter_name_similarity(expected_title, candidate_title),
    )
    preview_sim = _sliding_preview_similarity(official_preview, candidate_content)
    head_sim = _sequence_similarity(
        official_preview[:PREVIEW_PREFERRED_MAX],
        _candidate_body_head(candidate_title, candidate_content)[:PREVIEW_PREFERRED_MAX],
    )

    # High-confidence preview bypasses title check.
    if preview_sim >= PREVIEW_HIGH_CONFIDENCE and head_sim >= HEAD_PREVIEW_SIMILARITY_THRESHOLD:
        return {
            "alignmentPassed": True,
            "titleSimilarity": round(title_sim, 4),
            "previewSimilarity": round(preview_sim, 4),
            "headPreviewSimilarity": round(head_sim, 4),
            "alignmentReason": "preview_high_confidence",
        }

    # Standard check: both must pass thresholds.
    if (
        title_sim >= TITLE_SIMILARITY_THRESHOLD
        and preview_sim >= PREVIEW_SIMILARITY_THRESHOLD
        and head_sim >= HEAD_PREVIEW_SIMILARITY_THRESHOLD
    ):
        return {
            "alignmentPassed": True,
            "titleSimilarity": round(title_sim, 4),
            "previewSimilarity": round(preview_sim, 4),
            "headPreviewSimilarity": round(head_sim, 4),
            "alignmentReason": "title_and_preview_matched",
        }

    # Relaxed path: exact-ish title plus a clear body-head match.
    if (
        title_sim >= 0.95
        and preview_sim >= RELAXED_PREVIEW_SIMILARITY_THRESHOLD
        and head_sim >= RELAXED_HEAD_PREVIEW_SIMILARITY_THRESHOLD
    ):
        return {
            "alignmentPassed": True,
            "titleSimilarity": round(title_sim, 4),
            "previewSimilarity": round(preview_sim, 4),
            "headPreviewSimilarity": round(head_sim, 4),
            "alignmentReason": "title_and_head_matched",
        }

    reason_parts = []
    if title_sim < TITLE_SIMILARITY_THRESHOLD:
        reason_parts.append("title_low")
    if preview_sim < PREVIEW_SIMILARITY_THRESHOLD:
        reason_parts.append("preview_low")
    if head_sim < HEAD_PREVIEW_SIMILARITY_THRESHOLD:
        reason_parts.append("head_low")
    return {
        "alignmentPassed": False,
        "titleSimilarity": round(title_sim, 4),
        "previewSimilarity": round(preview_sim, 4),
        "headPreviewSimilarity": round(head_sim, 4),
        "alignmentReason": "+".join(reason_parts) or "alignment_failed",
    }


# ── alignment JSON builder ───────────────────────────────────────────────────


def build_source_alignment_json(
    *,
    selected_content_source: str,
    official_content_length: int = 0,
    candidate_content_length: int = 0,
    title_similarity: float = 0.0,
    preview_similarity: float = 0.0,
    head_preview_similarity: float = 0.0,
    alignment_passed: bool = False,
    alignment_reason: str = "",
    candidate_source_id: str = "",
    primary_source_id: str = "",
) -> dict[str, Any]:
    """Build the JSON structure stored in ``source_alignment_json`` column."""
    return {
        "selectedContentSource": selected_content_source,
        "primarySourceId": primary_source_id,
        "officialContentLength": official_content_length,
        "candidateSourceId": candidate_source_id,
        "candidateContentLength": candidate_content_length,
        "titleSimilarity": round(title_similarity, 4),
        "previewSimilarity": round(preview_similarity, 4),
        "headPreviewSimilarity": round(head_preview_similarity, 4),
        "alignmentPassed": alignment_passed,
        "alignmentReason": alignment_reason,
    }


# ── deviation score (plan §10) ───────────────────────────────────────────────


def compute_deviation_score(original: str, ai_output: str, ai_self_score: float | None = None) -> float:
    """Compute deviation score between original content and AI output.

    Score range: 0.0 (completely different) to 1.0 (identical).
    Uses character-level LCS similarity after normalization, blended with
    the AI self-rating when available.

    Per plan §10.1.1: code_similarity * 0.7 + AI self-rating * 0.3.
    When ai_self_score is None, fall back to code_similarity only for
    backwards compatibility.
    """
    norm_orig = _normalize_for_compare(original)
    norm_out = _normalize_for_compare(ai_output)
    if not norm_orig or not norm_out:
        code_similarity = 0.0
    else:
        code_similarity = SequenceMatcher(None, norm_orig, norm_out, autojunk=False).ratio()

    if ai_self_score is None:
        return code_similarity
    return code_similarity * 0.7 + ai_self_score * 0.3
