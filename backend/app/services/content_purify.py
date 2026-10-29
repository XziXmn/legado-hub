"""Chapter content purification (ads / watermarks).

Write path (aggregate processor) and read path (Reading / Legado chapter API)
share the same pure-function rules so already-stored chapters and third-party
direct reads get a second gate without reprocessing the library.
"""

from __future__ import annotations

import logging
import re
from difflib import SequenceMatcher
from typing import Iterable

logger = logging.getLogger(__name__)

CHAPTER_TITLE_RE = re.compile(r"^(#\s*)?第[一二三四五六七八九十百零\d]+章.*$")

# 6 / 9 separator variants: = ＝ . 。 ． and optional junk (+ ＋ or rare glyphs like ꁘ).
_SIX_NINE = r"6\s*[=＝.。．]\s*9"
_BOOK_BA = r"[书書][_＿\s'′]*吧"

# Confusable separators that some jumbled watermarks insert between every
# character (often doubled), e.g. 最⊥新⊥小⊥说⊥在⊥六⊥9⊥⊥书⊥⊥吧⊥⊥首⊥发！
_CONFUSABLE_GAP = r"[\s⊥⋚⋛⋯…·•※◦∘∙⋅~～_\-—]{0,3}"


def _glyph_spaced(phrase: str) -> str:
    """Allow optional confusable separators between every character of *phrase*."""
    return _CONFUSABLE_GAP.join(phrase)


# 最⊥新⊥小⊥说⊥在⊥六⊥9⊥⊥书⊥⊥吧⊥⊥首⊥发！
_LATEST_69_AD = _glyph_spaced("最新小说在六9书吧首发")

# Inline / residual watermarks (full phrases first, then 69 fragments like bare 6=9+).
INLINE_AD_RES: tuple[re.Pattern[str], ...] = (
    re.compile(
        rf"无错版本在读[！!]?\s*{_SIX_NINE}\s*[+＋]?\s*{_BOOK_BA}\s*首发本小说[。．.]?"
    ),
    re.compile(r"无错版本在读[！!]?"),
    re.compile(rf"{_SIX_NINE}\s*[+＋]?\s*{_BOOK_BA}\s*首发本小说[。．.]?"),
    # 更多最新热门小说在6.9ꁘ書吧看！
    re.compile(
        rf"更多最新热门小说在\s*{_SIX_NINE}\s*.{{0,4}}{_BOOK_BA}\s*看[！!]?"
    ),
    re.compile(rf"最新热门小说在\s*{_SIX_NINE}\s*.{{0,4}}{_BOOK_BA}\s*看[！!]?"),
    re.compile(r"更多最新热门小说在"),
    # 正确内（容在%六九%书'吧读！{
    re.compile(
        r"正确内[（(]?\s*容在\s*[%％]?\s*六九\s*[%％]?\s*书[''′]?\s*吧\s*读[！!]?\s*[\{｛]?"
    ),
    re.compile(r"正确内[（(]?\s*容在"),
    re.compile(r"[%％]\s*六九\s*[%％]\s*书[''′]?\s*吧"),
    # Residual fragments after partial strip (screenshot: lone line 6=9+)
    re.compile(rf"(?m)^\s*{_SIX_NINE}\s*[+＋]?\s*$"),
    re.compile(rf"{_SIX_NINE}\s*[+＋]"),
    re.compile(rf"{_SIX_NINE}\s*.{{0,4}}{_BOOK_BA}"),
    re.compile(r"(?m)^\s*[%％]\s*六九\s*[%％]\s*$"),
    re.compile(r"(?m)^\s*[书書][_＿'′]?\s*吧\s*$"),
    re.compile(r"[%％]\s*六九\s*[%％]"),
    # 最⊥新⊥小⊥说⊥在⊥六⊥9⊥⊥书⊥⊥吧⊥⊥首⊥发！
    re.compile(_LATEST_69_AD + r"[。．.!！]?"),
)

GLOBAL_AD_PATTERNS: list[str] = [
    r"最新网址|最新地址|最新域名|最新章节地址",
    r"本章未完|本章未完.*点击下一页",
    r"请收藏|请记住本书首发域名",
    r"手机用户请浏览|手机阅读",
    r"百度搜索|百度搜",
    r"天才一秒|笔趣阁",
    r"一秒记住.*\.com|一秒记住.*\.net",
    r"亲,.*收藏|加入书签",
    r"www\.|\.com|\.net|\.org",
    # 69 书吧类混淆水印：无错版本在读！6=9+书_吧首发本小说。
    r"无错版本在读",
    r"首发本小说",
    r"更多最新热门小说在",
    r"热门小说在\s*6",
    rf"{_SIX_NINE}\s*[+＋]?\s*{_BOOK_BA}",
    rf"{_SIX_NINE}\s*.{{0,4}}{_BOOK_BA}",
    r"新?69\s*[书書]\s*吧",
    r"阅读sto55|爱75奇书屋",
    # 正确内（容在%六九%书'吧读！{
    r"正确内.?容在",
    r"[%％]\s*六九\s*[%％]",
    r"[书書][''′]?\s*吧\s*(?:读|看)",
    r"记住首发网站域名",
    r"本书由.{0,40}全网首发",
    r"[TtＴｔТт](?:\s*[TtＴｔТт])?\s*[KkＫｋΚκ]\s*[AaＡａĀā]?\s*[NnＮｎ]",
    r"^\s*</?(?:ins|div)>\s*$",
    # Residuals: bare 6=9+ / 6.9 (line containing this fragment is dropped)
    rf"{_SIX_NINE}\s*[+＋]",
    # 最⊥新⊥小⊥说⊥在⊥六⊥9⊥⊥书⊥⊥吧⊥⊥首⊥发！(whole-line fallback)
    _LATEST_69_AD + r"[。．.!！]?",
]


def merge_ad_patterns(*groups: Iterable[str] | None) -> list[str]:
    """Dedupe pattern strings while preserving first-seen order."""
    combined: list[str] = []
    seen: set[str] = set()
    for group in groups:
        if not group:
            continue
        for pat in group:
            if not pat or pat in seen:
                continue
            seen.add(pat)
            combined.append(pat)
    return combined


def compile_ad_patterns(patterns: list[str]) -> re.Pattern[str]:
    """Compile regex fragments into a multiline whole-line matcher."""
    if not patterns:
        return re.compile(r"(?!.*)", re.MULTILINE)
    escaped = [f"(?:{p})" for p in patterns]
    return re.compile(r"(?im)^.*(?:" + "|".join(escaped) + r").*$")


def strip_inline_ads(text: str) -> str:
    for pattern in INLINE_AD_RES:
        text = pattern.sub("", text)
    return text


def purify_content(
    content: str,
    *,
    ad_patterns: list[str] | None = None,
    chapter_title: str = "",
) -> str:
    """Strip ads/watermarks and normalize whitespace.

    Steps:
    1. Basic cleanup (line endings / blank lines).
    2. Inline watermark strip, then whole-line ad patterns.
    3. Drop duplicate chapter-title lines.
    4. Integrity: if cleaned text is too short or shrinks >50%, keep basic cleanup only.
    """
    if not content:
        return content

    original_length = len(content)

    text = content.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    basic_text = text.strip()

    patterns = merge_ad_patterns(ad_patterns, GLOBAL_AD_PATTERNS)
    text = strip_inline_ads(basic_text)
    text = compile_ad_patterns(patterns).sub("", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    if chapter_title:
        normalized_title = re.sub(r"\s+", "", chapter_title.lstrip("#").strip())
        lines = text.splitlines()
        for index, line in enumerate(lines[:3]):
            normalized_line = re.sub(r"\s+", "", line.lstrip("#").strip())
            if normalized_title and normalized_line == normalized_title:
                lines.pop(index)
                break
        text = "\n".join(lines)
    else:
        seen_titles: set[str] = set()
        kept_lines: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            title_key = stripped.lstrip("#").strip()
            if CHAPTER_TITLE_RE.match(stripped) and title_key in seen_titles:
                continue
            seen_titles.add(title_key)
            kept_lines.append(line)
        text = "\n".join(kept_lines)

    cleaned_length = len(text)
    if cleaned_length < 50 or (original_length > 100 and cleaned_length < original_length * 0.5):
        return basic_text
    return text.strip()


def purify_content_with_audit(
    content: str,
    *,
    ad_patterns: list[str] | None = None,
    chapter_title: str = "",
) -> tuple[str, list[dict[str, object]]]:
    """Apply the existing regex cleaner and retain a bounded, replayable diff."""
    cleaned = purify_content(
        content,
        ad_patterns=ad_patterns,
        chapter_title=chapter_title,
    )
    if cleaned == content:
        return cleaned, []

    audit: list[dict[str, object]] = []
    for opcode, start, end, _replacement_start, _replacement_end in SequenceMatcher(
        None, content, cleaned, autojunk=False
    ).get_opcodes():
        if opcode not in {"delete", "replace"} or start == end:
            continue
        audit.append({
            "rule": "content_purify",
            "ruleVersion": "v1",
            "position": start,
            "removedText": content[start:end],
        })
    return cleaned, audit


def load_plugin_ad_patterns(source_id: str | None = None) -> list[str]:
    """Best-effort plugin ad patterns for a source; empty when unavailable."""
    if not source_id:
        return []
    try:
        from app.source_plugins.scheduler import get_plugin_scheduler

        scheduler = get_plugin_scheduler()
        plugin = scheduler._plugins.get(source_id)
        if plugin is None:
            return []
        meta_patterns = list(plugin.metadata.ad_patterns or [])
        source_patterns: list[str] = []
        if hasattr(plugin.source, "get_ad_patterns") and callable(
            getattr(plugin.source, "get_ad_patterns")
        ):
            try:
                result = plugin.source.get_ad_patterns()
                if isinstance(result, list):
                    source_patterns = [str(p) for p in result if isinstance(p, str)]
            except Exception:
                logger.debug("get_ad_patterns failed for %s", source_id, exc_info=True)
        return merge_ad_patterns(source_patterns, meta_patterns)
    except Exception:
        logger.debug("plugin ad patterns unavailable for %s", source_id, exc_info=True)
        return []


def purify_for_reading(content: str, *, source_id: str | None = None) -> str:
    """Read-path gate: global rules + optional plugin patterns. No AI / lexicon."""
    if not content or not content.strip():
        return content
    try:
        return purify_content(
            content,
            ad_patterns=load_plugin_ad_patterns(source_id),
        )
    except Exception:
        logger.debug("reading purify failed for source=%s", source_id, exc_info=True)
        return content
