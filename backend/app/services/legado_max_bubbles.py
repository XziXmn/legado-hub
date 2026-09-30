"""Embed clickable review bubbles in content for Legado Max readers."""

from __future__ import annotations

import base64
import html
import json
import re
import unicodedata
from html.parser import HTMLParser
from urllib.parse import quote, urlencode
from xml.etree import ElementTree

from bs4 import BeautifulSoup


_BLOCK_TAGS = {"p", "div", "section", "article", "li"}
_EMOTICON_RE = re.compile(r"\[fn=\d+\]")


def _bubble(count: int, url: str, *, emphasis: bool = False) -> str:
    label = str(count) if count < 1000 else "999+"
    # Keep the trailing image-option JSON out of Max's num and status values.
    src = f"bubble://paragraph?num={quote(label)}&status={'emphasis' if emphasis else 'normal'}&v=1"
    click = f"java.showBrowser({json.dumps(url, ensure_ascii=False)})"
    options = json.dumps({"style": "TEXT", "click": click}, ensure_ascii=False, separators=(",", ":"))
    return f'<img src="{src},{options}">'


def _chapter_previews(reviews: dict) -> list[str]:
    previews: list[str] = []
    seen: set[str] = set()
    for item in (reviews.get("chapterEndHot") or []) + (reviews.get("chapterEnd") or []):
        if not isinstance(item, dict):
            continue
        content = BeautifulSoup(str(item.get("content") or item.get("Content") or "")[:4000], "html.parser")
        value = " ".join(_EMOTICON_RE.sub("", content.get_text(" ", strip=True)).split())
        if not value:
            continue
        user = str(item.get("userName") or item.get("UserName") or item.get("nickName") or "").strip()
        preview = f"{user[:20]}：{value}" if user else value
        key = str(item.get("id") or item.get("reviewId") or preview)
        if key in seen:
            continue
        seen.add(key)
        previews.append(preview[:180])
        if len(previews) == 3:
            break
    return previews


def _wrap_preview(text: str, *, width: int = 46) -> list[str]:
    lines: list[str] = []
    current = ""
    used = 0
    for char in text:
        size = 2 if unicodedata.east_asian_width(char) in ("F", "W") else 1
        if used + size > width:
            lines.append(current)
            current, used = "", 0
            if len(lines) == 2:
                lines[-1] = lines[-1][:-1] + "…"
                return lines
        current += char
        used += size
    if current:
        lines.append(current)
    return lines


def _chapter_card(count: int, reviews: dict, url: str) -> str:
    preview_lines = [_wrap_preview(preview) for preview in _chapter_previews(reviews)]
    height = 112 + sum(len(lines) * 35 + 18 for lines in preview_lines)
    root = ElementTree.Element("svg", {
        "xmlns": "http://www.w3.org/2000/svg", "width": "720", "height": str(height),
        "viewBox": f"0 0 720 {height}",
    })
    ElementTree.SubElement(root, "rect", {
        "x": "2", "y": "2", "width": "716", "height": str(height - 4),
        "rx": "8", "fill": "#293237", "stroke": "#637477", "stroke-width": "2",
    })
    ElementTree.SubElement(root, "rect", {
        "x": "32", "y": "33", "width": "6", "height": "37", "fill": "#e29a70",
    })
    heading = ElementTree.SubElement(root, "text", {
        "x": "55", "y": "64", "fill": "#f5f6f4", "font-size": "34", "font-family": "sans-serif",
    })
    heading.text = "本章说"
    count_text = ElementTree.SubElement(root, "text", {
        "x": "681", "y": "62", "fill": "#bdd0d0", "font-size": "25",
        "font-family": "sans-serif", "text-anchor": "end",
    })
    count_text.text = f"{count} 条评论  >"
    ElementTree.SubElement(root, "line", {
        "x1": "32", "y1": "88", "x2": "688", "y2": "88",
        "stroke": "#637477", "stroke-width": "2",
    })
    y = 126
    for lines in preview_lines:
        for line in lines:
            text = ElementTree.SubElement(root, "text", {
                "x": "34", "y": str(y), "fill": "#e0e6e3", "font-size": "27",
                "font-family": "sans-serif",
            })
            text.text = line
            y += 35
        y += 18
    encoded = base64.b64encode(ElementTree.tostring(root, encoding="utf-8")).decode("ascii")
    click = f"java.showBrowser({json.dumps(url, ensure_ascii=False)})"
    options = json.dumps({"style": "FULL", "click": click}, ensure_ascii=False, separators=(",", ":"))
    return f'<img src="data:image/svg+xml;base64,{encoded},{options}">'


class _BubbleInserter(HTMLParser):
    def __init__(self, markers: dict[int, list[str]]) -> None:
        super().__init__(convert_charrefs=False)
        self.markers = markers
        self.output: list[str] = []
        self.fragment: list[str] = []
        self.plain: list[str] = []
        self.index = 0
        self.first_paragraph = True

    def _flush(self) -> None:
        raw = "".join(self.fragment)
        plain = html.unescape("".join(self.plain)).strip()
        if plain:
            if self.first_paragraph and plain.startswith("# "):
                self.first_paragraph = False
            else:
                self.output.append(raw)
                self.output.extend(self.markers.get(self.index, ()))
                self.index += 1
                self.first_paragraph = False
                self.fragment.clear()
                self.plain.clear()
                return
        self.output.append(raw)
        self.fragment.clear()
        self.plain.clear()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _BLOCK_TAGS or tag == "br":
            self._flush()
        self.fragment.append(self.get_starttag_text())

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _BLOCK_TAGS or tag == "br":
            self._flush()
        self.fragment.append(self.get_starttag_text())

    def handle_endtag(self, tag: str) -> None:
        if tag in _BLOCK_TAGS:
            self._flush()
        self.fragment.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        for part in re.split(r"(\n)", data):
            if part == "\n":
                self._flush()
                self.fragment.append(part)
            else:
                self.fragment.append(part)
                self.plain.append(part)

    def handle_entityref(self, name: str) -> None:
        self.fragment.append(f"&{name};")
        self.plain.append(html.unescape(f"&{name};"))

    def handle_charref(self, name: str) -> None:
        self.fragment.append(f"&#{name};")
        self.plain.append(html.unescape(f"&#{name};"))

    def handle_comment(self, data: str) -> None:
        self.fragment.append(f"<!--{data}-->")

    def finish(self) -> str:
        self._flush()
        return "".join(self.output)


def decorate_legado_max_content(content: str, reviews: dict, *, view_url: str) -> str:
    """Place a bubble after each matched paragraph, preserving chapter markup."""
    markers: dict[int, list[str]] = {}
    for item in reviews.get("hotParagraphReviews") or []:
        if not isinstance(item, dict) or not item.get("matchedText"):
            continue
        try:
            index = int(item["matchedParagraphIndex"])
            span = int(item.get("matchedParagraphCount") or 1)
            paragraph_id = int(item["paragraphId"])
            count = int(
                item.get("commentCount")
                or item.get("totalCommentCount")
                or item.get("hotCommentCount")
                or 0
            )
        except (KeyError, TypeError, ValueError):
            continue
        if index < 0 or span not in (1, 2) or paragraph_id < 0 or count <= 0:
            continue
        url = f"{view_url}?{urlencode({'tab': 'paragraph', 'paragraphId': paragraph_id})}"
        markers.setdefault(index + span - 1, []).append(_bubble(count, url))

    summary = reviews.get("summary") or {}
    try:
        chapter_count = int(summary.get("chapterEndCount") or 0)
    except (TypeError, ValueError, AttributeError):
        chapter_count = 0
    if chapter_count <= 0:
        chapter_count = max(len(reviews.get("chapterEnd") or []), len(reviews.get("chapterEndHot") or []))
    if not markers and chapter_count <= 0:
        return content

    normalized = str(content or "").replace("\r\n", "\n").replace("\r", "\n")
    parser = _BubbleInserter(markers)
    parser.feed(normalized)
    parser.close()
    result = parser.finish()
    if chapter_count > 0:
        result = result.rstrip() + "\n\n" + _chapter_card(chapter_count, reviews, view_url + "?tab=chapter")
    return result
