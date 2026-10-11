"""Reading-facing chapter review markup, cache, and HTML view rendering."""

from __future__ import annotations

import copy
import hashlib
import html
import re
import time
from collections import OrderedDict
from typing import Any
from urllib.parse import urlencode, urlparse

REVIEW_CACHE_TTL_SECONDS = 600
REVIEW_CACHE_MAX_ITEMS = 256
REVIEW_EMOTICON_CDN = "https://qdfepccdn.qidian.com/gtimg/app_emoji_new/newface_{}.png"
REVIEW_IMAGE_HOST_SUFFIXES = (
    "qidian.com",
    "qpic.cn",
    "myqcloud.com",
    "yuewen.com",
)
_INLINE_EMOTICON_RE = re.compile(r"\[fn=(\d+)\]")


class ChapterReviewCache:
    """Small process-local cache that keeps review failures off the body path."""

    def __init__(self) -> None:
        self._items: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()

    def get(self, chapter_id: str) -> dict[str, Any] | None:
        cached = self._items.get(chapter_id)
        if cached is None:
            return None
        created_at, payload = cached
        if time.monotonic() - created_at >= REVIEW_CACHE_TTL_SECONDS:
            self._items.pop(chapter_id, None)
            return None
        self._items.move_to_end(chapter_id)
        return copy.deepcopy(payload)

    def set(self, chapter_id: str, payload: dict[str, Any]) -> None:
        self._items[chapter_id] = (time.monotonic(), copy.deepcopy(payload))
        self._items.move_to_end(chapter_id)
        while len(self._items) > REVIEW_CACHE_MAX_ITEMS:
            self._items.popitem(last=False)


chapter_review_cache = ChapterReviewCache()


def _review_id(review: dict[str, Any]) -> str:
    return str(review.get("id") or review.get("reviewId") or "")


def _dedupe_reviews(*buckets: Any) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    for bucket in buckets:
        if not isinstance(bucket, list):
            continue
        for review in bucket:
            if not isinstance(review, dict):
                continue
            review_id = _review_id(review)
            if review_id and review_id in seen:
                continue
            result.append(review)
            if review_id:
                seen.add(review_id)
    return result


def _count_label(value: Any) -> str:
    try:
        number = max(0, int(value or 0))
    except (TypeError, ValueError):
        number = 0
    if number >= 10000:
        return f"{number / 10000:.1f}w"
    if number >= 1000:
        return f"{number / 1000:.1f}k"
    return str(number)


def _safe_review_image_url(value: Any) -> str:
    """Allow only confirmed HTTPS Qidian/Yuewen image CDN hosts."""
    candidate = str(value or "").strip()
    if candidate.startswith("//"):
        candidate = f"https:{candidate}"
    if not candidate or len(candidate) > 2048:
        return ""
    try:
        parsed = urlparse(candidate)
        host = (parsed.hostname or "").lower().rstrip(".")
        port = parsed.port
    except ValueError:
        return ""
    if parsed.scheme.lower() != "https" or not host or parsed.username or parsed.password:
        return ""
    if port not in (None, 443):
        return ""
    if not any(host == suffix or host.endswith(f".{suffix}") for suffix in REVIEW_IMAGE_HOST_SUFFIXES):
        return ""
    return candidate


def _review_avatar(review: dict[str, Any], *, author: bool = False, compact: bool = False) -> str:
    """Render a remote avatar/frame with an always-present initial fallback."""
    user_name = str(review.get("userName") or review.get("authorName") or "书友")
    initial = html.escape(user_name[:1] or "书")
    classes = ["comment-avatar"]
    if author or review.get("authorSay"):
        classes.append("author")
    if compact:
        classes.append("compact")
    avatar = _safe_review_image_url(review.get("avatar") or review.get("headImageUrl"))
    frame = _safe_review_image_url(review.get("avatarFrame"))
    photo = (
        f'<img class="avatar-photo" src="{html.escape(avatar, quote=True)}" alt="" '
        'loading="lazy" decoding="async" referrerpolicy="no-referrer" onerror="this.hidden=true">'
        if avatar
        else ""
    )
    frame_image = (
        f'<img class="avatar-frame" src="{html.escape(frame, quote=True)}" alt="" '
        'loading="lazy" decoding="async" referrerpolicy="no-referrer" onerror="this.hidden=true">'
        if frame
        else ""
    )
    return (
        f'<span class="{" ".join(classes)}" title="{html.escape(user_name, quote=True)}">'
        f'<span class="avatar-fallback">{initial}</span>{photo}{frame_image}</span>'
    )


def _official_title_image(title: dict[str, Any], *, class_name: str) -> str:
    """Render one complete title image; the artwork already contains its text."""
    image = _safe_review_image_url(title.get("image"))
    if not image:
        return ""
    name = str(title.get("name") or "用户头衔").strip()
    escaped_name = html.escape(name, quote=True)
    return (
        f'<span class="{class_name}" title="{escaped_name}">'
        f'<img src="{html.escape(image, quote=True)}" alt="{escaped_name}" loading="lazy" '
        'decoding="async" referrerpolicy="no-referrer" onerror="this.hidden=true"></span>'
    )


def _review_identity_tags(review: dict[str, Any], *, author: bool = False) -> str:
    """Render author, position, and de-duplicated title metadata."""
    tags: list[str] = []
    if author or review.get("authorSay"):
        tags.append('<span class="author-badge">作家</span>')

    raw_titles = review.get("titles") if isinstance(review.get("titles"), list) else []
    titles = [item for item in raw_titles if isinstance(item, dict)]
    if not titles:
        titles = [
            {"name": str(name)}
            for name in review.get("badges", [])
            if str(name).strip()
        ] if isinstance(review.get("badges"), list) else []

    position = str(review.get("position") or "").strip()
    seen_names: set[str] = set()
    if position:
        position_title = next(
            (title for title in titles if str(title.get("name") or "").strip() == position),
            {},
        )
        position_image = _official_title_image(position_title, class_name="position-title-image")
        tags.append(position_image or f'<span class="position-badge">{html.escape(position)}</span>')
        seen_names.add(position)

    for title in titles:
        name = str(title.get("name") or "").strip()
        if name and name in seen_names:
            continue
        image_html = _official_title_image(title, class_name="title-image")
        if image_html:
            tags.append(image_html)
        elif name:
            tags.append(f'<span class="title-badge">{html.escape(name)}</span>')
        else:
            continue
        if name:
            seen_names.add(name)
    return f'<span class="identity-tags">{"".join(tags)}</span>' if tags else ""


def _review_related_position(review: dict[str, Any]) -> str:
    """Render the replied-to user's official position image when available."""
    position = str(review.get("relatedPosition") or "").strip()
    if not position:
        return ""
    raw_titles = review.get("relatedTitles") if isinstance(review.get("relatedTitles"), list) else []
    position_title = next(
        (
            title
            for title in raw_titles
            if isinstance(title, dict) and str(title.get("name") or "").strip() == position
        ),
        {},
    )
    position_image = _official_title_image(position_title, class_name="related-position-image")
    return position_image or f'<span class="related-position">{html.escape(position)}</span>'


def _review_content(value: Any) -> str:
    """Escape review text and render Qidian's confirmed 1-64 icon set."""
    escaped = html.escape(str(value or ""))

    def render_emoticon(match: re.Match[str]) -> str:
        icon_id = int(match.group(1))
        if 1 <= icon_id <= 64:
            icon_url = REVIEW_EMOTICON_CDN.format(icon_id)
            return (
                f'<img class="comment-emoticon" src="{icon_url}" alt="表情" '
                f'title="起点表情 {icon_id}" loading="lazy" decoding="async" '
                'referrerpolicy="no-referrer">'
            )
        return (
            '<span class="comment-emoticon-fallback" '
            f'title="起点表情 {icon_id}">表情</span>'
        )

    return _INLINE_EMOTICON_RE.sub(
        render_emoticon,
        escaped,
    )


def _review_media(review: dict[str, Any]) -> str:
    """Render a confirmed image/GIF URL without proxying plugin data."""
    image_url = _safe_review_image_url(review.get("imageUrl"))
    preview_url = _safe_review_image_url(review.get("imagePreview"))
    media_url = image_url or preview_url
    if not media_url:
        return ""
    return (
        '<div class="comment-media-wrap">'
        f'<img class="comment-media" src="{html.escape(media_url, quote=True)}" alt="评论图片" '
        'loading="lazy" decoding="async" referrerpolicy="no-referrer" onerror="this.hidden=true">'
        '</div>'
    )


def _reply_row(reply: dict[str, Any], *, target_name: str) -> str:
    """Render one compact reply using the same rich identity contract."""
    reply_name = str(reply.get("userName") or "书友")
    reply_target = str(reply.get("replyToUserName") or target_name)
    target_position = _review_related_position(reply)
    content = _review_content(reply.get("content"))
    text = f'<p>{content}</p>' if content else ""
    reply_id = _review_id(reply) or hashlib.sha256(
        "\x1f".join(
            str(reply.get(field) or "")
            for field in (
                "userId",
                "userName",
                "replyToUserName",
                "content",
                "reviewTime",
                "createTime",
                "imageUrl",
            )
        ).encode("utf-8")
    ).hexdigest()
    reply_attr = f' data-review-id="{html.escape(reply_id, quote=True)}"'
    reply_time = str(reply.get("reviewTime") or reply.get("createTime") or "")
    like_label = _count_label(reply.get("likeNum"))
    return (
        f'<div class="reply-line"{reply_attr}>'
        + _review_avatar(reply, compact=True)
        + '<div class="reply-body"><div class="reply-heading">'
        f'<span class="reply-author">{html.escape(reply_name)}</span>'
        + _review_identity_tags(reply)
        + (
            f'<span class="reply-arrow">▶</span>'
            f'<span class="reply-target">{html.escape(reply_target)}</span>{target_position}'
            if reply_target and reply_target != reply_name
            else ""
        )
        + '</div>'
        + text
        + _review_media(reply)
        + '<div class="reply-meta">'
        + (f'<time>{html.escape(reply_time)}</time>' if reply_time else "")
        + '<span class="meta-reply-btn">回复</span>'
        + f'<span class="meta-like" title="点赞"><span class="meta-icon meta-icon-like" aria-hidden="true"></span>'
        + f'<span class="meta-count">{like_label}</span></span>'
        + '</div></div></div>'
    )


def _review_reply_context(review: dict[str, Any]) -> str:
    """Keep reply target context when a reply is rendered as its own card."""
    target_name = str(review.get("replyToUserName") or "").strip()
    if not target_name:
        return ""
    target_position = _review_related_position(review)
    return (
        '<div class="comment-reply-context"><span>回复</span>'
        f'<strong>{html.escape(target_name)}</strong>{target_position}</div>'
    )


def _review_card(
    review: dict[str, Any],
    *,
    author: bool = False,
    review_view_url: str = "",
    link_to_paragraph: bool = False,
    paragraph_text: str = "",
) -> str:
    user_name = str(review.get("userName") or "书友")
    content = str(review.get("content") or "")
    review_time = str(review.get("reviewTime") or review.get("createTime") or "")
    review_id = _review_id(review)
    try:
        paragraph_id = int(review.get("paragraphId", -1))
    except (TypeError, ValueError):
        paragraph_id = -1
    replies = review.get("replies") if isinstance(review.get("replies"), list) else []
    replies = [reply for reply in replies if isinstance(reply, dict)]
    reply_rows = []
    for reply in replies:
        reply_rows.append(_reply_row(reply, target_name=user_name))
    reply_stack = ""
    reply_total = max(len(reply_rows), int(review.get("replyCount") or 0))
    if reply_rows or reply_total > 0:
        toggle = ""
        reply_surface_id = f"reply-surface-{review_id}" if review_id else ""
        reply_surface_attr = (
            f' id="{html.escape(reply_surface_id, quote=True)}"'
            if reply_surface_id
            else ""
        )
        if len(reply_rows) > 1 or reply_total > len(reply_rows):
            open_label = f"展开{_count_label(reply_total)}条回复"
            reply_url_attr = ""
            if review_view_url and review_id:
                query_values: dict[str, Any] = {"tab": "paragraph", "rootReviewId": review_id}
                if paragraph_id >= -1:
                    query_values["paragraphId"] = paragraph_id
                reply_url = f"{review_view_url}?{urlencode(query_values)}"
                reply_url_attr = f' data-reply-url="{html.escape(reply_url, quote=True)}"'
            controls_attr = (
                f' aria-controls="{html.escape(reply_surface_id, quote=True)}"'
                if reply_surface_id
                else ""
            )
            toggle = (
                '<button class="reply-toggle" type="button" data-reply-toggle aria-expanded="false" '
                + reply_url_attr
                + controls_attr
                + f'data-open-label="{html.escape(open_label, quote=True)}" data-close-label="收起">'
                f'<span class="reply-toggle-label">{html.escape(open_label)}</span>'
                '<span class="reply-toggle-chevron" aria-hidden="true"></span></button>'
            )
        reply_stack = (
            f'<div class="reply-stack"><div class="reply-surface"{reply_surface_attr}>'
            + "".join(reply_rows)
            + '</div>'
            + toggle
            + '</div>'
        )

    # Page-hot / overview: whole card jumps to that paragraph's reviews (no extra link).
    paragraph_href = ""
    if review_view_url and link_to_paragraph and paragraph_id >= 0:
        query = urlencode({"tab": "paragraph", "paragraphId": paragraph_id})
        paragraph_href = f"{review_view_url}?{query}"

    content_html = _review_content(content)
    text_html = f'<p class="comment-text">{content_html}</p>' if content_html else ""
    paragraph_html = (
        f'<blockquote class="comment-paragraph">{html.escape(paragraph_text.strip())}</blockquote>'
        if paragraph_text.strip()
        else ""
    )
    review_attr = f' data-review-id="{html.escape(review_id, quote=True)}"' if review_id else ""
    item_classes = "comment-item"
    jump_attr = ""
    if paragraph_href:
        item_classes += " comment-item--jump"
        jump_attr = (
            f' data-paragraph-href="{html.escape(paragraph_href, quote=True)}"'
            ' role="link" tabindex="0" title="查看该段全部评论"'
        )
    return (
        f'<article class="{item_classes}"{review_attr}{jump_attr}>'
        + _review_avatar(review, author=author)
        + '<div class="comment-body">'
        + '<div class="comment-head">'
        + f'<div class="comment-identity"><strong>{html.escape(user_name)}</strong>'
        + _review_identity_tags(review, author=author)
        + '</div></div>'
        + _review_reply_context(review)
        + paragraph_html
        + text_html
        + _review_media(review)
        + '<div class="comment-meta">'
        + '<div class="comment-meta-left">'
        + (f'<time>{html.escape(review_time)}</time>' if review_time else "")
        + '<span class="meta-reply-btn">回复</span></div>'
        + '<div class="comment-meta-right">'
        + f'<span class="meta-like" title="点赞"><span class="meta-icon meta-icon-like" aria-hidden="true"></span>'
        + f'<span class="meta-count">{_count_label(review.get("likeNum"))}</span></span>'
        + f'<span class="meta-reply-count" title="回复"><span class="meta-icon meta-icon-reply" aria-hidden="true"></span>'
        + f'<span class="meta-count">{_count_label(review.get("replyCount"))}</span></span>'
        + '</div></div>'
        + reply_stack
        + '</div></article>'
    )


def _empty_state(message: str) -> str:
    return f'<p class="empty-state">{html.escape(message)}</p>'


def _folded_list(
    items: list[str],
    *,
    list_id: str,
    visible_count: int,
    empty_message: str,
    open_label: str = "展开更多",
    close_label: str = "收起",
    auto_load: bool = False,
) -> str:
    if not items and not auto_load:
        return _empty_state(empty_message)
    visible_count = max(1, visible_count)
    auto_list_attr = ' data-auto-load-list="true"' if auto_load else ""
    container = (
        f'<div class="fold-list" id="{html.escape(list_id, quote=True)}" data-fold-list{auto_list_attr} '
        f'data-visible-count="{visible_count}">'
        + "".join(items)
        + '</div>'
    )
    if not items:
        # Auto-paginated lists must keep their container on every page. The reader
        # pulls `#<list id>` out of each follow-up response and appends its
        # children, so swapping the container for the empty state turned a healthy
        # 200 (source said hasMore, then returned no rows) into a bogus
        # "网络错误，点击重试" in the sheet.
        return container + _empty_state(empty_message)
    toggle = ""
    if len(items) > visible_count:
        auto_toggle_attr = ' data-auto-fold-toggle="true"' if auto_load else ""
        toggle = (
            f'<button class="fold-toggle" type="button" data-fold-toggle="{html.escape(list_id, quote=True)}" '
            f'aria-controls="{html.escape(list_id, quote=True)}" aria-expanded="false" '
            f'{auto_toggle_attr} '
            f'data-open-label="{html.escape(open_label, quote=True)}" '
            f'data-close-label="{html.escape(close_label, quote=True)}">{html.escape(open_label)}</button>'
        )
    return container + toggle


def _pagination_nav(
    payload: dict[str, Any],
    *,
    review_view_url: str,
    query: dict[str, Any],
    cursor: bool = False,
    auto_load: bool = False,
    list_id: str = "",
) -> str:
    page = max(1, int(payload.get("page") or 1))
    page_size = max(1, int(payload.get("pageSize") or 10))
    has_more = bool(payload.get("hasMore"))
    links: list[str] = []
    if page > 1:
        previous = {**query, "page": page - 1, "pageSize": page_size}
        if cursor:
            previous["cursorId"] = 0
        links.append(
            f'<a data-previous-page href="{html.escape(review_view_url, quote=True)}?{urlencode(previous)}">上一页</a>'
        )
    if has_more:
        following = {
            **query,
            "page": int(payload.get("nextPage") or page + 1),
            "pageSize": page_size,
        }
        if cursor:
            following["cursorId"] = int(payload.get("nextCursorId") or 0)
        links.append(
            f'<a data-next-page href="{html.escape(review_view_url, quote=True)}?{urlencode(following)}">加载更多</a>'
        )
    if not links:
        return ""
    auto_attr = (
        f' data-auto-pagination="true" data-list-id="{html.escape(list_id, quote=True)}"'
        if auto_load
        else ""
    )
    return f'<nav class="pagination-row"{auto_attr} aria-label="评论分页">{"".join(links)}</nav>'


def _paragraph_groups(
    reviews: dict[str, Any],
    *,
    review_view_url: str,
    selected_paragraph_id: int | None,
    paragraph_detail: dict[str, Any] | None,
) -> str:
    groups = [
        item
        for item in reviews.get("hotParagraphReviews", [])
        if isinstance(item, dict) and (item.get("matchedText") or item.get("paragraphText"))
    ]
    if selected_paragraph_id is not None:
        groups = [item for item in groups if int(item.get("paragraphId", -1)) == selected_paragraph_id]
    if not groups:
        return _empty_state("本章暂无可定位的段评")

    rendered: list[str] = []
    for item in groups:
        paragraph_id = int(item.get("paragraphId", 0) or 0)
        detail_comments = []
        if selected_paragraph_id == paragraph_id and isinstance(paragraph_detail, dict):
            detail_comments = paragraph_detail.get("comments") or []
        comments = (
            detail_comments
            if selected_paragraph_id == paragraph_id and isinstance(paragraph_detail, dict)
            else item.get("topReviews") or []
        )
        comments = [review for review in comments if isinstance(review, dict)]
        is_detail = selected_paragraph_id == paragraph_id and isinstance(paragraph_detail, dict)
        query = urlencode({"tab": "paragraph", "paragraphId": paragraph_id})
        comment_html = _folded_list(
            [
                _review_card(
                    review,
                    review_view_url=review_view_url,
                    # Overview: tap a top comment to open that paragraph's full list.
                    link_to_paragraph=not is_detail,
                )
                for review in comments
            ],
            list_id=f"paragraph-comments-{paragraph_id}",
            visible_count=10 if is_detail else 2,
            empty_message="该段暂未返回评论",
            open_label="加载更多评论" if is_detail else "展开更多评论",
            close_label="收起评论",
            auto_load=is_detail,
        )
        quote_text = str(item.get("matchedText") or item.get("paragraphText") or "").strip()
        quote_jump = ""
        if not is_detail and review_view_url:
            quote_jump = (
                f' data-paragraph-href="{html.escape(review_view_url + "?" + query, quote=True)}"'
                ' role="link" tabindex="0" title="查看该段全部评论"'
            )
        rendered.append(
            f'<article class="paragraph-group" data-paragraph-id="{paragraph_id}">'
            f'<div class="paragraph-quote-block{" paragraph-quote-block--jump" if quote_jump else ""}"{quote_jump}>'
            '<div class="paragraph-quote-label">原文</div>'
            f'<p class="paragraph-quote">{html.escape(quote_text)}</p>'
            '</div>'
            + comment_html
            + '</article>'
        )
    body = _folded_list(
        rendered,
        list_id="hot-paragraph-groups",
        visible_count=10,
        empty_message="本章暂无可定位的段评",
        open_label="加载更多热门段落",
        close_label="收起热门段落",
        auto_load=True,
    )
    if selected_paragraph_id is not None and isinstance(paragraph_detail, dict):
        body += _pagination_nav(
            paragraph_detail,
            review_view_url=review_view_url,
            query={"tab": "paragraph", "paragraphId": selected_paragraph_id},
            auto_load=True,
            list_id=f"paragraph-comments-{selected_paragraph_id}",
        )
    return body


def _page_hot_review_list(
    detail: dict[str, Any],
    *,
    review_view_url: str,
    paragraph_texts: dict[str, str] | None = None,
) -> str:
    # Page-hot view is a flat hot-list: keep jump links, never render paragraph quotes.
    _ = paragraph_texts
    comments = [item for item in detail.get("comments", []) if isinstance(item, dict)]
    body = _folded_list(
        [
            _review_card(
                review,
                review_view_url=review_view_url,
                link_to_paragraph=True,
                paragraph_text="",
            )
            for review in comments
        ],
        list_id="page-hot-comments",
        visible_count=10,
        empty_message="当前页暂未返回热评",
        open_label="加载更多热评",
        close_label="收起当前批次热评",
        auto_load=True,
    )
    paragraph_ids = detail.get("paragraphIds") or []
    paragraph_ids_text = ",".join(str(value) for value in paragraph_ids)
    return body + _pagination_nav(
        detail,
        review_view_url=review_view_url,
        query={"tab": "paragraph", "paragraphIds": paragraph_ids_text},
        auto_load=True,
        list_id="page-hot-comments",
    )


def _reply_detail_list(
    detail: dict[str, Any],
    *,
    review_view_url: str,
) -> str:
    root = detail.get("rootReview") if isinstance(detail.get("rootReview"), dict) else None
    replies = [item for item in detail.get("replies", []) if isinstance(item, dict)]
    target_name = str((root or {}).get("userName") or "书友")
    debug = detail.get("debug") if isinstance(detail.get("debug"), dict) else {}
    unsupported = debug.get("supported") is False
    # Keep the response container even for empty results. The client uses it to
    # distinguish an empty page from an invalid response, and retains previews.
    if not replies or debug.get("error"):
        failed = bool(debug.get("error")) and not unsupported
        message = (
            "当前书源暂不支持加载完整回复，已展示的回复仍可展开"
            if unsupported else "回复加载失败，请重试" if failed else "暂无更多回复"
        )
        error_attr = ' data-reply-error="true"' if failed else ""
        return (
            f'<div id="reply-detail-comments"{error_attr}>'
            + _empty_state(message) + '</div>'
        )
    body = _folded_list(
        [_reply_row(reply, target_name=target_name) for reply in replies],
        list_id="reply-detail-comments",
        visible_count=10,
        empty_message="该评论暂未返回回复",
        open_label="展开当前批次回复",
        close_label="收起当前批次回复",
    )
    return body + _pagination_nav(
        detail,
        review_view_url=review_view_url,
        query={
            "tab": "paragraph",
            "rootReviewId": detail.get("rootReviewId") or "",
        },
        cursor=True,
        auto_load=True,
        list_id="reply-detail-comments",
    )


_REVIEW_CSS = """
/* Douyin-like airy comment stream; colors stay CSS-var for client theme inject */
:root{
  color-scheme:light;
  font-family:"PingFang SC","Microsoft YaHei",system-ui,-apple-system,sans-serif;
  letter-spacing:0;
  --paper:#ffffff;
  --ink:#161823;
  --muted:#73747b;
  --faint:#a3a4ab;
  --name:#73747b;
  --line:#f0f0f2;
  --line-soft:#f5f5f7;
  --blue:#2f5dff;
  --blue-soft:#eef2ff;
  --link:#576b95;
  --green:#0f9f6e;
  --green-soft:#e7f7f0;
  --rose:#fe2c55;
  --author-bg:#fe2c55;
  --author-fg:#ffffff;
  --quote:#73747b;
  --quote-line:#d0d1d6;
}
*{box-sizing:border-box}
html,body{height:100%;margin:0;overflow:hidden}
body{
  min-width:320px;
  background:var(--paper);
  color:var(--ink);
  -webkit-text-size-adjust:100%;
}
button{font:inherit;letter-spacing:0;color:inherit}
a{color:var(--link)}

.review-sheet{
  display:flex;
  width:min(720px,100%);
  height:100%;
  margin:0 auto;
  flex-direction:column;
  background:var(--paper);
}

/* Tabs: left-weighted underline like modern comment shells */
.review-tabs{
  display:flex;
  flex:none;
  gap:4px;
  padding:0 16px;
  border-bottom:1px solid var(--line);
}
.review-tab{
  position:relative;
  min-height:44px;
  padding:0 14px;
  border:0;
  background:transparent;
  color:var(--muted);
  cursor:pointer;
  font-size:15px;
  font-weight:500;
  transition:color .15s ease;
}
.review-tab[aria-selected=true]{
  color:var(--ink);
  font-weight:700;
}
.review-tab[aria-selected=true]::after{
  content:"";
  position:absolute;
  bottom:0;
  left:50%;
  width:28px;
  height:3px;
  border-radius:3px 3px 0 0;
  background:var(--ink);
  transform:translateX(-50%);
}
.review-tab small{
  margin-left:4px;
  color:var(--faint);
  font-size:12px;
  font-weight:500;
}
.review-tab[aria-selected=true] small{color:var(--muted)}

.sheet-content{
  min-height:0;
  flex:1;
  overflow-y:auto;
  -webkit-overflow-scrolling:touch;
  touch-action:pan-y;
  padding:0 0 max(12px,env(safe-area-inset-bottom,0px));
}
.review-panel{
  display:none;
  padding:4px 16px 12px;
}
.review-panel.active{
  display:block;
  animation:review-panel-in .18s ease;
}
@keyframes review-panel-in{
  from{opacity:.35;transform:translateY(4px)}
  to{opacity:1;transform:none}
}
@media (prefers-reduced-motion:reduce){
  .review-panel.active{animation:none}
}

/* Root comments: spacing only, no hard card chrome */
.comment-item{
  display:grid;
  grid-template-columns:40px minmax(0,1fr);
  gap:12px;
  padding:16px 0;
  border-bottom:0;
}
.comment-item+.comment-item{
  border-top:1px solid var(--line-soft);
}
.comment-item--jump{
  cursor:pointer;
  border-radius:8px;
  transition:background-color .12s ease;
}
.comment-item--jump:active{
  background:color-mix(in srgb, var(--ink) 4%, var(--paper));
}
.comment-item--jump:focus-visible{
  outline:2px solid var(--blue);
  outline-offset:2px;
}
.comment-body{min-width:0}
.comment-avatar{
  position:relative;
  display:inline-flex;
  flex:none;
  overflow:visible;
  width:40px;
  height:40px;
  align-items:center;
  justify-content:center;
  border-radius:50%;
  background:var(--line-soft);
  color:var(--muted);
  font-size:13px;
  font-weight:700;
}
.comment-avatar.author{background:color-mix(in srgb, var(--rose) 12%, var(--paper));color:var(--rose)}
.avatar-fallback{position:relative;z-index:0}
.avatar-photo{
  position:absolute;z-index:1;inset:0;
  width:100%;height:100%;
  border-radius:50%;
  object-fit:cover;
  background:var(--line-soft);
}
.avatar-frame{
  position:absolute;z-index:2;
  top:-5px;left:-5px;
  width:calc(100% + 10px);height:calc(100% + 10px);
  object-fit:contain;pointer-events:none;
}
.comment-avatar.compact{width:28px;height:28px;font-size:11px}
.comment-avatar.compact .avatar-frame{
  top:-3px;left:-3px;
  width:calc(100% + 6px);height:calc(100% + 6px);
}

.comment-head{display:block}
.comment-identity{
  display:flex;
  min-width:0;
  align-items:center;
  flex-wrap:wrap;
  gap:6px;
}
.comment-identity strong{
  color:var(--name);
  font-size:14px;
  font-weight:500;
  overflow-wrap:anywhere;
}
.identity-tags{
  display:inline-flex;
  align-items:center;
  flex-wrap:wrap;
  gap:4px;
  max-width:100%;
}
.author-badge,
.position-badge,
.related-position,
.title-badge{
  display:inline-flex;
  min-height:16px;
  align-items:center;
  gap:2px;
  padding:0 5px;
  border-radius:4px;
  font-size:10px;
  font-weight:600;
  line-height:16px;
}
.author-badge{
  background:var(--author-bg);
  color:var(--author-fg);
  border-radius:3px;
}
.position-badge{background:var(--green-soft);color:var(--green)}
.related-position{margin-left:2px;background:var(--blue-soft);color:var(--blue)}
.title-badge{
  border:0;
  background:color-mix(in srgb, var(--ink) 5%, var(--paper));
  color:var(--muted);
}
.title-badge img{width:14px;height:14px;object-fit:contain}

/* 原文 block */
.paragraph-quote-block{
  margin:4px 0 8px;
  padding:0 0 4px;
}
.paragraph-quote-label{
  margin-bottom:6px;
  color:var(--faint);
  font-size:12px;
  font-weight:500;
}
.paragraph-quote,
.comment-paragraph{
  margin:0;
  padding:0 0 0 10px;
  border-left:2px solid var(--quote-line);
  border-radius:0;
  background:transparent;
  color:var(--quote);
  font-family:inherit;
  font-size:13px;
  line-height:1.6;
}
.comment-paragraph{margin-top:8px}

.comment-text{
  margin:6px 0 0;
  color:var(--ink);
  font-size:15px;
  line-height:1.55;
  font-weight:400;
}
.comment-text,.reply-line p{
  overflow-wrap:anywhere;
  word-break:break-word;
  white-space:pre-wrap;
}

/* Meta: time · 回复  left; heart/count right */
.comment-meta{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
  margin-top:8px;
  color:var(--faint);
  font-size:12px;
}
.comment-meta-left,
.comment-meta-right,
.reply-meta{
  display:inline-flex;
  align-items:center;
  flex-wrap:wrap;
  gap:10px;
  min-height:22px;
}
.comment-meta-right{gap:14px;margin-left:auto}
.comment-meta time,
.reply-meta time{
  color:var(--faint);
  font-size:12px;
  white-space:nowrap;
}
.meta-reply-btn{
  color:var(--faint);
  font-size:12px;
  font-weight:500;
  background:transparent;
  border:0;
  padding:0;
  cursor:default;
}
.meta-like,
.meta-reply-count{
  display:inline-flex;
  align-items:center;
  gap:4px;
  color:var(--faint);
  font-size:12px;
}
.meta-count{font-variant-numeric:tabular-nums}
.meta-icon{
  display:inline-block;
  width:16px;
  height:16px;
  background:currentColor;
  opacity:.85;
  -webkit-mask-size:contain;
  mask-size:contain;
  -webkit-mask-repeat:no-repeat;
  mask-repeat:no-repeat;
  -webkit-mask-position:center;
  mask-position:center;
}
.meta-icon-like{
  -webkit-mask-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='black' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'><path d='M12 21s-6.5-4.35-9.2-8.2C1.1 10.5 1.5 7.2 4 5.5 6.1 4.1 8.7 4.6 10 6.2 11.3 4.6 13.9 4.1 16 5.5c2.5 1.7 2.9 5 1.2 7.3C18.5 16.65 12 21 12 21z'/></svg>");
  mask-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='black' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'><path d='M12 21s-6.5-4.35-9.2-8.2C1.1 10.5 1.5 7.2 4 5.5 6.1 4.1 8.7 4.6 10 6.2 11.3 4.6 13.9 4.1 16 5.5c2.5 1.7 2.9 5 1.2 7.3C18.5 16.65 12 21 12 21z'/></svg>");
}
.meta-icon-reply{
  -webkit-mask-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='black' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'><path d='M21 12a8.5 8.5 0 0 1-8.5 8.5H5l-3 3V12A8.5 8.5 0 1 1 21 12z'/></svg>");
  mask-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24' fill='none' stroke='black' stroke-width='1.8' stroke-linecap='round' stroke-linejoin='round'><path d='M21 12a8.5 8.5 0 0 1-8.5 8.5H5l-3 3V12A8.5 8.5 0 1 1 21 12z'/></svg>");
}

.comment-reply-context{
  display:flex;
  align-items:center;
  flex-wrap:wrap;
  gap:4px;
  margin-top:4px;
  color:var(--faint);
  font-size:12px;
}
.comment-reply-context strong{color:var(--link);font-size:12px;font-weight:500}

/* Replies: pure hierarchy indent, no rounded box */
.reply-stack{
  margin-top:10px;
  margin-left:0;
  padding:0;
  border:0;
  background:transparent;
}
.reply-surface{overflow:hidden}
.reply-line{
  display:grid;
  grid-template-columns:28px minmax(0,1fr);
  gap:10px;
  padding:10px 0 4px;
  color:var(--muted);
  font-size:13px;
  line-height:1.5;
}
.reply-line+.reply-line{
  display:none;
}
.reply-stack.open .reply-line{display:grid}
.reply-body{min-width:0}
.reply-heading{
  display:flex;
  align-items:center;
  flex-wrap:wrap;
  gap:4px;
}
.reply-heading .identity-tags{gap:3px}
.reply-line p{
  margin:4px 0 0;
  color:var(--ink);
  font-size:14px;
  line-height:1.5;
}
.reply-author{color:var(--name);font-weight:500;font-size:13px}
.reply-target{color:var(--name);font-weight:500;font-size:13px}
.reply-arrow{
  margin:0 2px;
  color:var(--faint);
  font-size:9px;
  transform:scaleX(.85);
}
.reply-meta{
  margin-top:6px;
  width:100%;
}
.reply-meta .meta-like{margin-left:auto}

.reply-toggle{
  display:inline-flex;
  position:relative;
  min-height:28px;
  align-items:center;
  gap:6px;
  margin:4px 0 0 38px;
  padding:4px 0;
  border:0;
  background:transparent;
  color:var(--faint);
  cursor:pointer;
  font-size:13px;
  font-weight:500;
}
.reply-toggle::before{
  content:"";
  display:inline-block;
  width:20px;
  height:1px;
  background:var(--line);
  margin-right:2px;
}
.reply-toggle-chevron{
  display:inline-block;
  width:0;height:0;
  border-left:4px solid transparent;
  border-right:4px solid transparent;
  border-top:5px solid currentColor;
  opacity:.75;
}
.reply-stack.open .reply-toggle-chevron{
  border-top:0;
  border-bottom:5px solid currentColor;
}
.reply-toggle:active{opacity:.7}

.fold-toggle{
  display:block;
  width:100%;
  min-height:40px;
  margin-top:4px;
  border:0;
  border-radius:0;
  background:transparent;
  color:var(--faint);
  cursor:pointer;
  font-size:13px;
  font-weight:500;
}
.fold-toggle:active{opacity:.7}

.pagination-row{
  display:flex;
  justify-content:center;
  gap:10px;
  padding:14px 0 4px;
}
.pagination-row a{
  min-width:0;
  min-height:32px;
  display:inline-flex;
  align-items:center;
  justify-content:center;
  padding:0 8px;
  border:0;
  border-radius:0;
  color:var(--faint);
  font-size:13px;
  font-weight:500;
  text-decoration:none;
  background:transparent;
}
.pagination-row a::before{
  content:"";
  display:inline-block;
  width:16px;
  height:1px;
  margin-right:8px;
  background:var(--line);
}

.paragraph-group{padding:12px 0 4px}
.paragraph-group+.paragraph-group{
  margin-top:8px;
  border-top:1px solid var(--line-soft);
  padding-top:16px;
}
.paragraph-quote-block--jump{
  cursor:pointer;
  border-radius:8px;
  padding:8px 10px;
  margin:0 -10px 8px;
  transition:background-color .12s ease;
}
.paragraph-quote-block--jump:active{
  background:color-mix(in srgb, var(--ink) 4%, var(--paper));
}
.paragraph-quote-block--jump:focus-visible{
  outline:2px solid var(--blue);
  outline-offset:2px;
}

.empty-state{
  padding:48px 12px;
  color:var(--faint);
  font-size:14px;
  text-align:center;
}
.load-status{
  display:block;
  width:100%;
  min-height:40px;
  border:0;
  background:transparent;
  color:var(--faint);
  font-size:13px;
  text-align:center;
}
.load-status.error{color:var(--rose);cursor:pointer}

.comment-emoticon{
  display:inline-block;
  width:20px;height:20px;
  object-fit:contain;
  vertical-align:text-bottom;
}
.comment-emoticon-fallback{
  display:inline-flex;
  min-height:18px;
  align-items:center;
  padding:0 5px;
  border:0;
  border-radius:4px;
  background:var(--line-soft);
  color:var(--muted);
  font-size:11px;
  vertical-align:middle;
}
.comment-media-wrap{margin-top:10px}
.comment-media{
  display:block;
  width:auto;
  max-width:min(240px,72%);
  max-height:280px;
  border:0;
  border-radius:4px;
  background:var(--line-soft);
  object-fit:contain;
}
.reply-line .comment-media{
  max-width:min(180px,70%);
  max-height:200px;
  border-radius:4px;
}

.position-title-image,.title-image,.related-position-image{
  display:inline-flex;height:16px;align-items:center;
}
.position-title-image img,.title-image img,.related-position-image img{
  display:block;width:auto;height:16px;max-width:96px;object-fit:contain;
}
.related-position-image{margin-left:2px}

[hidden]{display:none!important}

@media (max-width:640px){
  .review-panel{padding-right:14px;padding-left:14px}
  .review-tabs{padding-right:8px;padding-left:8px}
  .comment-media{max-height:220px}
  .reply-line .comment-media{max-height:160px}
}

@supports not (background:color-mix(in srgb, #000 10%, #fff)){
  .comment-avatar.author{background:#ffe8ed;color:var(--rose)}
  .title-badge,.comment-emoticon-fallback,.comment-media{
    background:var(--line-soft);
  }
}
"""


_REVIEW_SCRIPT = r"""
const tabs = [...document.querySelectorAll("[data-tab]")];
const panels = [...document.querySelectorAll("[data-panel]")];
const scrollRoot = document.querySelector(".sheet-content");
const loadedUrls = new Set();
let loading = false;
let observer;
let replyObserver;

function activePanel() {
  return document.querySelector(".review-panel.active");
}

function applyFold(list, open) {
  const limit = Number(list.dataset.visibleCount || 1);
  [...list.children].forEach((item, index) => {
    item.hidden = !open && index >= limit;
  });
  list.dataset.open = String(open);
}

function setReplyOpen(button, open) {
  const stack = button.closest(".reply-stack");
  stack.classList.toggle("open", open);
  button.setAttribute("aria-expanded", String(open));
  button.querySelector(".reply-toggle-label").textContent = open
    ? button.dataset.closeLabel
    : button.dataset.openLabel;
  replyObserver.unobserve(button);
  if (open && button.dataset.nextReplyUrl) replyObserver.observe(button);
}

async function loadReplyDetails(button, url) {
  if (!url || button.dataset.loading) return false;
  const loadedReplyUrls = button.loadedReplyUrls || new Set();
  if (loadedReplyUrls.has(url)) return false;
  button.dataset.loading = "true";
  button.disabled = true;
  try {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const source = new DOMParser().parseFromString(await response.text(), "text/html");
    const sourceList = source.getElementById("reply-detail-comments");
    const surface = button.closest(".reply-stack")?.querySelector(".reply-surface");
    if (!sourceList || !surface) throw new Error("回复分页结构不完整");
    if (sourceList.dataset.replyError === "true") throw new Error("回复请求失败");
    appendUnique(surface, sourceList);
    const next = source.querySelector(
      '[data-auto-pagination][data-list-id="reply-detail-comments"] [data-next-page]'
    );
    loadedReplyUrls.add(url);
    button.loadedReplyUrls = loadedReplyUrls;
    button.dataset.loaded = "true";
    button.dataset.nextReplyUrl = next?.href && !loadedReplyUrls.has(next.href) ? next.href : "";
    delete button.dataset.replyError;
    return true;
  } catch (error) {
    button.dataset.replyError = "true";
    button.querySelector(".reply-toggle-label").textContent = "加载失败，点击重试";
    return false;
  } finally {
    button.disabled = false;
    delete button.dataset.loading;
  }
}

function bindReplyToggles(root = document) {
  root.querySelectorAll("[data-reply-toggle]").forEach((button) => {
    if (button.dataset.bound) return;
    button.dataset.bound = "true";
    button.addEventListener("click", async () => {
      const stack = button.closest(".reply-stack");
      const open = stack.classList.contains("open");
      if (open && button.dataset.replyError !== "true") {
        setReplyOpen(button, false);
        return;
      }
      const replyUrl = button.dataset.loaded === "true"
        ? button.dataset.nextReplyUrl
        : button.dataset.replyUrl;
      if (replyUrl) {
        button.querySelector(".reply-toggle-label").textContent = "正在加载回复...";
        if (!await loadReplyDetails(button, replyUrl)) return;
      }
      setReplyOpen(button, true);
    });
  });
}

function ensureStatus(panel) {
  let status = panel.querySelector(":scope > .load-status");
  if (!status) {
    status = document.createElement("button");
    status.type = "button";
    status.className = "load-status";
    status.setAttribute("aria-live", "polite");
    status.setAttribute("aria-atomic", "true");
    status.hidden = true;
    status.addEventListener("click", () => {
      if (status.classList.contains("error")) loadNext(panel);
    });
  }
  panel.append(status);
  return status;
}

function setStatus(panel, text, state = "") {
  const status = ensureStatus(panel);
  status.textContent = text;
  status.hidden = !text;
  status.disabled = state !== "error";
  status.classList.toggle("error", state === "error");
}

function revealAutoFold(button) {
  const panel = button.closest(".review-panel");
  const list = document.getElementById(button.dataset.foldToggle);
  if (!list) return;
  const items = [...list.children];
  const batchSize = Number(list.dataset.visibleCount || 10);
  const visibleCount = items.filter((item) => !item.hidden).length;
  const nextLimit = Math.min(items.length, visibleCount + batchSize);
  items.forEach((item, index) => { item.hidden = index >= nextLimit; });
  const complete = nextLimit >= items.length;
  list.dataset.open = String(complete);
  if (complete) button.remove();
  if (complete && !nextLink(panel)) setStatus(panel, "没有更多了", "end");
  arm();
}

function bindFoldToggles(root = document) {
  root.querySelectorAll("[data-fold-toggle]").forEach((button) => {
    if (button.dataset.bound) return;
    button.dataset.bound = "true";
    button.addEventListener("click", () => {
      if (button.dataset.autoFoldToggle) {
        revealAutoFold(button);
        return;
      }
      const list = document.getElementById(button.dataset.foldToggle);
      const open = list.dataset.open !== "true";
      applyFold(list, open);
      button.setAttribute("aria-expanded", String(open));
      button.textContent = open ? button.dataset.closeLabel : button.dataset.openLabel;
    });
  });
}

function nextLink(panel) {
  return panel?.querySelector("[data-auto-pagination] [data-next-page]") || null;
}

function loadTarget(panel) {
  return panel?.querySelector("[data-auto-fold-toggle]")
    || panel?.querySelector("[data-auto-pagination]")
    || null;
}

function itemKey(element) {
  if (element.dataset.reviewId) return `review:${element.dataset.reviewId}`;
  if (element.dataset.paragraphId) return `paragraph:${element.dataset.paragraphId}`;
  return "";
}

function appendUnique(currentList, sourceList) {
  const known = new Set([...currentList.children].map(itemKey).filter(Boolean));
  let appended = 0;
  [...sourceList.children].forEach((item) => {
    const key = itemKey(item);
    if (key && known.has(key)) return;
    currentList.append(item.cloneNode(true));
    if (key) known.add(key);
    appended += 1;
  });
  return appended;
}

async function loadNext(panel = activePanel()) {
  if (!panel || loading) return;
  const foldButton = panel.querySelector("[data-auto-fold-toggle]");
  if (foldButton) {
    revealAutoFold(foldButton);
    return;
  }
  const link = nextLink(panel);
  if (!link || loadedUrls.has(link.href)) return;

  loading = true;
  observer.disconnect();
  setStatus(panel, "加载中...", "loading");
  let failed = false;
  try {
    const response = await fetch(link.href);
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const source = new DOMParser().parseFromString(await response.text(), "text/html");
    const sourcePanel = source.querySelector(`[data-panel="${CSS.escape(panel.dataset.panel)}"]`);
    const currentNav = panel.querySelector("[data-auto-pagination]");
    const listId = currentNav?.dataset.listId || "";
    const currentList = listId ? document.getElementById(listId) : null;
    const sourceList = listId ? source.getElementById(listId) : null;
    if (!sourcePanel || !currentList) throw new Error("评论分页结构不完整");

    /* A follow-up page may legitimately carry no rows; that is the end of the
       list, not a transport failure. */
    if (sourceList) appendUnique(currentList, sourceList);
    bindReplyToggles(currentList);
    bindFoldToggles(currentList);
    loadedUrls.add(link.href);

    panel.querySelector("[data-auto-pagination]")?.remove();
    const newNav = sourcePanel.querySelector("[data-auto-pagination]")?.cloneNode(true);
    newNav?.querySelectorAll("[data-previous-page]").forEach((item) => item.remove());
    if (newNav?.querySelector("[data-next-page]")) panel.append(newNav);
    setStatus(panel, nextLink(panel) ? "" : "没有更多了", "end");
  } catch (error) {
    failed = true;
    setStatus(panel, "网络错误，点击重试", "error");
  } finally {
    loading = false;
    if (!failed) arm();
  }
}

function arm() {
  observer.disconnect();
  const target = loadTarget(activePanel());
  if (target) observer.observe(target);
}

function activate(name, resetScroll = false) {
  tabs.forEach((tab) => {
    const selected = tab.dataset.tab === name;
    tab.setAttribute("aria-selected", String(selected));
    tab.tabIndex = selected ? 0 : -1;
  });
  panels.forEach((panel) => {
    const selected = panel.dataset.panel === name;
    panel.classList.toggle("active", selected);
    panel.hidden = !selected;
  });
  if (resetScroll) scrollRoot.scrollTop = 0;
  arm();
}

observer = new IntersectionObserver(
  (entries) => {
    if (entries.some((entry) => entry.isIntersecting)) loadNext();
  },
  { root: scrollRoot, rootMargin: "0px 0px 120px 0px" },
);
replyObserver = new IntersectionObserver(
  (entries) => {
    entries.forEach(async (entry) => {
      const button = entry.target;
      if (!entry.isIntersecting || !button.dataset.nextReplyUrl || button.dataset.loading) return;
      replyObserver.unobserve(button);
      const loaded = await loadReplyDetails(button, button.dataset.nextReplyUrl);
      if (loaded && button.closest(".reply-stack")?.classList.contains("open") && button.dataset.nextReplyUrl) {
        replyObserver.observe(button);
      }
    });
  },
  { root: scrollRoot, rootMargin: "0px 0px 100px 0px" },
);
function paragraphJumpTarget(node) {
  return node?.closest?.("[data-paragraph-href]") || null;
}

function shouldIgnoreParagraphJump(target) {
  return Boolean(
    target.closest(
      "a,button,input,textarea,select,label,.reply-stack,.reply-toggle,.fold-toggle,.pagination-row,.meta-like,.meta-reply-count"
    )
  );
}

function goParagraphHref(el) {
  const href = el?.dataset?.paragraphHref;
  if (href) window.location.href = href;
}

function bindParagraphJumps() {
  document.addEventListener("click", (event) => {
    if (shouldIgnoreParagraphJump(event.target)) return;
    const jump = paragraphJumpTarget(event.target);
    if (!jump) return;
    event.preventDefault();
    goParagraphHref(jump);
  });
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const jump = event.target?.closest?.("[data-paragraph-href]");
    if (!jump || event.target !== jump) return;
    event.preventDefault();
    goParagraphHref(jump);
  });
}

function bindTabSwipe() {
  /* Chapter / paragraph tabs only — page-hot & reply drill-downs have a single panel. */
  if (!scrollRoot || tabs.length < 2) return;
  const order = tabs.map((tab) => tab.dataset.tab).filter(Boolean);
  if (order.length < 2) return;

  const threshold = 56;
  const axisLock = 10;
  let startX = 0;
  let startY = 0;
  let tracking = false;
  let axis = ""; /* "" | "h" | "v" */

  const onStart = (clientX, clientY) => {
    tracking = true;
    axis = "";
    startX = clientX;
    startY = clientY;
  };

  const onMove = (clientX, clientY) => {
    if (!tracking || axis) return;
    const dx = clientX - startX;
    const dy = clientY - startY;
    if (Math.abs(dx) < axisLock && Math.abs(dy) < axisLock) return;
    axis = Math.abs(dx) > Math.abs(dy) * 1.15 ? "h" : "v";
  };

  const onEnd = (clientX) => {
    if (!tracking) return;
    tracking = false;
    if (axis !== "h") return;
    const dx = clientX - startX;
    if (Math.abs(dx) < threshold) return;
    const current = activePanel()?.dataset.panel || order[0];
    const index = order.indexOf(current);
    if (index < 0) return;
    if (dx < 0 && index < order.length - 1) {
      activate(order[index + 1], true); /* swipe left → next (段评说) */
    } else if (dx > 0 && index > 0) {
      activate(order[index - 1], true); /* swipe right → prev (本章说) */
    }
  };

  scrollRoot.addEventListener(
    "touchstart",
    (event) => {
      if (event.touches.length !== 1) return;
      onStart(event.touches[0].clientX, event.touches[0].clientY);
    },
    { passive: true }
  );
  scrollRoot.addEventListener(
    "touchmove",
    (event) => {
      if (!tracking || event.touches.length !== 1) return;
      onMove(event.touches[0].clientX, event.touches[0].clientY);
    },
    { passive: true }
  );
  scrollRoot.addEventListener(
    "touchend",
    (event) => {
      if (!tracking) return;
      const touch = event.changedTouches[0];
      onEnd(touch ? touch.clientX : startX);
    },
    { passive: true }
  );
  scrollRoot.addEventListener("touchcancel", () => {
    tracking = false;
    axis = "";
  }, { passive: true });

  /* Pointer drag for desktop / mouse-emulated WebView debugging */
  let pointerId = null;
  scrollRoot.addEventListener("pointerdown", (event) => {
    if (event.pointerType === "mouse" && event.button !== 0) return;
    if (event.pointerType === "touch") return; /* touch path above */
    pointerId = event.pointerId;
    onStart(event.clientX, event.clientY);
  });
  scrollRoot.addEventListener("pointermove", (event) => {
    if (pointerId !== event.pointerId) return;
    onMove(event.clientX, event.clientY);
  });
  const endPointer = (event) => {
    if (pointerId !== event.pointerId) return;
    pointerId = null;
    onEnd(event.clientX);
  };
  scrollRoot.addEventListener("pointerup", endPointer);
  scrollRoot.addEventListener("pointercancel", (event) => {
    if (pointerId !== event.pointerId) return;
    pointerId = null;
    tracking = false;
    axis = "";
  });
}

document.querySelectorAll("[data-fold-list]").forEach((list) => applyFold(list, false));
bindReplyToggles();
bindFoldToggles();
bindParagraphJumps();
bindTabSwipe();
tabs.forEach((tab) => tab.addEventListener("click", () => activate(tab.dataset.tab, true)));
activate("__ACTIVE_TAB__");
"""


def render_chapter_reviews_html(
    *,
    chapter_title: str,
    reviews: dict[str, Any],
    review_view_url: str,
    active_tab: str = "chapter",
    selected_paragraph_id: int | None = None,
    paragraph_detail: dict[str, Any] | None = None,
    page_hot_detail: dict[str, Any] | None = None,
    chapter_detail: dict[str, Any] | None = None,
    reply_detail: dict[str, Any] | None = None,
) -> str:
    """Render the review sheet, limiting drill-down pages to paragraph reviews."""
    paragraph_only = any(
        isinstance(detail, dict)
        for detail in (paragraph_detail, page_hot_detail, reply_detail)
    )
    if paragraph_only:
        active_tab = "paragraph"
    active_tab = active_tab if active_tab in {"chapter", "paragraph"} else "chapter"
    chapter_reviews = _dedupe_reviews(reviews.get("chapterEndHot"), reviews.get("chapterEnd"))
    matched_paragraphs = [
        item
        for item in reviews.get("hotParagraphReviews", [])
        if isinstance(item, dict) and (item.get("matchedText") or item.get("paragraphText"))
    ]
    paragraph_count = sum(
        int(item.get("commentCount") or item.get("totalCommentCount") or 0)
        for item in matched_paragraphs
    )
    summary = reviews.get("summary") if isinstance(reviews.get("summary"), dict) else {}
    chapter_count = int(summary.get("chapterEndCount") or len(chapter_reviews))
    total_reviews = int(summary.get("totalReviews") or chapter_count + paragraph_count)
    chapter_page_reviews = (
        [item for item in chapter_detail.get("comments", []) if isinstance(item, dict)]
        if isinstance(chapter_detail, dict)
        else chapter_reviews
    )
    chapter_html = _folded_list(
        [_review_card(review, review_view_url=review_view_url) for review in chapter_page_reviews],
        list_id="chapter-comments",
        visible_count=10,
        empty_message="本章暂未返回章末评论",
        open_label="加载更多本章说",
        close_label="收起本章说",
        auto_load=True,
    )
    chapter_page_payload = chapter_detail if isinstance(chapter_detail, dict) else {
        "page": 1,
        "pageSize": 10,
        "hasMore": len(reviews.get("chapterEnd") or []) < chapter_count,
        "nextPage": 2,
    }
    chapter_html += _pagination_nav(
        chapter_page_payload,
        review_view_url=review_view_url,
        query={"tab": "chapter"},
        auto_load=True,
        list_id="chapter-comments",
    )
    if isinstance(reply_detail, dict):
        paragraph_html = _reply_detail_list(reply_detail, review_view_url=review_view_url)
        paragraph_scope_label = "评论回复"
        paragraph_scope_count = int(reply_detail.get("totalCount") or 0)
    elif isinstance(page_hot_detail, dict):
        paragraph_html = _page_hot_review_list(
            page_hot_detail,
            review_view_url=review_view_url,
        )
        paragraph_scope_label = "当前页热评"
        paragraph_scope_count = int(page_hot_detail.get("totalCount") or 0)
    else:
        paragraph_html = _paragraph_groups(
            reviews,
            review_view_url=review_view_url,
            selected_paragraph_id=selected_paragraph_id,
            paragraph_detail=paragraph_detail,
        )
        paragraph_scope_label = (
            "段落评论"
            if selected_paragraph_id is not None
            else f"{len(matched_paragraphs)} 个热门段落"
        )
        paragraph_scope_count = (
            int(paragraph_detail.get("totalCount") or 0)
            if isinstance(paragraph_detail, dict)
            else paragraph_count
        )

    def tab(name: str, label: str, count: int) -> str:
        selected = "true" if active_tab == name else "false"
        tab_index = "0" if active_tab == name else "-1"
        return (
            f'<button class="review-tab" id="review-tab-{name}" type="button" role="tab" '
            f'data-tab="{name}" aria-controls="review-panel-{name}" '
            f'aria-selected="{selected}" tabindex="{tab_index}">'
            f'{label}<small>{_count_label(count)}</small></button>'
        )

    def panel(name: str, label: str, count: int, body: str, *, tabbed: bool = True) -> str:
        # label/count used only for a11y; visible scope-row was duplicate chrome (title + count).
        _ = count
        active = " active" if active_tab == name else ""
        hidden = "" if active_tab == name else " hidden"
        semantics = (
            f' id="review-panel-{name}" role="tabpanel" aria-labelledby="review-tab-{name}"'
            if tabbed
            else f' role="region" aria-label="{html.escape(label, quote=True)}"'
        )
        return (
            f'<section class="review-panel{active}" data-panel="{name}"{semantics}{hidden}>'
            + body
            + '</section>'
        )

    tabs_html = ""
    panels_html = panel(
        "paragraph",
        paragraph_scope_label,
        paragraph_scope_count,
        paragraph_html,
        tabbed=not paragraph_only,
    )
    if not paragraph_only:
        tabs_html = (
            '<div class="review-tabs" role="tablist" aria-label="评论分类">'
            + tab("chapter", "本章说", chapter_count)
            + tab("paragraph", "段评说", paragraph_count)
            + '</div>'
        )
        panels_html = (
            panel("chapter", "本章说", chapter_count, chapter_html)
            + panels_html
        )

    if isinstance(reply_detail, dict):
        view_title = "评论回复"
    elif isinstance(page_hot_detail, dict):
        view_title = "页热评"
    elif selected_paragraph_id is not None:
        view_title = "段落评论"
    else:
        view_title = "本章评论"
    view_count = paragraph_scope_count if paragraph_only else total_reviews

    return (
        '<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8">'
        '<meta name="viewport" content="width=device-width,initial-scale=1">'
        # Document title only (not shown in-panel). Client already has drag handle;
        # in-page sheet-header + scope-row duplicated view type / chapter / count.
        f'<title>{html.escape(chapter_title)} · {view_title} · {_count_label(view_count)} 条</title>'
        f'<style>{_REVIEW_CSS}</style></head><body>'
        '<main class="review-sheet">'
        + tabs_html
        + '<div class="sheet-content">'
        + panels_html
        + '</div></main><script>'
        + _REVIEW_SCRIPT.replace("__ACTIVE_TAB__", active_tab)
        + '</script></body></html>'
    )
