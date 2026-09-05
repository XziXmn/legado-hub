"""Generate the Legado virtual source JSON for the shared subscription library.

The virtual source used to live under ``/api/legado/*``; after the shared
subscription refactor it is exposed at ``/api/subscribe/legado/*``.
"""

from __future__ import annotations

import json
from pathlib import Path

from app.config import GENERATED_DIR
from app.core.aggregate_config import load_aggregate_config
from app.core.app_config import AppConfig
from app.core.public_security import (
    get_public_base_url,
    is_lan_reading_base,
    normalize_public_base_url,
)


# Reading identifies this source by bookSourceUrl and only offers updates when
# lastUpdateTime increases (not when the display version string changes alone).
#
# Release discipline (do not mix):
# - BETA / daily rule tests: ONLY bump _READER_RULE_RELEASED_AT_MS to wall-clock
#   now (ms). Keep _READER_RULE_VERSION unchanged so the name does not churn.
# - FORMAL app release (git tag vX.Y.Z): bump BOTH — version (shown in name /
#   comment / jsLib) and RELEASED_AT_MS.
_READER_RULE_VERSION = "0.0.28"
# Last beta marker: header no-ajax so login sheet can open (ms). Bump this alone for tests.
_READER_RULE_RELEASED_AT_MS = 1_788_631_303_365

# Dual source identity: public vs LAN imports coexist in Reading.
_PUBLIC_BOOK_SOURCE_URL = "LegadoHub"
_LAN_BOOK_SOURCE_URL = "LegadoHub-LAN"
_LAN_NAME_MARK = "·内网"
_LAN_GROUP_MARK = "内网"


def _reader_rule_version_stamp(version: str = _READER_RULE_VERSION) -> int:
    """Secondary monotonic component derived from X.Y.Z (not a wall clock)."""
    parts = str(version or "0").strip().split(".")
    nums: list[int] = []
    for part in parts[:3]:
        try:
            nums.append(max(0, int(part)))
        except ValueError:
            nums.append(0)
    while len(nums) < 3:
        nums.append(0)
    major, minor, patch = nums
    return major * 100_000_000 + minor * 100_000 + patch * 100


def _reader_rule_last_update_time(config: AppConfig) -> int:
    """Reading update signal: max(RELEASED_AT + version stamp, AppConfig mtime).

    Clients key off lastUpdateTime. For beta rule ships, only RELEASED_AT_MS
    needs to increase; the version stamp is stable between formal releases.
    """
    floor = _READER_RULE_RELEASED_AT_MS + _reader_rule_version_stamp()
    try:
        config_modified_at = config.path.stat().st_mtime_ns // 1_000_000
    except OSError:
        return floor
    return max(floor, config_modified_at)


def _login_ui(*, bound_access_code: bool = False) -> str:
    """Reading source sheet: 订阅 + 书库 only (no tip/password/login buttons).

    Avoid type=text tip rows — some clients fail to open the sheet when the only
    non-button control is a fake tip field. Auth is automatic for personal links.
    """
    del bound_access_code  # reserved for future copy variants
    btn = {"layout_flexGrow": 1, "layout_flexBasisPercent": 0.48}
    return json.dumps(
        [
            {
                "name": "订阅",
                "type": "button",
                "action": "legadoHubOpenSubscriptions()",
                "style": dict(btn),
            },
            {
                "name": "书库",
                "type": "button",
                "action": "legadoHubOpenLibrary()",
                "style": dict(btn),
            },
        ],
        ensure_ascii=False,
    )


def _auth_runtime_js(base_api: str, *, access_code: str | None = None) -> str:
    """Shared JS: resolve Bearer from LoginHeader, else redeem bound/form code.

    Used by book-source ``header`` (search/toc/book) and by chapter ``java.ajax``
    so search / subscribe / chapter share the same token logic.
    Does not eval ``loginUrl`` (that previously broke Reading login UI).
    """
    base_literal = json.dumps(str(base_api or "").rstrip("/"), ensure_ascii=False)
    access_literal = json.dumps(str(access_code or ""), ensure_ascii=False)
    return f"""
function legadoHubReadStoredAuth() {{
    try {{
        var raw = source.getLoginHeader();
        var st = typeof raw === "string" ? JSON.parse(raw || "{{}}") : (raw || {{}});
        var a = st && (st.Authorization || st.authorization) || "";
        return String(a || "").trim();
    }} catch (e) {{
        return "";
    }}
}}
function legadoHubReadAccessCode() {{
    var code = {access_literal};
    if (String(code || "").trim()) return String(code).trim();
    try {{
        var info = source.getLoginInfoMap();
        if (!info) return "";
        try {{
            var v = info.get("授权码");
            if (v !== null && v !== undefined && String(v).trim()) return String(v).trim();
        }} catch (e1) {{}}
        try {{
            if (info["授权码"] && String(info["授权码"]).trim()) return String(info["授权码"]).trim();
        }} catch (e2) {{}}
    }} catch (e3) {{}}
    return "";
}}
function legadoHubHasBoundToken() {{
    return !!String({access_literal} || "").trim();
}}
function legadoHubRedeemToAuth() {{
    var code = legadoHubReadAccessCode();
    var base = {base_literal};
    if (!code || !base) return "";
    try {{
        var body = JSON.stringify({{accessCode: code}});
        var opt = {{
            method: "POST",
            headers: {{"Accept": "application/json", "Content-Type": "application/json"}},
            body: body
        }};
        var text = String(java.ajax(base + "/api/auth/access/redeem," + JSON.stringify(opt)) || "").trim();
        if (!text) return "";
        var payload = JSON.parse(text);
        if (!payload || !payload.token) return "";
        var auth = "Bearer " + String(payload.token);
        try {{ source.putLoginHeader(JSON.stringify({{Authorization: auth}})); }} catch (e4) {{}}
        return auth;
    }} catch (e5) {{
        return "";
    }}
}}
function legadoHubResolveAuth() {{
    var auth = legadoHubReadStoredAuth();
    if (auth) return auth;
    // Only auto-redeem when personal link embeds a code, or user already saved 授权码.
    return legadoHubRedeemToAuth();
}}
function legadoHubAuthHeaders() {{
    var h = {{"Accept": "application/json"}};
    try {{
        var auth = legadoHubResolveAuth();
        if (auth) h.Authorization = auth;
    }} catch (e) {{}}
    return h;
}}
function legadoHubAjax(url, method, body) {{
    var opt = {{
        method: String(method || "GET").toUpperCase(),
        headers: legadoHubAuthHeaders()
    }};
    if (body !== undefined && body !== null) {{
        opt.body = typeof body === "string" ? body : JSON.stringify(body);
        if (!opt.headers["Content-Type"]) opt.headers["Content-Type"] = "application/json";
    }}
    return String(java.ajax(String(url || "") + "," + JSON.stringify(opt)) || "");
}}
"""


def _request_header_rule(base_api: str = "", *, access_code: str | None = None) -> str:
    """AnalyzeUrl header: inject stored Bearer only — never network.

    Reading evaluates book-source header when opening the login sheet. Any
    java.ajax / redeem here prevents the sheet from appearing. Token redeem
    happens in searchUrl / legadoHubAjax / loginCheckJs instead.
    """
    del base_api, access_code
    return (
        "@js:\n"
        "var h = {\"Accept\": \"application/json\"};\n"
        "try {\n"
        "  var raw = source.getLoginHeader();\n"
        "  var st = typeof raw === \"string\" ? JSON.parse(raw || \"{}\") : (raw || {});\n"
        "  var a = st && (st.Authorization || st.authorization);\n"
        "  if (a && String(a).trim()) h.Authorization = String(a);\n"
        "} catch (e) {}\n"
        "JSON.stringify(h);"
    )


def _search_url_rule(base_api: str) -> str:
    """Search entry: resolve token (bound code / stored header) then hit API."""
    base = json.dumps(str(base_api or "").rstrip("/"), ensure_ascii=False)
    # key/page are injected by AnalyzeUrl for searchUrl @js.
    return (
        "@js:\n"
        "try { if (typeof legadoHubResolveAuth === \"function\") legadoHubResolveAuth(); } catch (e0) {}\n"
        "var _base = " + base + ";\n"
        "var _key = \"\";\n"
        "var _page = \"1\";\n"
        "try { _key = String(key != null ? key : \"\"); } catch (e1) { _key = \"\"; }\n"
        "try { _page = String(page != null ? page : \"1\"); } catch (e2) { _page = \"1\"; }\n"
        "_base + \"/api/subscribe/legado/search?keyword=\" + encodeURIComponent(_key) + \"&page=\" + encodeURIComponent(_page);"
    )


def _explore_url_rule(base_api: str) -> str:
    """Explore entry with pre-request token resolve (same as search)."""
    base = str(base_api or "").rstrip("/")
    # Keep group title prefix; URL body is @js so token is attached before GET.
    return (
        "已发布书库::@js:\n"
        "try { if (typeof legadoHubResolveAuth === \"function\") legadoHubResolveAuth(); } catch (e0) {}\n"
        "var _base = "
        + json.dumps(base, ensure_ascii=False)
        + ";\n"
        "var _page = \"1\";\n"
        "try { _page = String(page != null ? page : \"1\"); } catch (e1) { _page = \"1\"; }\n"
        "_base + \"/api/subscribe/legado/explore?page=\" + encodeURIComponent(_page);"
    )


def _login_script(base_api: str, *, access_code: str | None = None) -> str:
    base_literal = json.dumps(base_api.rstrip("/"), ensure_ascii=False)
    access_literal = json.dumps(str(access_code or ""), ensure_ascii=False)
    return f"""var LEGADOHUB_BASE = {base_literal};
var LEGADOHUB_ACCESS_CODE = {access_literal};

function legadoHubBoundAccessCode() {{
    try {{
        return String(LEGADOHUB_ACCESS_CODE || "").trim();
    }} catch (e) {{
        return "";
    }}
}}

function legadoHubLoginInfoValue(name) {{
    function readValue(info) {{
        if (!info) return null;
        try {{
            if (typeof info === "string") info = JSON.parse(info);
        }} catch (e) {{}}
        try {{
            var mapped = info.get(name);
            if (mapped !== null && mapped !== undefined) return String(mapped);
        }} catch (e) {{}}
        try {{
            var direct = info[name];
            if (direct !== null && direct !== undefined) return String(direct);
        }} catch (e) {{}}
        try {{
            if (info.containsKey(name)) return String(info.get(name) || "");
        }} catch (e) {{}}
        try {{
            if (info.has(name)) return String(info.get(name) || "");
        }} catch (e) {{}}
        try {{
            if (Object.prototype.hasOwnProperty.call(info, name)) return String(info[name] || "");
        }} catch (e) {{}}
        return null;
    }}

    var current = null;
    try {{
        if (typeof result !== "undefined") current = readValue(result);
    }} catch (e) {{}}
    if (current !== null) return current;

    var stored = null;
    try {{ stored = readValue(source.getLoginInfoMap()); }} catch (e) {{}}
    return stored === null ? "" : stored;
}}

function legadoHubHeaders() {{
    var headers = {{"Accept": "application/json", "Content-Type": "application/json"}};
    try {{
        var raw = source.getLoginHeader();
        var stored = typeof raw === "string" ? JSON.parse(raw || "{{}}") : raw;
        var authorization = stored && (stored.Authorization || stored.authorization);
        if (authorization) headers.Authorization = String(authorization);
    }} catch (e) {{}}
    return headers;
}}

function legadoHubRequest(path, method, body) {{
    var options = {{
        method: String(method || "GET").toUpperCase(),
        headers: legadoHubHeaders()
    }};
    if (body !== undefined && body !== null) options.body = JSON.stringify(body);
    var text = String(java.ajax(LEGADOHUB_BASE + path + "," + JSON.stringify(options)) || "").trim();
    return text ? JSON.parse(text) : {{}};
}}

function legadoHubUsername(payload) {{
    var username = payload && payload.user && payload.user.username;
    return typeof username === "string" ? username.trim() : "";
}}

function legadoHubHasLoginHeader() {{
    try {{
        var raw = source.getLoginHeader();
        var stored = typeof raw === "string" ? JSON.parse(raw || "{{}}") : raw;
        var authorization = stored && (stored.Authorization || stored.authorization);
        return !!(authorization && String(authorization).trim());
    }} catch (e) {{
        return false;
    }}
}}

function legadoHubRedeemAccessCode(code, showMessage) {{
    var payload = legadoHubRequest("/api/auth/access/redeem", "POST", {{accessCode: String(code || "")}});
    var username = legadoHubUsername(payload);
    if (!username || !payload.token) throw new Error("invalid identity");
    source.putLoginHeader(JSON.stringify({{Authorization: "Bearer " + String(payload.token)}}));
    source.putLoginInfo("{{}}");
    if (showMessage) java.toast("登录成功：" + username);
    return true;
}}

function legadoHubEnsureAuth(showMessage) {{
    if (legadoHubHasLoginHeader()) return true;
    var code = legadoHubLoginInfoValue("授权码").trim();
    if (!code) code = legadoHubBoundAccessCode();
    if (!code) {{
        if (showMessage) java.toast("请输入授权码，或使用管理员发放的专属订阅链接导入书源");
        return false;
    }}
    try {{
        return legadoHubRedeemAccessCode(code, !!showMessage);
    }} catch (e) {{
        if (showMessage) java.toast("登录失败，请检查授权码是否已重置或服务是否可用");
        return false;
    }}
}}

function legadoHubLogin() {{
    var code = legadoHubLoginInfoValue("授权码").trim();
    if (!code) code = legadoHubBoundAccessCode();
    if (!code) {{
        java.toast("请输入授权码");
        return false;
    }}
    try {{
        return legadoHubRedeemAccessCode(code, true);
    }} catch (e) {{
        java.toast("登录失败，请检查授权码或服务状态");
        return false;
    }}
}}

function login() {{
    return legadoHubLogin();
}}

function legadoHubStatus(showMessage) {{
    try {{
        if (!legadoHubHasLoginHeader()) {{
            if (legadoHubBoundAccessCode()) {{
                if (!legadoHubEnsureAuth(false)) {{
                    if (showMessage) java.toast("未登录或授权已失效");
                    return false;
                }}
            }} else {{
                if (showMessage) java.toast("未登录或授权已失效");
                return false;
            }}
        }}
        var payload = legadoHubRequest("/api/auth/access/me", "GET", null);
        var username = legadoHubUsername(payload);
        if (username) {{
            if (showMessage) java.toast("已登录：" + username);
            return true;
        }}
        source.removeLoginHeader();
        if (legadoHubBoundAccessCode() && legadoHubEnsureAuth(false)) {{
            payload = legadoHubRequest("/api/auth/access/me", "GET", null);
            username = legadoHubUsername(payload);
            if (username) {{
                if (showMessage) java.toast("已登录：" + username);
                return true;
            }}
        }}
        if (showMessage) java.toast("未登录或授权已失效");
        return false;
    }} catch (e) {{
        if (showMessage) java.toast("暂时无法检查登录状态");
        return false;
    }}
}}

function legadoHubOpenConsolePath(path, title) {{
    var code = legadoHubLoginInfoValue("授权码").trim();
    if (!code) code = legadoHubBoundAccessCode();
    var target = String(path || "/console/subscription");
    if (target.charAt(0) !== "/") target = "/" + target;
    var next = encodeURIComponent(target);
    var url = LEGADOHUB_BASE + "/api/auth/access/enter?next=" + next;
    if (code) url += "&code=" + encodeURIComponent(code);
    java.startBrowser(url, String(title || "LegadoHub"));
}}

function legadoHubOpenSubscriptions() {{
    legadoHubOpenConsolePath("/console/subscription", "订阅");
}}

function legadoHubOpenLibrary() {{
    legadoHubOpenConsolePath("/console/library", "书库");
}}

function legadoHubLogout() {{
    try {{ legadoHubRequest("/api/auth/access/logout", "POST", null); }} catch (e) {{}}
    try {{ source.removeLoginHeader(); }} catch (e) {{}}
    try {{ source.putLoginInfo("{{}}"); }} catch (e) {{}}
    try {{ cookie.removeCookie(LEGADOHUB_BASE); }} catch (e) {{}}
    java.toast("已退出登录");
    return true;
}}
"""


def _login_check_script() -> str:
    # On 401: clear stale Bearer. Bound code is re-attached on the next header
    # redeem; unbound sources stay logged-out so Reading opens the login UI.
    return """var legadoHubOriginalResponse = result;
try {
    eval(String(source.loginUrl));
    var body = String(legadoHubOriginalResponse == null ? "" : legadoHubOriginalResponse);
    var needLogin = /当前未登陆|未登陆|请登陆后使用|Unauthorized/i.test(body);
    if (needLogin) {
        try { source.removeLoginHeader(); } catch (e0) {}
        if (legadoHubBoundAccessCode()) {
            try { legadoHubEnsureAuth(false); } catch (e1) {}
        }
    } else if (!legadoHubHasLoginHeader() && legadoHubBoundAccessCode()) {
        try { legadoHubEnsureAuth(false); } catch (e2) {}
    }
} catch (e) {}
legadoHubOriginalResponse;"""


_LEGADO_E_READER_JS = r"""
function legadoHubReviewRoot(contentUrl) {
    return String(contentUrl || "").split("?")[0].replace(/\/+$/, "");
}

// Rewrite absolute Hub API URLs to the origin baked into this book source.
// Chapter/toc snapshots may still carry a LAN host from an earlier request;
// comments and content should follow the source entry (CF/public or LAN).
// Do NOT call baseUrl() here: AnalyzeRule often binds baseUrl to the chapter
// data: URL and that string shadows the jsLib helper.
function legadoHubRewriteApiUrl(absoluteUrl) {
    var value = String(absoluteUrl || "").trim();
    if (!/^https?:\/\//i.test(value)) return value;
    var configured = "";
    try {
        configured = String(legadoHubSourceBase() || "").trim().replace(/\/+$/, "");
    } catch (e) {
        configured = "";
    }
    if (!/^https?:\/\//i.test(configured)) return value.replace(/\/+$/, "");
    var pathWithQuery = value.replace(/^https?:\/\/[^\/?#]+/i, "");
    if (!pathWithQuery) pathWithQuery = "/";
    return configured + pathWithQuery;
}

function legadoHubReviewCount(item) {
    if (!item) return 0;
    var count = Number(item.commentCount || item.totalCommentCount || item.hotCommentCount || 0);
    return isFinite(count) && count > 0 ? Math.floor(count) : 0;
}

function legadoHubChapterEndReviewCount(reviews) {
    var summary = reviews && reviews.summary && typeof reviews.summary === "object" ? reviews.summary : {};
    var total = Number(summary.chapterEndCount || 0);
    if (isFinite(total) && total > 0) return Math.floor(total);
    return Math.max((reviews.chapterEnd || []).length, (reviews.chapterEndHot || []).length);
}

"""


def _reader_js_lib(base_api: str, *, access_code: str | None = None) -> str:
    base_literal = json.dumps(base_api.rstrip("/"), ensure_ascii=False)
    version_literal = json.dumps(_READER_RULE_VERSION, ensure_ascii=False)
    access_literal = json.dumps(str(access_code or ""), ensure_ascii=False)
    # baseUrl() is the historical helper; legadoHubSourceBase() is collision-safe
    # when AnalyzeRule binds the name baseUrl to a chapter data: URL.
    # LEGADOHUB_RULE_VERSION must change whenever rules change so Reading's
    # imported source body is not "same content, only title renamed".
    # LEGADOHUB_ACCESS_CODE is set only for personalized subscription links.
    # Auth helpers must live in jsLib so ruleContent java.ajax also carries Bearer.
    return (
        "var LEGADOHUB_RULE_VERSION = "
        + version_literal
        + ";\n"
        + "var LEGADOHUB_ACCESS_CODE = "
        + access_literal
        + ";\n"
        + "function legadoHubRuleVersion() { return LEGADOHUB_RULE_VERSION; }\n"
        + "function baseUrl() { return "
        + base_literal
        + "; }\n"
        + "function legadoHubSourceBase() { return "
        + base_literal
        + "; }\n"
        + _auth_runtime_js(base_api, access_code=access_code)
        + _LEGADO_E_READER_JS
    )


def _chapter_comment_url_rule() -> str:
    return (
        "@js:\n"
        "var location = String(baseUrl || '');\n"
        "var matched = /^data:contentUrl;base64,([^,]+)/i.exec(location);\n"
        "if (!matched) throw new Error('chapter comment content URL missing');\n"
        "var contentUrl = String(java.base64Decode(matched[1]) || '').trim();\n"
        "if (!/^https?:\\/\\//i.test(contentUrl)) throw new Error('invalid chapter comment content URL');\n"
        "legadoHubReviewRoot(legadoHubRewriteApiUrl(contentUrl)) + '/reviews';"
    )


def _chapter_comment_data_rule() -> str:
    return (
        "@js:\n"
        "var reviews = JSON.parse(String(result || '{}'));\n"
        "var segments = [];\n"
        "(reviews.hotParagraphReviews || []).forEach(function (item) {\n"
        "  if (!item || typeof item !== 'object') return;\n"
        "  var paragraphId = Number(item.paragraphId);\n"
        "  var paragraphIndex = Number(item.matchedParagraphIndex);\n"
        "  if (!isFinite(paragraphId) || paragraphId < 0 || !isFinite(paragraphIndex) || paragraphIndex < 0) return;\n"
        "  var total = legadoHubReviewCount(item);\n"
        "  if (total <= 0) return;\n"
        "  var hot = Array.isArray(item.topReviews) ? item.topReviews.length : Number(item.hotCommentCount || 0);\n"
        "  segments.push({\n"
        "    id: String(Math.floor(paragraphId)),\n"
        "    paragraphIndex: Math.floor(paragraphIndex),\n"
        "    paragraphCount: Math.max(1, Math.floor(Number(item.matchedParagraphCount) || 1)),\n"
        "    excerpt: String(item.matchedText || item.paragraphText || ''),\n"
        "    counts: {total: total, hot: Math.max(0, Math.floor(hot || 0))},\n"
        "    pageEligible: true,\n"
        "    actionData: {paragraphId: String(Math.floor(paragraphId))}\n"
        "  });\n"
        "});\n"
        "var chapterHot = Array.isArray(reviews.chapterEndHot) ? reviews.chapterEndHot : [];\n"
        "var chapterEnd = Array.isArray(reviews.chapterEnd) ? reviews.chapterEnd : [];\n"
        "var chapterItems = chapterHot.concat(chapterEnd);\n"
        "function cleanReviewPreview(item) {\n"
        "  return String(item && (item.content || item.Content) || '')\n"
        "    .replace(/<[^>]*>/g, ' ')\n"
        "    .replace(/\\[fn=\\d+\\]/g, '')\n"
        "    .replace(/\\s+/g, ' ')\n"
        "    .trim();\n"
        "}\n"
        "function cleanReviewUser(item) {\n"
        "  return String(item && (item.userName || item.UserName || item.nickName) || '')\n"
        "    .replace(/<[^>]*>/g, ' ')\n"
        "    .replace(/\\s+/g, ' ')\n"
        "    .trim();\n"
        "}\n"
        "var chapterPreviews = [];\n"
        "var seenChapterPreviews = {};\n"
        "chapterItems.some(function (item) {\n"
        "  var content = cleanReviewPreview(item);\n"
        "  if (!content) return false;\n"
        "  var user = cleanReviewUser(item);\n"
        "  var value = (user ? user + '：' : '') + content;\n"
        "  var key = '$' + String(item && (item.id || item.reviewId) || value);\n"
        "  if (seenChapterPreviews[key]) return false;\n"
        "  seenChapterPreviews[key] = true;\n"
        "  chapterPreviews.push(value.slice(0, 512));\n"
        "  return chapterPreviews.length >= 3;\n"
        "});\n"
        "var authorItems = Array.isArray(reviews.authorReviews) ? reviews.authorReviews : [];\n"
        "var author = null;\n"
        "authorItems.some(function (item) {\n"
        "  var content = cleanReviewPreview(item);\n"
        "  if (!content) return false;\n"
        "  var authorPreview = content.slice(0, 512);\n"
        "  author = {\n"
        "    label: cleanReviewUser(item) || '作者',\n"
        "    badge: '作家说',\n"
        "    counts: {total: 0, hot: 0},\n"
        "    actionData: null,\n"
        "    previews: [authorPreview]\n"
        "  };\n"
        "  return true;\n"
        "});\n"
        "var chapterTotal = legadoHubChapterEndReviewCount(reviews);\n"
        "JSON.stringify({\n"
        "  version: 2,\n"
        "  segments: segments,\n"
        "  author: author,\n"
        "  chapter: chapterTotal > 0 ? {\n"
        "    label: '本章说',\n"
        "    counts: {total: chapterTotal, hot: chapterHot.length},\n"
        "    actionData: {},\n"
        "    previews: chapterPreviews\n"
        "  } : null\n"
        "});"
    )


def _chapter_comment_action_rule() -> str:
    # Client executes this via source.evalJS (not AnalyzeUrl). Bindings include
    # chapter/event/result/baseUrl, but jsLib also defines function baseUrl(), so
    # prefer chapter.getAbsoluteURL() and only treat baseUrl as a string location.
    return (
        "@js:\n"
        "var rawEvent = event;\n"
        "if (rawEvent == null || rawEvent === undefined || rawEvent === '') rawEvent = result;\n"
        "if (rawEvent == null || rawEvent === undefined || rawEvent === '') rawEvent = '{}';\n"
        "var actionEvent = JSON.parse(String(rawEvent));\n"
        "var location = '';\n"
        "try {\n"
        "  if (chapter != null && chapter.getAbsoluteURL) location = String(chapter.getAbsoluteURL() || '');\n"
        "} catch (e1) {}\n"
        "if (!location) {\n"
        "  try {\n"
        "    if (chapter != null && chapter.url) location = String(chapter.url || '');\n"
        "  } catch (e2) {}\n"
        "}\n"
        "if (!location) {\n"
        "  try {\n"
        "    var baseCandidate = baseUrl;\n"
        "    if (typeof baseCandidate !== 'function') location = String(baseCandidate || '');\n"
        "  } catch (e3) {}\n"
        "}\n"
        "var contentUrl = '';\n"
        "var matched = /^data:contentUrl;base64,([^,]+)/i.exec(location);\n"
        "if (matched) {\n"
        "  contentUrl = String(java.base64Decode(matched[1]) || '').trim();\n"
        "} else {\n"
        "  var bare = String(location || '').split(/\\s*,\\s*(?=\\{)/)[0].trim();\n"
        "  if (/^https?:\\/\\//i.test(bare)) contentUrl = bare;\n"
        "}\n"
        "if (!/^https?:\\/\\//i.test(contentUrl)) throw new Error('chapter comment content URL missing');\n"
        "contentUrl = legadoHubRewriteApiUrl(contentUrl).replace(/\\/+$/, '');\n"
        "var viewRoot = contentUrl + '/reviews/view';\n"
        "var commentScope = String(actionEvent.scope || '');\n"
        "var viewUrl = '';\n"
        "var sheetTitle = '';\n"
        "if (commentScope === 'chapter') {\n"
        "  viewUrl = viewRoot + '?tab=chapter';\n"
        "  sheetTitle = '本章说';\n"
        "} else if (commentScope === 'page') {\n"
        "  var ids = [];\n"
        "  var segmentIds = actionEvent.segmentIds || [];\n"
        "  for (var i = 0; i < segmentIds.length && ids.length < 50; i++) {\n"
        "    var sid = String(segmentIds[i] || '');\n"
        "    if (/^\\d+$/.test(sid)) ids.push(sid);\n"
        "  }\n"
        "  if (!ids.length) throw new Error('page comment segment missing');\n"
        "  viewUrl = viewRoot + '?tab=paragraph&paragraphIds=' + encodeURIComponent(ids.join(','));\n"
        "  sheetTitle = '页热评';\n"
        "} else if (commentScope === 'segment') {\n"
        "  var id = String(actionEvent.segmentId || (actionEvent.segmentIds || [])[0] || '');\n"
        "  if (!/^\\d+$/.test(id)) throw new Error('segment comment id missing');\n"
        "  viewUrl = viewRoot + '?tab=paragraph&paragraphId=' + encodeURIComponent(id);\n"
        "  sheetTitle = '段评说';\n"
        "} else {\n"
        "  throw new Error('unsupported chapter comment scope');\n"
        "}\n"
        "JSON.stringify({type: 'sourceWebView', url: viewUrl, title: sheetTitle, presentation: 'bottomSheet', heightRatio: 0.78});"
    )


def _source_identity_for_base(base_api: str) -> tuple[str, str, str, bool]:
    """Return (bookSourceUrl, display name stem, group, is_lan) for this base."""
    config = load_aggregate_config()
    name = str(config.get("name") or "LegadoHub 聚合").strip() or "LegadoHub 聚合"
    group = str(config.get("group") or "聚合,LegadoHub").strip() or "聚合,LegadoHub"
    lan = is_lan_reading_base(base_api)
    if not lan:
        return _PUBLIC_BOOK_SOURCE_URL, name, group, False
    display = name if _LAN_NAME_MARK in name else f"{name}{_LAN_NAME_MARK}"
    parts = [part.strip() for part in group.split(",") if part.strip()]
    if _LAN_GROUP_MARK not in parts:
        parts.append(_LAN_GROUP_MARK)
    return _LAN_BOOK_SOURCE_URL, display, ",".join(parts), True


def _build_source(
    base_api: str | None = None,
    *,
    access_code: str | None = None,
) -> dict:
    base_api = normalize_public_base_url(base_api or get_public_base_url())
    book_source_url, name, group, is_lan = _source_identity_for_base(base_api)
    app_config = AppConfig.get()
    chapter_comment = app_config.chapter_comment
    bound = bool(str(access_code or "").strip())

    explore_url = _explore_url_rule(base_api)
    network_note = (
        "本条为内网书源（bookSourceUrl=LegadoHub-LAN），可与公网书源并存；"
        if is_lan
        else "本条为公网书源（bookSourceUrl=LegadoHub），可与内网书源并存；"
    )
    bind_note = (
        "专属书源：搜索/目录/正文自动鉴权；登录页提供「订阅 / 书库」入口。"
        if bound
        else "请使用管理员发放的专属书源链接导入。"
    )
    return {
        "bookSourceName": f"{name}({_READER_RULE_VERSION})",
        "bookSourceGroup": group,
        "bookSourceUrl": book_source_url,
        "lastUpdateTime": _reader_rule_last_update_time(app_config),
        "bookSourceType": 0,
        "enabled": True,
        "enabledCookieJar": True,
        "enabledExplore": True,
        # Header must stay free of java.ajax — login sheet evaluates it on open.
        "header": _request_header_rule(),
        "loginUi": _login_ui(bound_access_code=bound),
        "loginUrl": _login_script(base_api, access_code=access_code),
        "loginCheckJs": _login_check_script(),
        "bookSourceComment": (
            f"规则版本 {_READER_RULE_VERSION}。"
            f"{network_note}"
            f"{bind_note}"
            "搜索同时显示已发布共享书和启用的第三方书源；官方源仍只用于后台聚合，"
            "新增订阅及运维操作统一在 Web Console 完成。"
        ),
        # Progressive: page1 library + short third-party batch; page2+ continue
        # the same server job for new remotes (see subscribe._legado_search_response).
        "searchUrl": _search_url_rule(base_api),
        # Slightly above page2 short-wait (20s) so follow-up search pages can finish.
        "respondTime": 25000,
        "exploreUrl": explore_url,
        "ruleSearch": {
            "bookList": "$.items",
            "name": "$.name",
            "author": "$.author",
            "coverUrl": "$.coverUrl",
            "intro": "$.intro",
            "kind": "$.kind",
            "lastChapter": "$.readingLastChapter",
            "wordCount": "$.wordCount",
            "bookUrl": "$.bookUrl",
            "checkKeyWord": "",
        },
        "ruleExplore": {
            "bookList": "$.items",
            "name": "$.name",
            "author": "$.author",
            "coverUrl": "$.coverUrl",
            "intro": "$.intro",
            "kind": "$.kind",
            "lastChapter": "$.lastChapter",
            "wordCount": "$.wordCount",
            "bookUrl": "$.bookUrl",
        },
        "ruleBookInfo": {
            "init": "$.data",
            "name": "$.name",
            "author": "$.author",
            "coverUrl": "$.coverUrl",
            "intro": "$.intro",
            "kind": "$.kind",
            "lastChapter": "$.lastChapter",
            "wordCount": "$.wordCount",
            "updateTime": "$.updateTime",
            "tocUrl": "$.tocUrl",
            "canReName": "1",
        },
        "ruleToc": {
            "chapterList": "$.chapters",
            "chapterName": "$.title",
            "chapterUrl": (
                "<js>\n"
                "var contentUrl = String(result.chapterUrl || '');\n"
                "try { contentUrl = legadoHubRewriteApiUrl(contentUrl); } catch (e) {}\n"
                "var metadata = {type: 'legadoHub'};\n"
                "`data:contentUrl;base64,${java.base64Encode(contentUrl)},${JSON.stringify(metadata)}`;\n"
                "</js>"
            ),
            "isVip": "$.isVip",
            "isPay": "$.isPay",
            "updateTime": "$.updateTime",
        },
        "ruleContent": {
            # Must use legadoHubAjax (jsLib) so chapter fetch carries the same
            # Bearer as search/toc. Plain java.ajax(contentUrl) skipped source header.
            "content": '@js:\n'
            'var payload = String(result || "");\n'
            'var contentUrl = "";\n'
            'try {\n'
            '  contentUrl = String(java.hexDecodeToString(payload) || "").trim();\n'
            '  try { contentUrl = legadoHubRewriteApiUrl(contentUrl); } catch (e0) {}\n'
            '  if (/^https?:\\/\\//i.test(contentUrl)) {\n'
            '    try {\n'
            '      payload = String(legadoHubAjax(contentUrl) || "");\n'
            '    } catch (eAjax) {\n'
            '      payload = String(java.ajax(contentUrl) || "");\n'
            '    }\n'
            '  }\n'
            '} catch (e) {}\n'
            'var text = payload;\n'
            'var chapterPayload = null;\n'
            'try {\n'
            '  chapterPayload = JSON.parse(payload);\n'
            '  if (typeof chapterPayload.content === "string") text = chapterPayload.content;\n'
            '  else if (typeof chapterPayload.detail === "string") text = chapterPayload.detail;\n'
            '  else if (chapterPayload.detail && chapterPayload.detail.message) text = chapterPayload.detail.message;\n'
            '} catch (e) {}\n'
            'text = String(text || "").replace(/\\r\\n/g, "\\n").replace(/\\r/g, "\\n");\n'
            'result = /<(?:p|div)\\b/i.test(text) ? text : text.replace(/\\n\\n+/g, "<br><br>").replace(/\\n/g, "<br>");',
            "title": "$.title",
            "chapterComment": {
                "protocolVersion": 2,
                "url": _chapter_comment_url_rule(),
                "data": _chapter_comment_data_rule(),
                "action": _chapter_comment_action_rule(),
                "display": {
                    "segment": {
                        "enabled": chapter_comment.segment_enabled,
                        "preset": "count" if chapter_comment.segment_enabled else "none",
                        "countField": "total",
                        "label": "",
                    },
                    "page": {
                        "enabled": chapter_comment.page_enabled,
                        "preset": "pull" if chapter_comment.page_enabled else "none",
                        "countField": "total",
                        "label": "热评",
                    },
                    "chapter": {
                        "enabled": chapter_comment.chapter_enabled,
                        "preset": "summaryRow" if chapter_comment.chapter_enabled else "none",
                        "countField": "total",
                        "label": "本章说",
                    },
                },
                "cacheTtlSeconds": 300,
            },
        },
        "jsLib": _reader_js_lib(base_api, access_code=access_code),
    }


def generate_legado_source(
    base_api: str | None = None,
    *,
    access_code: str | None = None,
) -> list[dict]:
    return [_build_source(base_api, access_code=access_code)]


def write_legado_source() -> str:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    path = GENERATED_DIR / "legadohub-source.json"
    data = generate_legado_source()
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)
