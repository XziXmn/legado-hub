const API_BASE = "/api/console"

export interface ApiErrorDetail {
  code?: string
  message?: string
  retryable?: boolean
  [key: string]: unknown
}

export interface ManagedUser {
  userId: string
  username: string
  role: "admin" | "user"
  disabled: boolean
  createdAt?: string
  updatedAt?: string
}

export interface ManagedUsersResponse {
  items: ManagedUser[]
  total: number
}

export interface SubscriptionAccessLinks {
  accessCode?: string
  sourceUrl?: string
  subscriptionUrl?: string
  publicSourceUrl?: string
  publicSubscriptionUrl?: string
  lanSourceUrl?: string
  lanSubscriptionUrl?: string
  maxSourceUrl?: string
  publicMaxSourceUrl?: string
  lanMaxSourceUrl?: string
}

export interface ManagedUserCreated extends ManagedUser, SubscriptionAccessLinks {}

export interface AccessCodeIssue extends SubscriptionAccessLinks {
  userId: string
  accessCode: string
}

export interface LibraryBookProcessingSettings {
  updateIntervalMinutes: number
  backlogChapterLimit: number
}

export interface LibraryBookSettingsResponse {
  bookId: string
  settings: LibraryBookProcessingSettings
  currentPolicyVersion: number
  intervalMinutes: number
  updated?: boolean
  policyChanged?: boolean
}

export interface FirstReadableChapter {
  chapterId: string
  chapterIndex: number
  title: string
  contentAccess: "full" | "preview"
}

export interface ProvisioningSummary {
  state: "ready" | "processing" | "error" | "paused"
  readableChapterCount: number
  previewChapterCount: number
  pendingChapterCount: number
  firstReadableChapter: FirstReadableChapter | null
}

export interface SubscribeCardResponse {
  ok: boolean
  created: boolean
  sharedBookCreated: boolean
  subscriptionCreated: boolean
  book: { aggregateBookId: string; [key: string]: unknown }
  provisioning: ProvisioningSummary
  processingWakeRequested: boolean
  aggregateBookId?: string
  [key: string]: unknown
}

export type ChapterReviewTab = "chapter" | "paragraph"

export interface ChapterReviewsResponse {
  chapterEnd?: Array<Record<string, unknown>>
  chapterEndHot?: Array<Record<string, unknown>>
  authorReviews?: Array<Record<string, unknown>>
  hotParagraphReviews?: Array<{
    paragraphId?: number
    matchedText?: string
    matchedParagraphIndex?: number
    matchedParagraphCount?: number
    commentCount?: number
    totalCommentCount?: number
    hotCommentCount?: number
    [key: string]: unknown
  }>
  paragraphs?: Record<string, Array<Record<string, unknown>>>
  summary?: {
    totalReviews?: number
    chapterEndCount?: number
    [key: string]: unknown
  }
  debug?: Record<string, unknown>
}

export class ApiError extends Error {
  readonly status: number
  readonly detail: ApiErrorDetail

  constructor(
    status: number,
    detail: ApiErrorDetail,
  ) {
    super(detail.message || `请求失败（${status}）`)
    this.name = "ApiError"
    this.status = status
    this.detail = detail
  }
}

const API_STATUS_MESSAGES: Record<number, string> = {
  401: "登录状态已失效，请重新登录。",
  403: "你没有权限执行此操作。",
  404: "请求的资源不存在或已被删除。",
  429: "操作过于频繁，请稍后重试。",
  503: "服务暂时不可用，请稍后重试。",
}

export function apiErrorMessage(error: unknown, fallback = "请求失败，请稍后重试。"): string {
  if (!error) return fallback

  if (typeof error === "string") {
    return error.trim() || fallback
  }

  if (typeof error === "object") {
    const candidate = error as {
      status?: unknown
      message?: unknown
      detail?: { message?: unknown }
    }
    const backendMessage = candidate.detail?.message
    if (typeof backendMessage === "string" && backendMessage.trim()) {
      return backendMessage.trim()
    }

    const status = Number(candidate.status)
    if (Number.isInteger(status) && API_STATUS_MESSAGES[status]) {
      return API_STATUS_MESSAGES[status]
    }

    if (typeof candidate.message === "string" && candidate.message.trim()) {
      return candidate.message.trim()
    }
  }

  return fallback
}

async function fetchRaw(path: string, options?: RequestInit): Promise<Response> {
  return fetch(`${API_BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    ...options,
  })
}

async function responseError(res: Response): Promise<Error> {
  const payload = await res.json().catch(() => null)
  const detail = payload?.detail ?? payload?.message ?? payload?.error
  if (detail && typeof detail === "object") {
    return new ApiError(res.status, detail as ApiErrorDetail)
  }
  return new ApiError(res.status, {
    message: typeof detail === "string" && detail.trim() ? detail : `请求失败（${res.status}）`,
  })
}

async function fetchJson(path: string, options?: RequestInit): Promise<any> {
  const res = await fetchRaw(path, options)
  if (!res.ok) throw await responseError(res)
  if (res.status === 204) return null
  return res.json()
}

async function fetchApiRaw(path: string, options?: RequestInit): Promise<Response> {
  return fetch(`/api${path}`, {
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    ...options,
  })
}

async function fetchApiJson(path: string, options?: RequestInit): Promise<any> {
  const res = await fetchApiRaw(path, options)
  if (!res.ok) throw await responseError(res)
  if (res.status === 204) return null
  return res.json()
}

function normalizeLibraryBook(raw: any): any {
  if (!raw || typeof raw !== "object") return raw
  return {
    ...raw,
    id: raw.aggregateBookId ?? raw.id,
    displayName: raw.displayName ?? raw.name ?? raw.canonicalName ?? "",
    displayAuthor: raw.displayAuthor ?? raw.author ?? raw.canonicalAuthor ?? "",
    lastChapterTitle: raw.lastChapterTitle ?? raw.lastSourceChapterTitle ?? raw.lastLocalChapterTitle ?? "",
    lastCheckedAt: raw.lastCheckedAt ?? raw.lastCheckTime ?? "",
  }
}

function normalizeLibraryList(data: any): any {
  return {
    ...data,
    items: (data?.items || []).map(normalizeLibraryBook),
  }
}

function normalizeLibraryDetail(data: any): any {
  const book = normalizeLibraryBook(data?.book ?? data)
  return {
    ...data,
    ...book,
    book,
  }
}

function normalizeChapterList(data: any): any {
  const rawChapters = Array.isArray(data?.items) && data.items.length > 0
    ? data.items
    : Array.isArray(data?.chapters)
      ? data.chapters
      : []
  const chapters = rawChapters.map((chapter: any) => ({
    ...chapter,
    id: chapter?.chapterId ?? chapter?.id,
    readChapterId: chapter?.readChapterId ?? "",
    chapterId: chapter?.chapterId ?? chapter?.id,
  }))
  return {
    ...data,
    chapters,
    items: chapters,
  }
}

function normalizeSearchCards(data: any): any {
  return {
    ...data,
    cards: (data?.cards || []).map((card: any) => ({
      ...card,
      status: card.status ?? "unknown",
      alreadyIngested: card.alreadyIngested ?? card.alreadyInLibrary ?? false,
      sourceSummaryText: Array.isArray(card.sourceSummary)
        ? card.sourceSummary.map((source: any) => source.sourceName || source.sourceId).filter(Boolean).join(" / ")
        : card.sourceSummary ?? "",
    })),
  }
}

export const api = {
  status: (): Promise<any> => fetchJson("/status"),

  plugins: (): Promise<any> => fetchJson("/plugins"),
  batchEnablePlugins: (pluginIds: string[], enabled: boolean): Promise<any> =>
    fetchJson("/plugins/batch-enable", {
      method: "POST",
      body: JSON.stringify({ pluginIds, enabled }),
    }),
  pingAllPlugins: (pluginIds?: string[]): Promise<any> =>
    fetchJson("/plugins/ping", { method: "POST", body: JSON.stringify({ pluginIds }) }),
  pluginAuthCheck: (id: string): Promise<any> => fetchJson(`/plugins/${id}/auth/check`, { method: "POST" }),
  pluginCookiesClear: (id: string): Promise<any> => fetchJson(`/plugins/${id}/cookies/clear`, { method: "POST" }),
  startLoginBrowser: (id: string): Promise<any> => fetchJson(`/plugins/${id}/login-browser`, { method: "POST" }),
  getLoginBrowserStatus: (id: string): Promise<any> => fetchJson(`/plugins/${id}/login-browser/status`),
  cancelLoginBrowser: (id: string): Promise<any> =>
    fetchJson(`/plugins/${id}/login-browser`, { method: "DELETE" }),

  // Official source login (generic protocol)
  loginCapabilities: (pluginId: string): Promise<any> =>
    fetchJson(`/official-sources/${pluginId}/login-capabilities`),
  loginPhoneRequestCode: (pluginId: string, payload: Record<string, any>): Promise<any> =>
    fetchJson(`/official-sources/${pluginId}/login/phone/request-code`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  loginPhoneVerify: (pluginId: string, payload: Record<string, any>): Promise<any> =>
    fetchJson(`/official-sources/${pluginId}/login/phone/verify`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  loginCookieVerify: (pluginId: string, payload: Record<string, any>): Promise<any> =>
    fetchJson(`/official-sources/${pluginId}/login/cookie/verify`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  loginLogout: (pluginId: string): Promise<any> =>
    fetchJson(`/official-sources/${pluginId}/login/logout`, { method: "POST" }),

  officialSources: (): Promise<any> => fetchJson("/official-sources"),
  createSearchJob: (payload: {
    keyword: string
    page?: number
    limit?: number
    sourceIds?: string[]
  }): Promise<any> =>
    fetchJson("/search-jobs", {
      method: "POST",
      body: JSON.stringify({
        keyword: payload.keyword,
        page: payload.page || 1,
        limit: payload.limit,
        sourceIds: payload.sourceIds,
      }),
    }),
  searchJob: (id: string): Promise<any> => fetchJson(`/search-jobs/${id}`),
  searchJobEvents: (id: string, after?: number): Promise<any> =>
    fetchJson(`/search-jobs/${id}/events?after=${after || 0}`),
  settings: (): Promise<any> => fetchJson("/settings"),
  updateSettings: (payload: Record<string, any>): Promise<any> =>
    fetchJson("/settings", { method: "POST", body: JSON.stringify(payload) }),

  // Subscription content workflow settings.
  aggregateSettings: (): Promise<any> => fetchJson("/aggregate-settings"),
  updateAggregateSettings: (payload: Record<string, any>): Promise<any> =>
    fetchJson("/aggregate-settings", { method: "POST", body: JSON.stringify(payload) }),

  // Auth (session cookie based)
  auth: {
    entrypoint: (): Promise<any> => fetchApiJson("/auth/entrypoint"),
    me: (): Promise<any> => fetchApiJson("/auth/me"),
    login: (payload: { username: string; password: string }): Promise<any> =>
      fetchApiJson("/auth/login", { method: "POST", body: JSON.stringify(payload) }),
    redeemAccessCode: (accessCode: string): Promise<any> =>
      fetchApiJson("/auth/access/redeem", {
        method: "POST",
        body: JSON.stringify({ accessCode }),
      }),
    changePassword: (payload: { currentPassword: string; newPassword: string }): Promise<any> =>
      fetchApiJson("/auth/change-password", { method: "POST", body: JSON.stringify(payload) }),
    logout: (): Promise<any> => fetchApiJson("/auth/logout", { method: "POST" }),
  },

  users: {
    list: (): Promise<ManagedUsersResponse> => fetchJson("/users"),
    create: (
      payload:
        | { username: string; role: "user" }
        | { username: string; password: string; role: "admin" },
    ): Promise<ManagedUserCreated> =>
      fetchJson("/users", { method: "POST", body: JSON.stringify(payload) }),
    resetPassword: (userId: string, password: string): Promise<{ userId: string; passwordReset: boolean }> =>
      fetchJson(`/users/${userId}/reset-password`, {
        method: "POST",
        body: JSON.stringify({ password }),
      }),
    resetAccessCode: (userId: string): Promise<AccessCodeIssue> =>
      fetchJson(`/users/${userId}/reset-access-code`, {
        method: "POST",
        body: JSON.stringify({}),
      }),
    accessLinks: (userId: string): Promise<AccessCodeIssue> =>
      fetchJson(`/users/${userId}/links`),
    revokeSessions: (userId: string): Promise<{ userId: string; revokedSessions: number }> =>
      fetchJson(`/users/${userId}/revoke-sessions`, {
        method: "POST",
        body: JSON.stringify({}),
      }),
    setDisabled: (userId: string, disabled: boolean): Promise<{ userId: string; disabled: boolean }> =>
      fetchJson(`/users/${userId}/disable`, {
        method: "POST",
        body: JSON.stringify({ disabled }),
      }),
    deleteUser: (userId: string): Promise<{
      userId: string
      deleted: boolean
      revokedSessions: number
      deletedSubscriptions: number
      deletedSearchJobs: number
    }> => fetchJson(`/users/${userId}`, { method: "DELETE" }),
  },

  // Subscription discovery (shared library)
  subscribe: {
    search: (payload: { keyword: string; page?: number }): Promise<any> =>
      fetchApiJson("/subscribe/search", { method: "POST", body: JSON.stringify(payload) }).then(normalizeSearchCards),
    searchJob: (jobId: string): Promise<any> =>
      fetchApiJson(`/subscribe/search/${jobId}`).then(normalizeSearchCards),
    subscribeCard: (
      jobId: string,
      candidateId: string,
      payload: { startChapterIndex?: number; autoArchiveOnComplete?: boolean }
    ): Promise<SubscribeCardResponse> =>
      fetchApiJson(`/subscribe/search/${jobId}/cards/${candidateId}/subscribe`, {
        method: "POST",
        body: JSON.stringify(payload),
      }),
    myLibrary: (keyword = ""): Promise<any> => {
      const qs = keyword ? `?${new URLSearchParams({ keyword })}` : ""
      return fetchApiJson(`/subscribe/library/mine${qs}`).then(normalizeLibraryList)
    },
    book: (bookId: string): Promise<any> =>
      fetchApiJson(`/subscribe/books/${bookId}`).then(normalizeLibraryDetail),
    chapters: (bookId: string, params?: Record<string, string>): Promise<any> => {
      const qs = params ? `?${new URLSearchParams(params)}` : ""
      return fetchApiJson(`/subscribe/books/${bookId}/chapters${qs}`).then(normalizeChapterList)
    },
    subscription: (bookId: string): Promise<any> =>
      fetchApiJson(`/subscribe/books/${bookId}/subscription`),
    updateSubscription: (
      bookId: string,
      payload: { status?: "active" | "paused" | "archived"; startChapterIndex?: number; autoArchiveOnComplete?: boolean },
    ): Promise<any> =>
      fetchApiJson(`/subscribe/books/${bookId}/subscription`, {
        method: "PATCH",
        body: JSON.stringify(payload),
      }),
    removeSubscription: (bookId: string): Promise<any> =>
      fetchApiJson(`/subscribe/books/${bookId}/subscription`, { method: "DELETE" }),
    chapter: (chapterId: string): Promise<any> =>
      fetchApiJson(`/subscribe/chapters/${encodeURIComponent(chapterId)}`),
  },

  reading: {
    chapterReviews: (chapterId: string): Promise<ChapterReviewsResponse> =>
      fetchApiJson(`/legado/chapter/${encodeURIComponent(chapterId)}/reviews`),
    chapterReviewsViewUrl: (
      chapterId: string,
      tab: ChapterReviewTab = "chapter",
      paragraphIds: number[] = [],
    ): string => {
      const params = new URLSearchParams({ tab })
      if (paragraphIds.length > 0) params.set("paragraphIds", paragraphIds.join(","))
      return `/api/legado/chapter/${encodeURIComponent(chapterId)}/reviews/view?${params}`
    },
  },

  // Admin shared library management
  libraryBooks: (params?: Record<string, string>): Promise<any> => {
    const qs = params ? `?${new URLSearchParams(params)}` : ""
    return fetchJson(`/library-books${qs}`).then(normalizeLibraryList)
  },
  libraryIntegrity: (): Promise<any> => fetchJson("/library-integrity"),
  repairLibraryIntegrity: (bookIds?: string[]): Promise<any> =>
    fetchJson("/library-integrity/repair", {
      method: "POST",
      body: JSON.stringify(bookIds?.length ? { bookIds } : {}),
    }),
  libraryBookSummary: (bookId: string): Promise<any> =>
    fetchJson(`/library-books/${bookId}`).then(normalizeLibraryDetail),
  libraryBookSettings: (bookId: string): Promise<LibraryBookSettingsResponse> =>
    fetchJson(`/library-books/${bookId}/settings`),
  updateLibraryBookSettings: (
    bookId: string,
    payload: LibraryBookProcessingSettings,
  ): Promise<LibraryBookSettingsResponse> =>
    fetchJson(`/library-books/${bookId}/settings`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  libraryBookChapters: (bookId: string, params?: Record<string, string>): Promise<any> => {
    const qs = params ? `?${new URLSearchParams(params)}` : ""
    return fetchJson(`/library-books/${bookId}/chapters${qs}`).then(normalizeChapterList)
  },
  libraryBookChapterProgress: (bookId: string, chapterId: string): Promise<any> =>
    fetchJson(`/library-books/${bookId}/chapters/${chapterId}/progress`),
  processLibraryBookChapter: (bookId: string, chapterId: string): Promise<any> =>
    fetchJson(`/library-books/${bookId}/chapters/${chapterId}/process`, { method: "POST" }),
  scanManualChapterCandidates: (bookId: string, chapterId: string): Promise<any> =>
    fetchJson(`/library-books/${bookId}/chapters/${chapterId}/manual-candidates/scan`, { method: "POST" }),
  applyManualChapterCandidate: (
    bookId: string,
    chapterId: string,
    payload: { sourceId: string; sourceChapterId: string },
  ): Promise<any> =>
    fetchJson(`/library-books/${bookId}/chapters/${chapterId}/manual-candidates/apply`, {
      method: "POST",
      body: JSON.stringify(payload),
    }),
  chapter: (chapterId: string): Promise<any> =>
    fetchJson(`/chapter/${encodeURIComponent(chapterId)}`),
  pauseLibraryBook: (bookId: string): Promise<any> =>
    fetchJson(`/library-books/${bookId}/pause`, { method: "POST" }),
  resumeLibraryBook: (bookId: string): Promise<any> =>
    fetchJson(`/library-books/${bookId}/resume`, { method: "POST" }),
  archiveLibraryBook: (bookId: string): Promise<any> =>
    fetchJson(`/library-books/${bookId}/archive`, { method: "POST" }),
  deleteLibraryBook: (bookId: string): Promise<any> =>
    fetchJson(`/library-books/${bookId}`, { method: "DELETE" }),
  rebuildLibraryBook: (bookId: string): Promise<any> =>
    fetchJson(`/library-books/${bookId}/rebuild`, { method: "POST" }),
  refreshLibraryBookSources: (bookId: string, payload?: Record<string, any>): Promise<any> =>
    fetchJson(`/library-books/${bookId}/source-map/refresh`, {
      method: "POST",
      body: JSON.stringify(payload || {}),
    }),
  repairLibraryBook: (bookId: string, payload?: Record<string, any>): Promise<any> =>
    fetchJson(`/library-books/${bookId}/repair`, {
      method: "POST",
      body: JSON.stringify(payload || {}),
    }),
  checkLibraryBookUpdate: (bookId: string): Promise<any> =>
    fetchJson(`/library-books/${bookId}/update-check`, { method: "POST" }),
  streamLibraryBookLogsUrl: (bookId: string): string =>
    `${window.location.origin}${API_BASE}/library-books/${bookId}/logs/stream`,

  // Sensitive-word lexicon
  lexiconStatus: (): Promise<any> => fetchJson("/lexicon/status"),
  updateLexicon: (): Promise<any> =>
    fetchJson("/lexicon/update", { method: "POST" }),

}
