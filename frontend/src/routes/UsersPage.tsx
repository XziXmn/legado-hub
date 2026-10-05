import { useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  Check,
  Copy,
  KeyRound,
  Loader2,
  LogOut,
  Plus,
  RefreshCw,
  Trash2,
  UserCheck,
  Users,
  UserX,
  Link2,
} from "lucide-react"
import { api, apiErrorMessage, type ManagedUser } from "@/lib/api"
import { useAuth } from "@/lib/auth"
import { copyTextToClipboard } from "@/lib/utils"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"

interface IssuedAccessCode {
  username: string
  accessCode: string
  sourceUrl?: string
  subscriptionUrl?: string
  publicSourceUrl?: string
  publicSubscriptionUrl?: string
  lanSourceUrl?: string
  lanSubscriptionUrl?: string
  maxSourceUrl?: string
  publicMaxSourceUrl?: string
  lanMaxSourceUrl?: string
  /** "reveal" = re-opened from the user list; copy stays available anytime. */
  mode?: "issue" | "reveal"
}

function formatDate(value?: string) {
  if (!value) return "-"
  try {
    return new Date(value).toLocaleString("zh-CN")
  } catch {
    return value
  }
}

/** Prefer public/default source; show LAN only when it differs. */
function issuedSourceLinks(issued: IssuedAccessCode) {
  const primary =
    issued.sourceUrl ||
    issued.publicSourceUrl ||
    issued.lanSourceUrl ||
    ""
  const lan = issued.lanSourceUrl || ""
  const showLan = !!lan && lan !== primary && lan !== (issued.publicSourceUrl || "")
  const maxPrimary =
    issued.maxSourceUrl ||
    issued.publicMaxSourceUrl ||
    issued.lanMaxSourceUrl ||
    ""
  const maxLan = issued.lanMaxSourceUrl || ""
  const showMaxLan = !!maxLan && maxLan !== maxPrimary && maxLan !== (issued.publicMaxSourceUrl || "")
  return { primary, lan: showLan ? lan : "", maxPrimary, maxLan: showMaxLan ? maxLan : "" }
}

function CopyIconButton({
  copied,
  label,
  onCopy,
}: {
  copied: boolean
  label: string
  onCopy: () => void
}) {
  return (
    <Button
      type="button"
      variant="outline"
      size="icon"
      className="h-9 w-9 shrink-0"
      title={copied ? "已复制" : label}
      aria-label={copied ? "已复制" : label}
      onClick={(event) => {
        event.preventDefault()
        event.stopPropagation()
        onCopy()
      }}
    >
      {copied ? <Check className="h-4 w-4 text-emerald-600" /> : <Copy className="h-4 w-4" />}
    </Button>
  )
}

export function UsersPage() {
  const queryClient = useQueryClient()
  const { user: currentUser } = useAuth()
  const [createOpen, setCreateOpen] = useState(false)
  const [username, setUsername] = useState("")
  const [password, setPassword] = useState("")
  const [role, setRole] = useState<"admin" | "user">("user")
  const [creating, setCreating] = useState(false)
  const [createError, setCreateError] = useState("")
  const [resetTarget, setResetTarget] = useState<ManagedUser | null>(null)
  const [resetPassword, setResetPassword] = useState("")
  const [resetting, setResetting] = useState(false)
  const [resetError, setResetError] = useState("")
  const [issuedAccessCode, setIssuedAccessCode] = useState<IssuedAccessCode | null>(null)
  const [copiedKey, setCopiedKey] = useState<string>("")
  const [copyError, setCopyError] = useState("")
  const [actionNotice, setActionNotice] = useState("")

  const usersQuery = useQuery({ queryKey: ["users"], queryFn: api.users.list })
  const refreshUsers = () => queryClient.invalidateQueries({ queryKey: ["users"] })
  const statusMutation = useMutation({
    mutationFn: ({ userId, disabled }: { userId: string; disabled: boolean }) =>
      api.users.setDisabled(userId, disabled),
    onSuccess: async () => {
      setActionNotice("")
      await refreshUsers()
    },
  })
  const revokeMutation = useMutation({
    mutationFn: ({ userId }: { userId: string }) => api.users.revokeSessions(userId),
    onSuccess: async (result, variables) => {
      const target = usersQuery.data?.items.find((item) => item.userId === variables.userId)
      setActionNotice(`已撤销 ${target?.username || "用户"} 的 ${result.revokedSessions} 个登录会话。`)
      await refreshUsers()
    },
  })
  const deleteMutation = useMutation({
    mutationFn: ({ userId }: { userId: string }) => api.users.deleteUser(userId),
    onSuccess: async (_result, variables) => {
      const target = usersQuery.data?.items.find((item) => item.userId === variables.userId)
      setActionNotice(`已删除普通用户 ${target?.username || "用户"}。`)
      await refreshUsers()
    },
  })

  const openCreateDialog = () => {
    setUsername("")
    setPassword("")
    setRole("user")
    setCreateError("")
    setCreateOpen(true)
  }
  const closeCreateDialog = () => {
    setCreateOpen(false)
    setUsername("")
    setPassword("")
    setCreateError("")
  }
  const closeResetDialog = () => {
    setResetTarget(null)
    setResetPassword("")
    setResetError("")
  }
  const closeIssuedDialog = () => {
    setIssuedAccessCode(null)
    setCopiedKey("")
    setCopyError("")
  }

  const [linksLoadingFor, setLinksLoadingFor] = useState("")
  const [linksError, setLinksError] = useState("")
  const [linksActionTarget, setLinksActionTarget] = useState<ManagedUser | null>(null)

  const openUserLinks = async (item: ManagedUser) => {
    setLinksError("")
    setLinksLoadingFor(item.userId)
    try {
      const result = await api.users.accessLinks(item.userId)
      setIssuedAccessCode({
        username: item.username,
        accessCode: result.accessCode,
        sourceUrl: result.sourceUrl,
        subscriptionUrl: result.subscriptionUrl,
        publicSourceUrl: result.publicSourceUrl,
        publicSubscriptionUrl: result.publicSubscriptionUrl,
        lanSourceUrl: result.lanSourceUrl,
        lanSubscriptionUrl: result.lanSubscriptionUrl,
        maxSourceUrl: result.maxSourceUrl,
        publicMaxSourceUrl: result.publicMaxSourceUrl,
        lanMaxSourceUrl: result.lanMaxSourceUrl,
        mode: "reveal",
      })
    } catch (error) {
      setLinksError(apiErrorMessage(error, "书源链接获取失败。"))
    } finally {
      setLinksLoadingFor("")
    }
  }

  const submitCreate = async (event: React.FormEvent) => {
    event.preventDefault()
    setCreateError("")
    setCreating(true)
    try {
      const result = role === "user"
        ? await api.users.create({ username: username.trim(), role: "user" })
        : await api.users.create({ username: username.trim(), password, role: "admin" })
      if (role === "user") {
        if (!result.accessCode) throw new Error("服务端未返回授权码")
        setIssuedAccessCode({
          username: result.username,
          accessCode: result.accessCode,
          sourceUrl: result.sourceUrl,
          subscriptionUrl: result.subscriptionUrl,
          publicSourceUrl: result.publicSourceUrl,
          publicSubscriptionUrl: result.publicSubscriptionUrl,
          lanSourceUrl: result.lanSourceUrl,
          lanSubscriptionUrl: result.lanSubscriptionUrl,
          maxSourceUrl: result.maxSourceUrl,
          publicMaxSourceUrl: result.publicMaxSourceUrl,
          lanMaxSourceUrl: result.lanMaxSourceUrl,
          mode: "issue",
        })
      }
      closeCreateDialog()
      await refreshUsers()
    } catch (error) {
      setCreateError(apiErrorMessage(error, "用户创建失败。"))
    } finally {
      setCreating(false)
    }
  }

  const submitReset = async (event: React.FormEvent) => {
    event.preventDefault()
    if (!resetTarget) return
    setResetError("")
    setResetting(true)
    try {
      if (resetTarget.role === "user") {
        const result = await api.users.resetAccessCode(resetTarget.userId)
        if (!result.accessCode) throw new Error("服务端未返回授权码")
        setIssuedAccessCode({
          username: resetTarget.username,
          accessCode: result.accessCode,
          sourceUrl: result.sourceUrl,
          subscriptionUrl: result.subscriptionUrl,
          publicSourceUrl: result.publicSourceUrl,
          publicSubscriptionUrl: result.publicSubscriptionUrl,
          lanSourceUrl: result.lanSourceUrl,
          lanSubscriptionUrl: result.lanSubscriptionUrl,
          maxSourceUrl: result.maxSourceUrl,
          publicMaxSourceUrl: result.publicMaxSourceUrl,
          lanMaxSourceUrl: result.lanMaxSourceUrl,
          mode: "issue",
        })
      } else {
        await api.users.resetPassword(resetTarget.userId, resetPassword)
      }
      closeResetDialog()
      await refreshUsers()
    } catch (error) {
      setResetError(apiErrorMessage(error, resetTarget.role === "user" ? "授权码重置失败。" : "密码重置失败。"))
    } finally {
      setResetting(false)
    }
  }

  const copyText = (key: string, value?: string) => {
    const text = String(value ?? "").trim()
    if (!text) {
      setCopyError("没有可复制的内容。")
      return
    }
    void (async () => {
      try {
        await copyTextToClipboard(text)
        setCopiedKey(key)
        setCopyError("")
        window.setTimeout(() => {
          setCopiedKey((current) => (current === key ? "" : current))
        }, 2000)
      } catch {
        setCopiedKey("")
        setCopyError("复制失败：请长按选中文本手动复制。")
      }
    })()
  }

  const users = usersQuery.data?.items || []
  const enabledCount = users.filter((item) => !item.disabled).length
  const adminCount = users.filter((item) => item.role === "admin" && !item.disabled).length
  const pageError = usersQuery.error || statusMutation.error || revokeMutation.error || deleteMutation.error

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight text-slate-900">用户管理</h1>
          <p className="mt-1 text-sm text-slate-500">普通用户发专属书源链接即可；重新生成后旧链接失效。管理员使用密码。</p>
        </div>
        <Button onClick={openCreateDialog}><Plus className="mr-2 h-4 w-4" />新建用户</Button>
      </div>

      <div className="grid grid-cols-2 gap-4 sm:grid-cols-3">
        <Card className="shadow-none"><CardContent className="p-4"><div className="text-xs font-medium text-slate-500">用户总数</div><div className="mt-2 text-2xl font-semibold tabular-nums text-slate-900">{users.length}</div></CardContent></Card>
        <Card className="shadow-none"><CardContent className="p-4"><div className="text-xs font-medium text-slate-500">可用账户</div><div className="mt-2 text-2xl font-semibold tabular-nums text-emerald-700">{enabledCount}</div></CardContent></Card>
        <Card className="col-span-2 shadow-none sm:col-span-1"><CardContent className="p-4"><div className="text-xs font-medium text-slate-500">可用管理员</div><div className="mt-2 text-2xl font-semibold tabular-nums text-blue-700">{adminCount}</div></CardContent></Card>
      </div>

      {pageError && (
        <Alert variant="destructive">
          <AlertDescription className="flex items-center justify-between gap-3">
            <span>{apiErrorMessage(pageError, "用户数据加载失败。")}</span>
            {usersQuery.error && <Button variant="outline" size="sm" onClick={() => usersQuery.refetch()}><RefreshCw className="mr-2 h-4 w-4" />重试</Button>}
          </AlertDescription>
        </Alert>
      )}
      {actionNotice && <Alert><AlertDescription>{actionNotice}</AlertDescription></Alert>}

      {linksError && (
        <Alert variant="destructive" className="mb-3">
          <AlertDescription>{linksError}</AlertDescription>
        </Alert>
      )}
      <Card className="border-slate-200 shadow-sm">
        <CardContent className="p-0">
          <div className="overflow-x-auto">
            <Table className="min-w-[780px]">
              <TableHeader className="bg-slate-50">
                <TableRow>
                  <TableHead>用户名</TableHead><TableHead>角色</TableHead><TableHead>状态</TableHead>
                  <TableHead>创建时间</TableHead><TableHead>最后更新</TableHead><TableHead className="text-right">操作</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {usersQuery.isLoading ? (
                  <TableRow><TableCell colSpan={6} className="h-28 text-center text-slate-500"><Loader2 className="mr-2 inline h-4 w-4 animate-spin" />加载中</TableCell></TableRow>
                ) : users.length === 0 ? (
                  <TableRow><TableCell colSpan={6} className="h-28 text-center text-slate-500"><Users className="mr-2 inline h-4 w-4" />暂无用户</TableCell></TableRow>
                ) : users.map((item) => {
                  const isCurrentUser = item.userId === currentUser?.userId
                  const statusPending = statusMutation.isPending && statusMutation.variables?.userId === item.userId
                  const revokePending = revokeMutation.isPending && revokeMutation.variables?.userId === item.userId
                  const deletePending = deleteMutation.isPending && deleteMutation.variables?.userId === item.userId
                  const resetLabel = item.role === "user" ? `重置 ${item.username} 的授权码` : `重置 ${item.username} 的密码`
                  return (
                    <TableRow key={item.userId} className={item.disabled ? "bg-slate-50/60 text-slate-500" : ""}>
                      <TableCell className="font-medium text-slate-900">{item.username}{isCurrentUser && <span className="ml-2 text-xs font-normal text-slate-400">当前账户</span>}</TableCell>
                      <TableCell><Badge variant="outline">{item.role === "admin" ? "管理员" : "普通用户"}</Badge></TableCell>
                      <TableCell><Badge className={item.disabled ? "bg-slate-100 text-slate-600" : "bg-emerald-50 text-emerald-700"}>{item.disabled ? "已禁用" : "可用"}</Badge></TableCell>
                      <TableCell className="whitespace-nowrap text-slate-500">{formatDate(item.createdAt)}</TableCell>
                      <TableCell className="whitespace-nowrap text-slate-500">{formatDate(item.updatedAt)}</TableCell>
                      <TableCell>
                        <div className="flex justify-end gap-1">
                          {item.role === "user" ? (
                            <Button
                              variant="outline"
                              size="icon"
                              aria-label={`打开 ${item.username} 的书源链接操作`}
                              title="查看 / 重新生成书源链接"
                              disabled={linksLoadingFor === item.userId || deletePending}
                              onClick={() => setLinksActionTarget(item)}
                            ><KeyRound className="h-4 w-4" /></Button>
                          ) : (
                            !isCurrentUser && <Button variant="outline" size="icon" aria-label={resetLabel} title={resetLabel} disabled={deletePending} onClick={() => { setResetPassword(""); setResetError(""); setResetTarget(item) }}><KeyRound className="h-4 w-4" /></Button>
                          )}
                          <Button
                            variant="outline" size="icon" aria-label={`撤销 ${item.username} 的登录会话`} title={isCurrentUser ? "当前会话请使用退出登录" : "撤销全部登录会话"}
                            disabled={isCurrentUser || revokePending || deletePending}
                            onClick={() => { if (window.confirm(`确定撤销“${item.username}”的全部登录会话吗？`)) revokeMutation.mutate({ userId: item.userId }) }}
                          >{revokePending ? <Loader2 className="h-4 w-4 animate-spin" /> : <LogOut className="h-4 w-4" />}</Button>
                          <Button
                            variant="outline" size="icon" disabled={isCurrentUser || statusPending || deletePending}
                            aria-label={item.disabled ? "启用" : "禁用"}
                            title={isCurrentUser ? "不能禁用当前账户" : item.disabled ? "启用账户" : "禁用账户"}
                            onClick={() => { const disabled = !item.disabled; if (!disabled || window.confirm(`确定禁用用户“${item.username}”吗？`)) statusMutation.mutate({ userId: item.userId, disabled }) }}
                          >
                            {statusPending ? <Loader2 className="h-4 w-4 animate-spin" /> : item.disabled ? <UserCheck className="h-4 w-4" /> : <UserX className="h-4 w-4" />}
                          </Button>
                          {item.role === "user" && (
                            <Button
                              variant="outline"
                              size="icon"
                              className="text-rose-600 hover:bg-rose-50 hover:text-rose-700"
                              aria-label={`删除 ${item.username}`}
                              title="删除普通用户"
                              disabled={deletePending}
                              onClick={() => {
                                if (window.confirm(`确定删除普通用户“${item.username}”吗？其登录会话和个人订阅将一并删除，此操作无法撤销。`)) {
                                  deleteMutation.mutate({ userId: item.userId })
                                }
                              }}
                            >
                              {deletePending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                            </Button>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
          </div>
        </CardContent>
      </Card>

      <Dialog open={createOpen} onOpenChange={(open) => { if (!open) closeCreateDialog() }}>
        <DialogContent className="max-w-md">
          <DialogHeader><DialogTitle>新建用户</DialogTitle><DialogDescription>{role === "user" ? "系统将生成个人授权码。" : "管理员使用用户名和密码登录。"}</DialogDescription></DialogHeader>
          <form className="space-y-4" onSubmit={submitCreate}>
            <div className="space-y-2"><Label htmlFor="managed-user-name">用户名</Label><Input id="managed-user-name" autoComplete="off" maxLength={64} value={username} onChange={(event) => setUsername(event.target.value)} /></div>
            <div className="space-y-2">
              <Label htmlFor="managed-user-role">角色</Label>
              <select id="managed-user-role" value={role} onChange={(event) => { setRole(event.target.value as "admin" | "user"); setPassword("") }} className="h-10 w-full rounded-md border border-slate-200 bg-white px-3 text-sm">
                <option value="user">普通用户</option><option value="admin">管理员</option>
              </select>
            </div>
            {role === "admin" && <div className="space-y-2"><Label htmlFor="managed-user-password">初始密码</Label><Input id="managed-user-password" type="password" autoComplete="new-password" minLength={8} maxLength={128} value={password} onChange={(event) => setPassword(event.target.value)} /></div>}
            {createError && <Alert variant="destructive"><AlertDescription>{createError}</AlertDescription></Alert>}
            <div className="flex justify-end gap-2"><Button type="button" variant="outline" onClick={closeCreateDialog}>取消</Button><Button type="submit" disabled={creating || !username.trim() || (role === "admin" && password.length < 8)}>{creating && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}创建</Button></div>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={!!resetTarget} onOpenChange={(open) => { if (!open) closeResetDialog() }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>{resetTarget?.role === "user" ? "重新生成订阅凭证" : "重置密码"}</DialogTitle>
            <DialogDescription>
              {resetTarget?.role === "user"
                ? `将为 ${resetTarget.username} 生成新授权码与专属订阅链接；旧码、旧链接与现有登录会话立即失效。`
                : resetTarget?.username || "管理员"}
            </DialogDescription>
          </DialogHeader>
          <form className="space-y-4" onSubmit={submitReset}>
            {resetTarget?.role === "admin" && <div className="space-y-2"><Label htmlFor="managed-user-reset-password">新密码</Label><Input id="managed-user-reset-password" type="password" autoComplete="new-password" minLength={8} maxLength={128} value={resetPassword} onChange={(event) => setResetPassword(event.target.value)} /></div>}
            {resetError && <Alert variant="destructive"><AlertDescription>{resetError}</AlertDescription></Alert>}
            <div className="flex justify-end gap-2"><Button type="button" variant="outline" onClick={closeResetDialog}>取消</Button><Button type="submit" disabled={resetting || (resetTarget?.role === "admin" && resetPassword.length < 8)}>{resetting && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}{resetTarget?.role === "user" ? "生成新链接" : "重置密码"}</Button></div>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={!!linksActionTarget} onOpenChange={(open) => { if (!open) setLinksActionTarget(null) }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>书源链接操作</DialogTitle>
            <DialogDescription>
              查看 {linksActionTarget?.username} 当前生效的书源链接，或重新生成授权码；重新生成后旧链接与现有登录会话立即失效。
            </DialogDescription>
          </DialogHeader>
          <div className="flex justify-end gap-2">
            <Button type="button" variant="outline" onClick={() => setLinksActionTarget(null)}>取消</Button>
            <Button
              type="button"
              variant="outline"
              disabled={linksLoadingFor === linksActionTarget?.userId}
              onClick={() => { const target = linksActionTarget; setLinksActionTarget(null); if (target) void openUserLinks(target) }}
            ><Link2 className="mr-2 h-4 w-4" />查看书源链接</Button>
            <Button
              type="button"
              onClick={() => { const target = linksActionTarget; setLinksActionTarget(null); if (target) { setResetPassword(""); setResetError(""); setResetTarget(target) } }}
            ><KeyRound className="mr-2 h-4 w-4" />重新生成授权码</Button>
          </div>
        </DialogContent>
      </Dialog>

      <Dialog open={!!issuedAccessCode} onOpenChange={(open) => { if (!open) closeIssuedDialog() }}>
        <DialogContent className="max-w-md">
          <DialogHeader>
            <DialogTitle>发给用户</DialogTitle>
            <DialogDescription>
              {issuedAccessCode?.username || "用户"} · {issuedAccessCode?.mode === "reveal" ? "当前生效的书源链接，可随时回到本页查看" : "复制书源链接即可在 Reading 导入，之后可在用户列表随时查看"}
            </DialogDescription>
          </DialogHeader>
          {issuedAccessCode && (() => {
            const { primary, lan, maxPrimary, maxLan } = issuedSourceLinks(issuedAccessCode)
            return (
              <div className="space-y-4">
                {primary ? (
                  <div className="space-y-2">
                    <div className="text-sm font-medium text-slate-800">书源链接</div>
                    <div className="flex min-w-0 items-start gap-2 rounded-md border border-slate-200 bg-slate-50 p-3">
                      <code className="min-w-0 flex-1 select-all break-all text-xs leading-relaxed text-slate-800">{primary}</code>
                      <CopyIconButton
                        copied={copiedKey === "source"}
                        label="复制书源链接"
                        onCopy={() => copyText("source", primary)}
                      />
                    </div>
                    {lan ? (
                      <div className="space-y-1">
                        <div className="text-xs text-slate-500">内网（可选）</div>
                        <div className="flex min-w-0 items-start gap-2 rounded-md border border-slate-100 bg-white p-2">
                          <code className="min-w-0 flex-1 select-all break-all text-[11px] text-slate-600">{lan}</code>
                          <CopyIconButton
                            copied={copiedKey === "lanSource"}
                            label="复制内网书源"
                            onCopy={() => copyText("lanSource", lan)}
                          />
                        </div>
                      </div>
                    ) : null}
                  </div>
                ) : (
                  <Alert>
                    <AlertDescription>
                      尚未生成公网书源链接。可在「设置 → 阅读 → 公网书源地址」填写后重新生成，或使用内网链接 / 下方授权码。
                    </AlertDescription>
                  </Alert>
                )}

                {maxPrimary ? (
                  <div className="space-y-2 border-t border-slate-100 pt-3">
                    <div className="text-sm font-medium text-slate-800">
                      Legado Max 书源链接
                      <span className="ml-2 text-xs font-normal text-slate-500">正文内嵌段评气泡与本章说卡片</span>
                    </div>
                    <div className="flex min-w-0 items-start gap-2 rounded-md border border-slate-200 bg-slate-50 p-3">
                      <code className="min-w-0 flex-1 select-all break-all text-xs leading-relaxed text-slate-800">{maxPrimary}</code>
                      <CopyIconButton
                        copied={copiedKey === "maxSource"}
                        label="复制 Legado Max 书源链接"
                        onCopy={() => copyText("maxSource", maxPrimary)}
                      />
                    </div>
                    {maxLan ? (
                      <div className="space-y-1">
                        <div className="text-xs text-slate-500">内网（可选）</div>
                        <div className="flex min-w-0 items-start gap-2 rounded-md border border-slate-100 bg-white p-2">
                          <code className="min-w-0 flex-1 select-all break-all text-[11px] text-slate-600">{maxLan}</code>
                          <CopyIconButton
                            copied={copiedKey === "maxLanSource"}
                            label="复制内网 Legado Max 书源"
                            onCopy={() => copyText("maxLanSource", maxLan)}
                          />
                        </div>
                      </div>
                    ) : null}
                  </div>
                ) : null}

                <div className="space-y-2 border-t border-slate-100 pt-3">
                  <div className="text-xs text-slate-500">授权码（备用）</div>
                  <div className="flex min-w-0 items-center gap-2 rounded-md border border-slate-100 bg-slate-50 px-2 py-1.5">
                    <code className="min-w-0 flex-1 select-all break-all text-[11px] text-slate-600">{issuedAccessCode.accessCode}</code>
                    <CopyIconButton
                      copied={copiedKey === "code"}
                      label="复制授权码"
                      onCopy={() => copyText("code", issuedAccessCode.accessCode)}
                    />
                  </div>
                </div>

                {copyError && (
                  <Alert variant="destructive">
                    <AlertDescription>{copyError}</AlertDescription>
                  </Alert>
                )}
              </div>
            )
          })()}
          <div className="flex justify-end"><Button onClick={closeIssuedDialog}>完成</Button></div>
        </DialogContent>
      </Dialog>
    </div>
  )
}
