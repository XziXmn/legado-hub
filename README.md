# LegadoHub

自托管小说聚合订阅服务，为 Reading / Legado 提供稳定的后端书源。配套阅读客户端：[legado-X](https://github.com/XziXmn/legado-X)。

---

## 解决什么问题

用「阅读」看小说的人，多少都被书源折腾过：

- 书源说挂就挂，换源意味着重新搜索、重新登录；
- 多台设备、多人使用，各自维护一套书源，同一本书重复抓取，浪费时间也浪费带宽。

LegadoHub 的做法是把这些麻烦事集中到服务端：**书源由管理员统一维护，章节由服务端统一抓取缓存，读者只要搜索、订阅、阅读。**

- **共享书库**：一本书入库后全站共享。多人订阅同一本书，共用一份章节缓存，阅读进度各记各的；
- **主源失效，候选补全**：正文优先从官方源（主源）获取；主源只给 VIP 预览时，自动从第三方候选源补全完整章节；
- **抓到的就是自己的**：章节抓取后按序缓存落盘，连载期间持续追更。之后源再怎么波动，已入库的章节始终可读；
- **邀请制访问**：管理员为每位读者发放**专属书源链接**（含授权码），导入后自动鉴权，无需再手输码。

```
管理员安装插件、登录官方源 → 创建用户、复制专属书源链接
                ↓
用户在 Reading 导入专属链接 → 自动登录
                ↓
搜索并订阅 → 服务端抓取/补全章节 → 阅读
```

---

## 镜像通道

开发会频繁合入 `main`，但**正式镜像不会每次提交都更新**。Docker Hub 上分两轨：

| 通道 | 标签 | 何时更新 | 用途 |
|------|------|----------|------|
| **正式** | `v0.3.0`、`latest` | 打 Git tag `v*` 之后 | 长期运行 / 对外推荐 |
| **开发测试** | `beta` | 每次推送到 `main` | 试新功能 |

```bash
docker pull xzixmn/legado-hub:latest   # 正式
docker pull xzixmn/legado-hub:v0.3.0   # 钉死正式版
docker pull xzixmn/legado-hub:beta     # 开发测试
```

发版节奏：日常只推 `beta`，验收通过后打 `vX.Y.Z` 才更新 `latest`。  
版本记录见 [CHANGELOG.md](CHANGELOG.md)；[VERSION](VERSION) 为当前正式版本号。

---

## 快速开始

推荐用 Docker Compose；不用 Compose 可看 [Docker CLI](#docker-cli)。默认镜像为正式版 `latest`。

### 前提条件

已安装 Docker，`8765`（阅读）和 `8766`（管理）端口空闲。

### 1. 准备目录

```bash
mkdir -p /opt/legado-hub && cd /opt/legado-hub
mkdir -p data config generated runtime plugins/sources/thirdparty plugins/sources/official
```

### 2. 下载 compose 文件

```bash
curl -fsSL -o docker-compose.yml \
  https://raw.githubusercontent.com/XziXmn/legado-hub/main/docker-compose.yml
```

通常只需留意：

| 配置项 | 说明 |
|--------|------|
| `PUID` / `PGID` | 宿主机用户/组，默认 `1000` |
| `volumes` 左侧路径 | NAS 建议改成绝对路径（文件内有示例注释） |
| `ports` | 仅冲突时改左侧宿主端口 |

如果改过 `PUID`/`PGID`，或书库文件权限异常：

```bash
LEGADOHUB_CHOWN_DATA=1 docker compose up -d --force-recreate legadohub
docker compose up -d --force-recreate legadohub
```

注意：不要用 `docker compose restart` 加载新环境变量。

### 3. 启动

```bash
docker compose pull
docker compose up -d
```

镜像较大（含 Chromium）。`healthy` 后自检：

```bash
curl -s http://127.0.0.1:8765/api/auth/entrypoint   # "entrypoint":"public"
curl -s http://127.0.0.1:8766/api/auth/entrypoint   # "entrypoint":"admin"
```

### 4. 管理员密码

首次启动会创建 `admin`，随机密码只在日志里打印一次：

```bash
docker compose logs legadohub | grep -i password
```

错过可重置：

```bash
docker compose exec -T legadohub \
  python /app/backend/scripts/reset_user_password.py --username admin
```

登录后请到 **设置 → 账户安全** 修改密码。

### 5. 初始配置

1. 打开 `http://服务器IP:8766`，用 `admin` 登录；
2. 书源：镜像内置全部第三方源和起点 Web 源。`plugins/sources/thirdparty/`、`plugins/sources/official/` 都是覆盖层：缺失的插件 ID 会由镜像补齐；宿主已有同 ID 目录优先，可放入新增书源或新版后重启容器；
3. （公网用户）在 **设置 → 阅读 → 公网书源地址** 填上对外 origin，例如 `https://book.example.com:2087`；不填的话，发给用户的链接就只有局域网地址；
4. **用户管理 → 新建用户**，弹窗里**复制书源链接**发给读者（只显示一次）。

### 6. 接入 Reading / Legado

读者导入管理员发放的**专属书源链接**即可（必须带 `code`）。推荐使用配套客户端 [legado-X](https://github.com/XziXmn/legado-X)（支持章节评论）；官方 Reading / Legado 及常见衍生版也可导入同一条书源，评论入口会自动降级。

例如：

```
http://服务器IP:8765/api/subscribe/legado/source?code=...
# 或公网
https://book.example.com:2087/api/subscribe/legado/source?code=...
```

- 导入后自动鉴权，搜索/目录/正文按当前用户会话访问；
- 书源登录页提供网页 **订阅**、**书库** 入口；
- **不支持**无 `code` 的公共书源地址。

> 重置授权码会使旧码、旧链接和已有会话立即失效，需重新发放。

### Docker CLI

```bash
docker pull xzixmn/legado-hub:latest

docker run -d \
  --name legadohub \
  --restart always \
  --init \
  --security-opt no-new-privileges:true \
  --cap-drop ALL \
  --cap-add CHOWN --cap-add DAC_OVERRIDE --cap-add SETGID --cap-add SETUID \
  -p 8765:8765 -p 8766:8766 \
  -e TZ=Asia/Shanghai -e PUID=1000 -e PGID=1000 -e LEGADOHUB_CHOWN_DATA=0 \
  --log-driver json-file --log-opt max-size=10m --log-opt max-file=3 \
  -v "$PWD/data:/app/backend/data" \
  -v "$PWD/config:/app/backend/config" \
  -v "$PWD/generated:/app/backend/generated" \
  -v "$PWD/runtime:/app/backend/runtime" \
  -v "$PWD/plugins/sources/thirdparty:/app/plugins/sources/thirdparty" \
  -v "$PWD/plugins/sources/official:/app/plugins/sources/official" \
  xzixmn/legado-hub:latest
```

---

## 公网部署（VPS / 域名）

### 访问与鉴权

| 层级 | 谁负责 |
|------|--------|
| 谁能连上 8765 / 反代 | **防火墙、雷池、安全组、反代**（应用不做公网 Host 白名单） |
| 谁能读内容 | **专属书源 `code` / 登录会话**（无匿名阅读、无开放注册） |
| 管理后台 8766 | 建议仅内网 / VPN / 来源 IP 限制 |

应用自身仍会做两件事：校验 Host 语法；拒绝公网客户端伪造局域网 Host（防止书源基址被写成 192.168.x.x 这类内网地址）。

### 公网书源地址

在管理后台 **设置 → 阅读 → 公网书源地址** 填写用户实际访问的 origin：

- 格式：`https://域名:端口` 或 `http://公网IP:8765`（**不要带路径**）
- 非 80/443 端口必须写上，并和浏览器 / Reading 的实际访问方式保持一致
- **只影响**「用户管理」生成的专属链接里的公网部分；**不填则只生成局域网链接**
- 不决定能不能从公网打开服务（那是防火墙/反代的事）

首次部署也可以设置 `LEGADOHUB_PUBLIC_BASE_URL=https://book.example.com`。后台保存的公网书源地址优先于该变量，并立即生效；变量不参与 Host 放行或 HTTPS 强制。

### 端口建议

| 端口 | 用途 | 建议 |
|------|------|------|
| `8765` | 阅读 / 书源 / 读者 Web | 可走域名反代；务必配合专属链接 |
| `8766` | 管理后台 | 不要裸奔公网 |

TLS、反代（Caddy / Nginx / Cloudflare 等）需自行配置。

### 公网与局域网双源

| 导入地址 | 书源身份（示意） |
|----------|------------------|
| 公网域名 / 公网 IP | `LegadoHub`（公网） |
| 局域网 IP | `LegadoHub-LAN`（内网） |

可并存；日常建议只启用当前网络对应的那一套。

### 自检

```bash
docker compose ps
curl -s http://127.0.0.1:8765/api/auth/entrypoint
curl -s http://127.0.0.1:8766/api/auth/entrypoint
curl -sI https://你的域名/api/auth/entrypoint
```

可选：compose 里配置 `LEGADOHUB_TRUSTED_PROXIES`（反代网段，用于识别真实客户端 IP）。

---

## 两种使用方式

导入**专属书源**后：

| 方式 | 说明 |
|------|------|
| **直接搜索第三方源** | 在阅读器里搜已启用的第三方源，随搜随看。适合临时读。 |
| **订阅后读共享库** | 在网页「订阅」或书源里的「订阅」入口建订阅；服务端抓取、补全、落盘，适合追更。 |

搜索结果里可同时出现第三方实时结果与已入库共享书。

---

## 功能

| 功能 | 说明 |
|------|------|
| 共享书库 | 一书一库，多用户共享章节缓存 |
| 主源优先，候选补全 | 官方主源优先；VIP 预览时用第三方补全 |
| 自动追更 | 连载持续抓取新章节 |
| 专属书源 | 每人一条带 `code` 的导入链接，导入后自动鉴权 |
| 凭证可吊销 | 重置授权码后旧链接与会话立即失效 |
| 公网 / 内网双源 | 同一服务同时提供两套书源身份 |
| 双入口 | `8765` 读者，`8766` 管理 |

---

## 常见问题

<details>
<summary>为什么在 Reading 里搜不到刚订阅的书？</summary>

Reading 只展示已发布且有可读章节的书。到 Web「书库」确认已发布且至少一章可读。
</details>

<details>
<summary>为什么部分章节只有预览？</summary>

不会绕过站点付费规则。主源无完整权限且候选源也补不全时，只能保留预览。
</details>

<details>
<summary>书源显示可达，正文还是失败？</summary>

「可达」只表示网络通，不代表页面结构、登录态或章节权限正常。请看书详情里的错误信息。
</details>

<details>
<summary>多个用户能订阅同一本书吗？</summary>

可以。章节数据共享一份，订阅与进度各自独立。
</details>

<details>
<summary>忘记管理员密码？</summary>

```bash
docker compose exec -T legadohub \
  python /app/backend/scripts/reset_user_password.py --username admin
```

会生成新密码并撤销该管理员全部会话。
</details>

<details>
<summary>用户授权码丢了怎么办？</summary>

只在创建或重置时显示一次。在「用户管理」重新生成凭证，把**新的**专属书源链接发给对方；旧链接立即失效。
</details>

<details>
<summary>没有 code 的书源地址能用吗？</summary>

不能。书源接口必须带 `?code=`，请使用管理员发放的专属链接。
</details>

<details>
<summary>第三方插件目录被清空了？</summary>

目录为空时，启动会从镜像恢复默认第三方插件。自行修改过的版本需自行备份。
</details>

<details>
<summary>公网访问与「公网书源地址」是一回事吗？</summary>

不是。谁能连上服务看防火墙/雷池/反代；「公网书源地址」只决定发给用户的链接里写哪个公网 origin。
</details>

---

## 安全与使用边界

- 公网暴露时请用防火墙、雷池、反代收敛入口；管理口（`8766`）勿裸奔。
- 无开放注册、无匿名阅读；专属书源链接等同长期凭证，请妥善发放，泄露后立即重置。
- 请只在有权访问和处理相应内容的前提下使用，遵守目标站条款与当地法律。
- 本项目不是 Legado 官方项目，不保证任何第三方书源持续可用。

---

## 开发者入口

| 主题 | 位置 |
|------|------|
| 仓库结构与本地启动 | [AGENTS.md](AGENTS.md) |
| 书源插件规范 | [docs/architecture/source-plugin-contract.zh-CN.md](docs/architecture/source-plugin-contract.zh-CN.md) |
| 插件编写教程 | [docs/skills/book-source-craft/README.md](docs/skills/book-source-craft/README.md) |
| 产品边界 | [docs/PRODUCT.md](docs/PRODUCT.md) |
| 完整校验 | `verify.ps1` |

Windows 本地开发：

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r backend/requirements.txt
.venv/Scripts/python.exe -m playwright install chromium
Set-Location frontend
npm install
npm run build
Set-Location ../backend
../.venv/Scripts/python.exe -m app.server --host 0.0.0.0 --public-port 8765 --admin-port 8766
```

也可直接 `.\start.bat`。

---

## 贡献

问题与建议：[GitHub Issues](https://github.com/XziXmn/legado-hub/issues)。

---

## 相关项目

- [legado-X](https://github.com/XziXmn/legado-X) — 基于 [Legado-E](https://github.com/Luoyacheng/legado-E) / [Legado](https://github.com/gedoor/legado) 的定制阅读客户端。导入本服务发放的专属书源即可搜索、订阅、阅读；支持段评、页热评与章末评论。普通 Reading / Legado 也可使用本服务，评论入口会自动降级。

---

## 友情链接

- [LINUX DO](https://linux.do/)

---

## 许可

[MIT License](LICENSE)。源码、文档与仓库附属资源均按 MIT 授权，可自由使用、修改、分发与商用，须保留版权声明。

---

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docker Image](https://img.shields.io/badge/Docker-xzixmn%2Flegado--hub-2496ED?logo=docker)](https://hub.docker.com/r/xzixmn/legado-hub)
[![GitHub](https://img.shields.io/badge/GitHub-XziXmn-181717?logo=github)](https://github.com/XziXmn/legado-hub)
[![legado-X](https://img.shields.io/badge/Client-legado--X-181717?logo=github)](https://github.com/XziXmn/legado-X)
