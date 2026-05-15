# 调试符号（Debug Symbols）

本文档说明 Dingo-Store 在 release 流程中如何打包、分发调试符号，
以及生产 crash 排查时如何使用。

## 为什么要分离调试符号？

CI 用 `RelWithDebInfo` 模式编译 `dingo-store` 各个 binary。这种模式
产出的是开启优化、同时携带完整 DWARF 调试信息的二进制，所以**单个
`dingodb_server` 大约 1.1 GB**——其中约 91% 是 DWARF 段（loader
运行时并不会加载这些段，只是占文件体积）。

为了让生产镜像瘦下来又不丢调试能力，release 流程把每个 binary 拆成
两份产物：

- **Stripped binary**（`/opt/dingo-store/build/bin/dingodb_server`）
  随 `dingodatabase/dingo-store:<tag>` 镜像发布。仅保留 runtime 需要
  的 ELF 段，体积约 **88 MB**。
- **Debug companion**（`dingodb_server.debug`）随
  `dingodatabase/dingo-store:<tag>-debug` sidecar 镜像发布。纯 DWARF
  调试信息，体积约 **1 GB**。

这套做法等价于 **Debian `-dbgsym`** 子包 / **RHEL `-debuginfo`** 子包
的工业惯例，只是把发布载体换成了 Docker 镜像。

## 工作原理（`.gnu_debuglink` + build-id）

拆分动作在 `.github/workflows/release-dockerhub.yml` 的
`prepare_release_artifacts` job 里跑，对每个 `dingodb_*` binary 做
标准 binutils 三步：

```bash
objcopy --only-keep-debug  dingodb_server  dingodb_server.debug
objcopy --strip-debug      dingodb_server
objcopy --add-gnu-debuglink=dingodb_server.debug  dingodb_server
```

三步执行完，stripped 后的 `dingodb_server` 在 ELF 内部会多出一个
`.gnu_debuglink` 段——里面记着 `.debug` 文件名 + CRC32 校验值。
同时 binary 跟 `.debug` 都保留链接器生成的 `.note.gnu.build-id`
（一个 SHA-1 fingerprint），这是两者一一对应的锚点。

gdb、addr2line、perf、elfutils 等所有标准工具都原生支持这套机制：
拿到 stripped binary 后，工具会按以下路径自动寻找配对的 `.debug`
文件，加载后跟未 strip 的体验完全一致：

- binary 同目录下的 `<basename>.debug`
- 同目录下 `.debug/` 子目录
- `/usr/lib/debug/.build-id/NN/<rest>.debug`（按 build-id 哈希路径）

## 镜像如何发布到 Docker Hub

镜像发布由两个 workflow 驱动：`ci_rocky8.yml`（负责编译）和
`release-dockerhub.yml`（负责打包 + push）。后者内部拆成两个 job
保持职责分层。

### 整体架构

镜像构建采用**两阶段多 stage 设计**：build 阶段使用 dingo-base:rocky8
（确保 glibc / libstdc++ ABI 兼容上游 `ci_rocky8.yml` 的 build env），
runtime 阶段切到 `rockylinux:8-minimal`（~140 MB）+ 仅装 binary 实际
依赖的 `.so`（`ldd dingodb_server` 验证）。Sidecar 镜像 FROM 主 runtime
再装 `gdb` + `tar` 等调试工具。

```
┌─────────────────────────────────────────────────────────────┐
│ ci_rocky8.yml（不变）                                        │
│  on: push/PR to main/develop/quickBI-v3.0.0                 │
│  └─ 上传 4 个常规 artifact:                                  │
│       dingo-store.tar.gz, branch_name, commit_id, event     │
│     + 1 个条件 artifact: tag_name（仅 tag push 时；见 NOTE）│
└─────────────────────────┬───────────────────────────────────┘
                          │ workflow_run on CMake_rocky8
                          ▼
┌─────────────────────────────────────────────────────────────┐
│ release-dockerhub.yml(2 个 job)                              │
│                                                             │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Job 1: prepare_release_artifacts                       │ │
│  │   if: workflow_run.conclusion == 'success' &&          │ │
│  │       workflow_run.event != 'pull_request'             │ │
│  │   permissions: { actions: read }                       │ │
│  │   ├─ 从 CMake_rocky8 下载 ci_rocky8 上传的全部 artifact│ │
│  │   │   (actions/download-artifact@v4 + run-id +         │ │
│  │   │    github-token)                                   │ │
│  │   ├─ 对 4 个 binary 跑 objcopy 三步 strip              │ │
│  │   ├─ 重新打包:                                          │ │
│  │   │   - dingo-store.tar.gz       (stripped 运行包)     │ │
│  │   │   - dingo-store-debug.tar.gz (4 个 .debug 文件)    │ │
│  │   ├─ 把这两个 artifact 上传到当前 workflow run         │ │
│  │   └─ outputs (3 个必需项 fail-fast + 1 个可选项):       │ │
│  │       branch_name, commit_id, event, tag_name          │ │
│  └──────────────────────────┬─────────────────────────────┘ │
│                             │ needs: prepare_release_artifacts
│                             ▼                               │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ Job 2: build_and_push_images                           │ │
│  │   needs: [prepare_release_artifacts]                   │ │
│  │   if: result == 'success'                              │ │
│  │       && event != '' && event != 'pull_request'        │ │
│  │   permissions: { packages: write, contents: read }     │ │
│  │   env:                                                 │ │
│  │     BRANCH_NAME: ${{ needs.X.outputs.branch_name }}    │ │
│  │     COMMIT_ID:   ${{ needs.X.outputs.commit_id }}      │ │
│  │     EVENT:       ${{ needs.X.outputs.event }}          │ │
│  │     TAG_NAME:    ${{ needs.X.outputs.tag_name }}       │ │
│  │   ├─ 从同 workflow run 下载 stripped + debug 两个 tar  │ │
│  │   │   (actions/download-artifact@v4, 默认 same-run)    │ │
│  │   ├─ Docker meta × 2 (runtime + debug sidecar)         │ │
│  │   └─ Build + push 两个镜像                              │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Job 职责边界

| Job | 输入 | 输出 | 责任 |
|---|---|---|---|
| **prepare_release_artifacts** | CMake_rocky8 的 artifact（4 常规 + 0/1 条件 tag_name） | 2 个新 artifact（stripped tar + debug tar）+ 4 个 job outputs（metadata，3 必需 + 1 可选） | **打包**：把 build 产物变成 release-ready 产物 |
| **build_and_push_images** | Job 1 的 artifacts + outputs | 2 个 Docker Hub tag（`:<tag>` + `:<tag>-debug`）| **发布**：拿发布包做镜像并 push |

其他 CI workflow（`ci_rocky9.yml`、`ci_ubuntu.yml`）只负责多发行版
build 验证，**不发布任何镜像**。

`ci_rocky8.yml` 保持极简——只跑 `RelWithDebInfo` 编译 + 上传原始
artifact。所有 release-only 的处理（strip、调试信息分离、重新打包、
镜像 build、push）都集中在 `release-dockerhub.yml` 里。两个 job 在
**同一次 workflow run 内**通过 `needs` 串起来，所有 artifact 留在
同 run namespace，不依赖 `workflow_run` 跨 run 查找，从根上避免了
"按名字找 artifact" 的竞态。

### 触发条件

| Git 事件 | push 到 Docker Hub 的 tag |
|---|---|
| Push 到 `main` | `:latest` + `:<sha7>`（如 `:729a688`）|
| Push 到 `develop` / `quickBI-v3.0.0` | `:<branch>-<sha7>` + `:<branch>-latest` |
| Git tag push（`refs/tags/<name>`）| `:<name>`（如 `:v1.0.0`）—— 见下方 NOTE |
| Pull request | 无——CI 会跑 build，但 `release-dockerhub.yml` 的 publish job 被 `if` 短路 |

> **关于 tag publishing 的 NOTE**：tag publishing 的 tag 规则在
> `release-dockerhub.yml` 里已经编码完整，但上游 `ci_rocky8.yml` 只
> 配了 `on.push.branches:` 没配 `on.push.tags:`，所以 tag push 当前
> **不会触发**本管线。这条路径保留下来，方便未来给 `ci_rocky8.yml`
> 补上 `tags:` 过滤后立即生效。

每个主镜像 tag 都配套发布一个 `-debug` sidecar：

| 主镜像 tag | 配套 sidecar tag |
|---|---|
| `:latest` | `:latest-debug` |
| `:<sha7>` | `:<sha7>-debug` |
| `:<branch>-<sha7>` | `:<branch>-<sha7>-debug` |
| `:<branch>-latest` | `:<branch>-latest-debug` |
| `:v1.0.0` | `:v1.0.0-debug` |

sidecar 镜像基于 `docker/Dockerfile.debug` 构建，本质是
`FROM dingodatabase/dingo-store:${STORE_TAG}` 再叠一层 `.debug`
文件到 `/opt/dingo-store/build/bin/`。因为 `FROM` 复用主镜像所有
layer，已经拉过主镜像的用户拉 sidecar 几乎是增量下载。

### Build pipeline 详细流程

```
ci_rocky8.yml（RelWithDebInfo build）
    ├─ cmake --build → 4 个带完整 DWARF 的 binary
    └─ tar -czvf dingo-store.tar.gz（4 binary + scripts + conf）
        │
        ▼（上传为 artifact + 3 个必需 metadata .txt + 1 个条件 tag_name.txt）
        │
release-dockerhub.yml（workflow_run 监听 CMake_rocky8）
  │
  ├─ job: prepare_release_artifacts
  │     ├─ 从 CMake_rocky8 下载全部 artifacts
  │     │   （actions/download-artifact@v4 + run-id + github-token，
  │     │    需要 actions:read 权限）
  │     ├─ 对 4 个 dingodb_* binary 跑 objcopy 三步
  │     ├─ 重打 dingo-store.tar.gz（stripped + scripts + conf）
  │     ├─ 打包 dingo-store-debug.tar.gz（4 个 .debug 文件）
  │     ├─ 把这两个 tarball 作为本次 workflow run 的 artifact 上传
  │     └─ 把 metadata（branch_name / commit_id / event / tag_name）
  │        暴露为 job outputs（无需文件转发）
  │
  └─ job: build_and_push_images（needs: prepare_release_artifacts）
        ├─ 下载 dingo-store.tar.gz       → ./docker/（同 run namespace，无需 run-id）
        ├─ 下载 dingo-store-debug.tar.gz → ./docker/（同上）
        ├─ 从 needs.prepare_release_artifacts.outputs 解析镜像 tag
        ├─ docker build -f docker/Dockerfile       → dingodatabase/dingo-store:<tag>
        └─ docker build -f docker/Dockerfile.debug → dingodatabase/dingo-store:<tag>-debug
```

主镜像 tag 跟 `-debug` sidecar tag 一定指向同一个 git commit——因为
两者都由同一次 `release-dockerhub.yml` run 产出，跑的是同一组 strip
后的 binary（同 `.note.gnu.build-id`），不可能版本错配。

## SRE 工作流

生产环境某个 server 跑挂了拿到 core file 之后，**推荐直接用 sidecar
镜像内的 gdb 跑，host 零依赖**——`:<tag>-debug` 镜像里已预装
`gdb` + `tar` + 必要工具，跟主镜像同 rocky8-minimal base 同 glibc /
libstdc++，省去 host 上对 gdb 版本 / glibc-debuginfo / sysroot 等的
各种适配。

```bash
# 1. 看生产 pod 跑的镜像 tag
kubectl describe pod <victim-pod> | grep Image:
# Image: dingodatabase/dingo-store:abc1234

# 2. 拉配套的 :-debug sidecar 镜像（同 tag + -debug 后缀，主镜像 layer 已缓存则增量小）
docker pull dingodatabase/dingo-store:abc1234-debug

# 3. 把 core 文件 mount 进 sidecar 镜像、直接跑 gdb
docker run --rm -it \
  -v /path/to/core.<pid>:/core:ro \
  --entrypoint=gdb \
  dingodatabase/dingo-store:abc1234-debug \
    /opt/dingo-store/build/bin/dingodb_server /core

# (gdb) thread apply all bt
# 函数名 + 源码 file:line 全部恢复（dingo 代码经 .gnu_debuglink 解析 .debug）
# glibc / libstdc++ 帧含函数名（容器内系统库 .symtab 可用）
```

`<tag>-debug` 镜像跟主镜像保证同一个 git commit（同一次
`release-dockerhub.yml` run 产出，build-id 一致），不存在版本错配。

### K8s 场景：ephemeral debug container

线上 K8s pod 还活着想 attach 调试（不一定是 crash 后分析）：

```bash
# 用 sidecar 镜像作为 ephemeral debug container，共享目标 pod 的 PID namespace
kubectl debug <victim-pod> \
  --image=dingodatabase/dingo-store:<tag>-debug \
  --target=<victim-container> \
  --share-processes \
  -it -- /bin/bash

# 进入 ephemeral 容器后：
gdb -p <victim-dingodb_server-pid>     # live attach
# 或对 core 文件
gdb /opt/dingo-store/build/bin/dingodb_server /path/to/core
```

### Fallback：要在 host 跑 gdb 的话

如果 host 已经有 gdb 配置、SRE 习惯 host 调试，可以拷 `.debug` 文件
出来到 stripped binary 旁边：

```bash
cid=$(docker create dingodatabase/dingo-store:<tag>-debug)
docker cp $cid:/opt/dingo-store/build/bin/dingodb_server.debug ./
docker rm $cid

# host 上 gdb 经 .gnu_debuglink 自动找到同目录下的 .debug 文件
gdb /opt/dingo-store/build/bin/dingodb_server core.<pid>
```

但 host 上 glibc 不一定跟容器内 glibc 版本一致，glibc 帧的函数名
可能解不出来（要装 `glibc-debuginfo` 包），dingo 代码部分则正常。
**推荐优先用容器内 gdb 方案**，host 兜底仅在不能跑 docker 时使用。

## 本地调试

开发者在本地直接 `cmake --build` 出来的 binary 跟 CI 一样是
`RelWithDebInfo`，`.debug_*` 段就在 binary 本身里，gdb 不需要额外
配置就能用。要在本地复现 CI 的 strip 流程：

```bash
cd build/bin
for bin in dingodb_server dingodb_cli dingodb_br dingodb_client; do
  objcopy --only-keep-debug "$bin" "$bin.debug"
  objcopy --strip-debug     "$bin"
  objcopy --add-gnu-debuglink="$bin.debug" "$bin"
done

# Round-trip 自检：addr2line 应该能通过 .debug 反向定位到源码行号
addr_main=$(nm dingodb_server | awk '/ T main$/{print "0x"$1}')
addr2line -e dingodb_server -f -C "$addr_main"
# 预期输出：main / src/server/main.cc:520
```

把 `$bin.debug` 文件移走再跑 addr2line，应该看到输出退化为 `??:?`；
移回来恢复正常。这条来回测试确认 `.gnu_debuglink` 链路真的在工作，
不是 gdb 在用 binary 里残留的什么旧符号瞒过去。

## 常见陷阱

1. **缺 build-id**。`objcopy --add-gnu-debuglink` 不依赖
   `.note.gnu.build-id`，但下游工具链（debuginfod、各发行版的
   split-debuginfo 流程）需要这个 section 来唯一识别 binary。CI 通过
   `readelf -n | grep 'Build ID'` 强制校验，缺则 fail。如果用自定义
   toolchain 编译，确保 `LDFLAGS` 含 `-Wl,--build-id=sha1`。

2. **CRC mismatch**。`.gnu_debuglink` section 里塞了 `.debug` 文件的
   CRC32 校验值。如果 `.debug` 文件被修改、重 build 或跟主 binary
   不同步，gdb 会静默忽略它（带 "CRC mismatch" warning）。永远成对
   重新生成。`objcopy` 三步固定顺序：`--only-keep-debug` →
   `--strip-debug` → `--add-gnu-debuglink`，反过来不行。

3. **`.debug` 不能跨 commit 用**。commit X build 出来的 `.debug` 文件
   对 commit Y 的 binary 没用——build-id 不同，CRC 也对不上。永远
   按 `<tag>` 配对使用主镜像和 `<tag>-debug` 镜像。

4. **gdb 找不到 `.debug` 文件**。默认 gdb 在 binary 同目录、`.debug/`
   子目录、`/usr/lib/debug/` 下找。如果 `.debug` 放在别的位置，可以
   在 `~/.gdbinit` 加 `set debug-file-directory /path/to/dir`，或者
   命令行加 `-iex 'set debug-file-directory /path'`。
