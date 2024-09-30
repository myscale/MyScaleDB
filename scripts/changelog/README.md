# Myscale changelog 脚本使用说明

## 概述
自动化 release 共包含两个脚本，`myscale_changelog.py` 与 `sync_release_note.py`，其中：

- `myscale_changelog.py` 脚本的主要功能是根据已合并的合并请求（MR）生成 changelog，并更新相关版本信息。该脚本通过与GitLab的集成，自动拉取合并请求的信息，并生成符合规范的 changelog。创建 release 分支、release tag，提交更新的 changelog 的 MR 到 `mqdb-dev` 主分支
- `sync_release_note.py` 脚本需要在更新主分支的 changelog MR 合并之后执行，且需要在主分支上执行，该脚本会自动获取当前最新的 changelog，通过与 GitHub 集成，将该更新自动提交到 GitHub 中，生成 PR 到 `myscale-deploy-guide` 主分支

## 依赖

在运行此脚本之前，请确保安装了以下依赖项：

- Python 3.x
- fuzzywuzzy
- python-gitlab

可以使用以下命令安装Python依赖项：
```bash
pip install fuzzywuzzy python-gitlab PyGithub
```

## 使用方法

### `myscale_changelog.py`

#### 脚本参数

运行脚本时，可以使用以下命令行参数：

- -d, --debug：启用调试日志记录。
- -v, --verbose：设置脚本的详细级别，可以多次使用以增加详细级别。
- -b, --branch：指定要生成 changelog 的分支。未指定 -b 时，默认基于 mqdb-dev 创建 release 分支发布
- --version：指定要发布的版本号，例如 `1.2.3` 或 `1.2.3-rc1`。
- --from-ref：指定生成 changelog 的起始提交 SHA, 分支名称或 tag。默认使用最后一次发布的版本的 tag
- --private-token：GitLab 访问的私有令牌，用于身份验证。

#### 运行脚本

使用以下命令运行脚本：

```bash
./myscale_changelog.py --version <版本号> --branch <分支名称> --private-token <GitLab私有令牌>
```

例如：

```bash
python myscale_changelog.py --version 1.2.3 --branch master --private-token your_private_token
```

#### 脚本流程

1. 解析参数：脚本首先会解析命令行传入的参数，并根据这些参数执行相应的操作。
2. 检查分支和标签：检查指定的分支和标签是否符合规范，并执行必要的Git操作。
3. 生成 changelog：根据指定的版本号，从GitLab获取相应时间范围内的合并请求，在生成 changelog 时可以对不恰当的 changelog 进行临时修改
4. 更新版本文件：根据生成的 changelog，更新 changelog 文件。
5. 提交和推送更改：在代码库中创建或者推送更新到远程仓库中 release/vx.x.x分支

#### 脚本日志

脚本运行时会生成详细的日志记录，便于调试和排查问题。日志记录包括执行的每一步操作以及相应的输出信息。

#### 注意事项

- 确保你的Git环境配置正确，并且已经登录到对应的GitLab账户。
- 在运行脚本前，确保已经拉取最新的代码，以避免因代码冲突导致的问题。
- 如果遇到 MR 创建失败，可能由于 access token 不具备写权限，可手动在 repo 中创建 MR 更新 changelog

### `sync_release_note.py`

#### 脚本参数

运行脚本时，可以使用以下命令行参数：

- --github-user-or-token：myscale-deploy-guide github 仓库用户名或者 access token，token 需要有该 repo 的访问权限
- --github-password：github 用户密码，在使用 access token 时，该项可不指定
- -v，--verbose：设置脚本的详细级别，可以多次使用以增加详细级别。
- --yes：确认操作时直接使用 Yes 替代

#### 运行脚本

使用以下命令运行脚本：

```bash
python sync_release_note.py --github-user-or-token <GitHub私有令牌>
```

#### 脚本流程

1. 获取最近的几个 release tag，tag 格式为 `myscale-vx.x.x...`
2. 获取最新的 changelog，比较最新的 changelog 版本与最新的 release tag 是否一致
3. 下载 `myscale-deploy-guide` 到本地，创建更新 release note 分支并切换到该分支
4. 更新 release note，并将修改推送到 github
5. 创建更新 release note 的 PR 到 `myscale-deploy-guide` 主分支

#### 注意事项

- 确保本地可以访问 gitlab `mqdb` 与 github `myscale-deploy-guide` repo