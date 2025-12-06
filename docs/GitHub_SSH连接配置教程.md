# GitHub SSH 连接配置教程

本教程适用于 Linux 环境（包括 Docker 容器）配置 GitHub SSH 连接。

## 1. 检查是否已有 SSH 密钥

```bash
ls -la ~/.ssh
```

如果看到 `id_rsa` 和 `id_rsa.pub`（或 `id_ed25519` 和 `id_ed25519.pub`），说明已有密钥，可跳到第 3 步。

## 2. 生成新的 SSH 密钥

推荐使用 Ed25519 算法（更安全、更快）：

```bash
ssh-keygen -t ed25519 -C "your_email@example.com"
```

或使用传统 RSA 算法：

```bash
ssh-keygen -t rsa -b 4096 -C "your_email@example.com"
```

**参数说明：**
- `-t`：指定密钥类型
- `-b`：指定密钥位数（仅 RSA 需要）
- `-C`：添加注释（通常用邮箱标识）

**生成过程中：**
1. 提示保存位置时，直接按 Enter 使用默认路径 `~/.ssh/id_ed25519`
2. 提示输入密码时，可以直接按 Enter 跳过（不设置密码），或输入密码增加安全性

## 3. 启动 SSH Agent 并添加密钥

```bash
# 启动 ssh-agent
eval "$(ssh-agent -s)"

# 添加私钥到 agent
ssh-add ~/.ssh/id_ed25519
# 或者如果使用 RSA
ssh-add ~/.ssh/id_rsa
```

## 4. 复制公钥内容

```bash
cat ~/.ssh/id_ed25519.pub
# 或者如果使用 RSA
cat ~/.ssh/id_rsa.pub
```

复制输出的全部内容（以 `ssh-ed25519` 或 `ssh-rsa` 开头，以邮箱结尾）。

## 5. 在 GitHub 添加 SSH 公钥

1. 登录 GitHub，点击右上角头像 → **Settings**
2. 左侧菜单选择 **SSH and GPG keys**
3. 点击 **New SSH key**
4. 填写：
   - **Title**：给密钥起个名字（如 "Docker Container" 或 "Linux Server"）
   - **Key type**：选择 Authentication Key
   - **Key**：粘贴刚才复制的公钥内容
5. 点击 **Add SSH key**

## 6. 配置 SSH（可选但推荐）

创建或编辑 SSH 配置文件：

```bash
mkdir -p ~/.ssh
chmod 700 ~/.ssh
nano ~/.ssh/config  # 或使用 vim
```

添加以下内容：

```
Host github.com
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
```

保存后设置权限：

```bash
chmod 600 ~/.ssh/config
```

## 7. 测试连接

```bash
ssh -T git@github.com
```

首次连接会提示：

```
The authenticity of host 'github.com (IP)' can't be established.
ED25519 key fingerprint is SHA256:+DiY3wvvV6TuJJhbpZisF/zLDA0zPMSvHdkr4UvCOqU.
Are you sure you want to continue connecting (yes/no/[fingerprint])?
```

输入 `yes` 并按 Enter。

**成功连接会显示：**

```
Hi username! You've successfully authenticated, but GitHub does not provide shell access.
```

## 8. 配置 Git 用户信息

```bash
git config --global user.name "Your Name"
git config --global user.email "your_email@example.com"
```

## 9. 使用 SSH 克隆仓库

```bash
git clone git@github.com:username/repository.git
```

## 常见问题

### Q1: Permission denied (publickey)

**原因：** SSH 密钥未正确配置或未添加到 GitHub

**解决：**
```bash
# 检查 ssh-agent 是否运行
eval "$(ssh-agent -s)"

# 重新添加密钥
ssh-add ~/.ssh/id_ed25519

# 验证密钥是否已添加
ssh-add -l

# 使用详细模式调试
ssh -vT git@github.com
```

### Q2: 容器重启后 SSH 失效

**原因：** ssh-agent 在容器重启后不会自动启动

**解决：** 在 `~/.bashrc` 或 `~/.profile` 中添加：

```bash
# 自动启动 ssh-agent
if [ -z "$SSH_AUTH_SOCK" ]; then
    eval "$(ssh-agent -s)" > /dev/null
    ssh-add ~/.ssh/id_ed25519 2>/dev/null
fi
```

### Q3: 已有 HTTPS 仓库，如何切换到 SSH

```bash
# 查看当前远程地址
git remote -v

# 修改为 SSH 地址
git remote set-url origin git@github.com:username/repository.git
```

### Q4: 多个 GitHub 账号

在 `~/.ssh/config` 中配置多个 Host：

```
# 个人账号
Host github.com
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_ed25519_personal

# 工作账号
Host github-work
    HostName github.com
    User git
    IdentityFile ~/.ssh/id_ed25519_work
```

使用时：
```bash
# 个人仓库
git clone git@github.com:personal/repo.git

# 工作仓库
git clone git@github-work:company/repo.git
```

## 快速命令汇总

```bash
# 一键生成密钥（无密码）
ssh-keygen -t ed25519 -C "your_email@example.com" -N "" -f ~/.ssh/id_ed25519

# 启动 agent 并添加密钥
eval "$(ssh-agent -s)" && ssh-add ~/.ssh/id_ed25519

# 显示公钥（复制到 GitHub）
cat ~/.ssh/id_ed25519.pub

# 测试连接
ssh -T git@github.com
```

---

*最后更新：2025年12月6日*
