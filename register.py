import json
import os
import re
import sys
import time
import uuid
import random
import string
import secrets
import hashlib
import base64
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Any, Dict, Optional, List
import urllib.parse
import urllib.request
import urllib.error

import asyncio
import requests as py_requests
try:
    import aiohttp
except ImportError:
    aiohttp = None

from curl_cffi import requests

OUT_DIR = Path(__file__).parent.resolve()
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"

# ========== GPTMail 客户端 (保留成功的 SSR 破解逻辑) ==========

class GPTMailClient:
    def __init__(self, proxies: Any = None):
        self.session = requests.Session(proxies=proxies, impersonate="chrome")
        self.session.headers.update({
            "User-Agent": UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Referer": "https://mail.chatgpt.org.uk/"
        })
        self.base_url = "https://mail.chatgpt.org.uk"

    def _init_browser_session(self):
        try:
            resp = self.session.get(self.base_url, timeout=15)
            gm_sid = self.session.cookies.get("gm_sid")
            if gm_sid:
                self.session.headers.update({"Cookie": f"gm_sid={gm_sid}"})
            token_match = re.search(r'(eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+)', resp.text)
            if token_match:
                self.session.headers.update({"x-inbox-token": token_match.group(1)})
        except Exception:
            pass

    def generate_email(self) -> str:
        self._init_browser_session()
        url = f"{self.base_url}/api/generate-email"
        resp = self.session.get(url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            email = data['data']['email']
            self.session.headers.update({"x-inbox-token": data['auth']['token']})
            return email
        raise RuntimeError(f"GPTMail 生成失败: {resp.status_code}")

    def list_emails(self, email: str) -> List[Dict]:
        encoded_email = urllib.parse.quote(email)
        url = f"{self.base_url}/api/emails?email={encoded_email}"
        resp = self.session.get(url, timeout=15)
        if resp.status_code == 200:
            return resp.json().get('data', {}).get('emails', [])
        return []

def get_email_and_code_fetcher(proxies: Any = None):
    client = GPTMailClient(proxies)
    email = client.generate_email()
    
    def fetch_code(timeout_sec: int = 180, poll: float = 6.0) -> str | None:
        regex = r"(?<!\d)(\d{6})(?!\d)"
        start = time.monotonic()
        attempt = 0
        while time.monotonic() - start < timeout_sec:
            attempt += 1
            try:
                summaries = client.list_emails(email)
                print(f"[otp] 轮询 #{attempt}, 收到 {len(summaries)} 封邮件, 目标: {email}")
                for s in summaries:
                    m = re.search(regex, str(s.get("subject", "")))
                    if m: return m.group(1)
            except: pass
            time.sleep(poll)
        return None

    return email, _gen_password(), fetch_code

# ========== OAuth 核心逻辑 (对齐原版的完美重定向流) ==========

def _gen_password() -> str:
    alphabet = string.ascii_letters + string.digits
    special = "!@#$%^&*.-"
    base = [random.choice(string.ascii_lowercase), random.choice(string.ascii_uppercase),
            random.choice(string.digits), random.choice(special)]
    base += [random.choice(alphabet + special) for _ in range(12)]
    random.shuffle(base)
    return "".join(base)

def _random_name() -> str:
    return ''.join(random.choice(string.ascii_lowercase) for _ in range(7)).capitalize()

def _random_birthdate() -> str:
    start = datetime(1975, 1, 1); end = datetime(1999, 12, 31)
    d = start + timedelta(days=random.randrange((end - start).days + 1))
    return d.strftime('%Y-%m-%d')

def _b64url_no_pad(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")

def _sha256_b64url_no_pad(s: str) -> str:
    return _b64url_no_pad(hashlib.sha256(s.encode("ascii")).digest())

def _pkce_verifier() -> str:
    return secrets.token_urlsafe(64)

def _parse_callback_url(callback_url: str) -> Dict[str, Any]:
    parsed = urllib.parse.urlparse(callback_url)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    def get1(k: str) -> str:
        return (query.get(k, [""])[0] or "").strip()
    return {"code": get1("code"), "state": get1("state"), "error": get1("error")}

def _decode_jwt_segment(seg: str) -> Dict[str, Any]:
    try:
        pad = "=" * ((4 - (len(seg) % 4)) % 4)
        return json.loads(base64.urlsafe_b64decode(seg + pad).decode("utf-8"))
    except: return {}

def _post_form(url: str, data: Dict[str, str]) -> Dict[str, Any]:
    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST",
                                 headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))

@dataclass(frozen=True)
class OAuthStart:
    auth_url: str; state: str; code_verifier: str; redirect_uri: str

def generate_oauth_url(redirect_uri: str = "http://localhost:1455/auth/callback") -> OAuthStart:
    state = secrets.token_urlsafe(16)
    verifier = _pkce_verifier()
    challenge = _sha256_b64url_no_pad(verifier)
    
    # 核心：保留原版这两个决定生死的神仙参数
    params = {
        "client_id": "app_EMoamEEZ73f0CkXaXp7hrann", "response_type": "code", "redirect_uri": redirect_uri,
        "scope": "openid email profile offline_access", "state": state, "code_challenge": challenge,
        "code_challenge_method": "S256", "prompt": "login",
        "id_token_add_organizations": "true",
        "codex_cli_simplified_flow": "true",
    }
    return OAuthStart(f"https://auth.openai.com/oauth/authorize?{urllib.parse.urlencode(params)}", state, verifier, redirect_uri)

def fetch_sentinel_token(flow: str, did: str, proxies: Any = None) -> Optional[str]:
    try:
        resp = requests.post(
            "https://sentinel.openai.com/backend-api/sentinel/req",
            headers={"content-type": "text/plain;charset=UTF-8"},
            data=json.dumps({"p": "", "id": did, "flow": flow}),
            proxies=proxies, impersonate="chrome", timeout=15
        )
        return resp.json().get("token") if resp.status_code == 200 else None
    except: return None

def submit_callback_url(callback_url: str, expected_state: str, code_verifier: str, redirect_uri: str) -> str:
    cb = _parse_callback_url(callback_url)
    if cb.get("state") != expected_state: raise ValueError("State mismatch")
    token_resp = _post_form("https://auth.openai.com/oauth/token", {
        "grant_type": "authorization_code", "client_id": "app_EMoamEEZ73f0CkXaXp7hrann", "code": cb["code"],
        "redirect_uri": redirect_uri, "code_verifier": code_verifier
    })
    id_token = token_resp.get("id_token", "")
    claims = _decode_jwt_segment(id_token.split(".")[1]) if "." in id_token else {}
    
    now = int(time.time())
    expires_in = int(token_resp.get("expires_in") or 0)
    
    config = {
        "id_token": id_token,
        "access_token": token_resp.get("access_token"),
        "refresh_token": token_resp.get("refresh_token"),
        "account_id": str((claims.get("https://api.openai.com/auth") or {}).get("chatgpt_account_id") or "").strip(),
        "last_refresh": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
        "email": str(claims.get("email") or "").strip(),
        "type": "codex",
        "expired": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now + expires_in)),
    }
    return json.dumps(config, ensure_ascii=False, separators=(",", ":"))


# ========== 轻量版 CPA 维护实现（内嵌，不依赖项目包） ==========
DEFAULT_MGMT_UA = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"

def _mgmt_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _safe_json(text: str):
    try:
        return json.loads(text)
    except Exception:
        return {}


def _extract_account_id(item: dict):
    for key in ("chatgpt_account_id", "chatgptAccountId", "account_id", "accountId"):
        val = item.get(key)
        if val:
            return str(val)
    return None


def _get_item_type(item: dict) -> str:
    return str(item.get("type") or item.get("typo") or "")


class MiniPoolMaintainer:
    def __init__(self, base_url: str, token: str, target_type: str = "codex", used_percent_threshold: int = 95, user_agent: str = DEFAULT_MGMT_UA):
        self.base_url = (base_url or "").rstrip("/")
        self.token = token
        self.target_type = target_type
        self.used_percent_threshold = used_percent_threshold
        self.user_agent = user_agent

    def upload_token(self, filename: str, token_data: dict, proxy: str = "") -> bool:
        if not self.base_url or not self.token:
            return False
        content = json.dumps(token_data, ensure_ascii=False).encode("utf-8")
        files = {"file": (filename, content, "application/json")}
        headers = {"Authorization": f"Bearer {self.token}"}
        proxies = {"http": proxy, "https": proxy} if proxy else None
        for attempt in range(3):
            try:
                resp = py_requests.post(f"{self.base_url}/v0/management/auth-files", files=files, headers=headers, timeout=30, verify=False, proxies=proxies)
                if resp.status_code in (200, 201, 204):
                    return True
            except Exception:
                pass
            if attempt < 2:
                time.sleep(2 ** attempt)
        return False

    def fetch_auth_files(self, timeout: int = 15):
        resp = py_requests.get(f"{self.base_url}/v0/management/auth-files", headers=_mgmt_headers(self.token), timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        return (data.get("files") if isinstance(data, dict) else []) or []

    async def probe_and_clean_async(self, workers: int = 20, timeout: int = 10, retries: int = 1):
        if aiohttp is None:
            raise RuntimeError("需要安装 aiohttp: pip install aiohttp")
        files = self.fetch_auth_files(timeout)
        candidates = [f for f in files if _get_item_type(f).lower() == self.target_type.lower()]
        if not candidates:
            return {"total": len(files), "candidates": 0, "invalid_count": 0, "deleted_ok": 0, "deleted_fail": 0}

        semaphore = asyncio.Semaphore(max(1, workers))
        connector = aiohttp.TCPConnector(limit=max(1, workers))
        client_timeout = aiohttp.ClientTimeout(total=max(1, timeout))

        async def probe_one(session, item):
            auth_index = item.get("auth_index")
            name = item.get("name") or item.get("id")
            res = {"name": name, "auth_index": auth_index, "invalid_401": False, "invalid_used_percent": False, "used_percent": None}
            if not auth_index:
                res["invalid_401"] = False
                return res
            account_id = _extract_account_id(item)
            header = {"Authorization": "Bearer $TOKEN$", "Content-Type": "application/json", "User-Agent": self.user_agent}
            if account_id:
                header["Chatgpt-Account-Id"] = account_id
            payload = {"authIndex": auth_index, "method": "GET", "url": "https://chatgpt.com/backend-api/wham/usage", "header": header}
            for attempt in range(retries + 1):
                try:
                    async with semaphore:
                        async with session.post(f"{self.base_url}/v0/management/api-call", headers={**_mgmt_headers(self.token), "Content-Type": "application/json"}, json=payload, timeout=timeout) as resp:
                            text = await resp.text()
                            if resp.status >= 400:
                                raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
                            data = _safe_json(text)
                            sc = data.get("status_code")
                            res["invalid_401"] = sc == 401
                            if sc == 200:
                                body = _safe_json(data.get("body", ""))
                                used_pct = (body.get("rate_limit", {}).get("primary_window", {}).get("used_percent"))
                                if used_pct is not None:
                                    res["used_percent"] = used_pct
                                    res["invalid_used_percent"] = used_pct >= self.used_percent_threshold
                            return res
                except Exception as e:
                    if attempt >= retries:
                        res["error"] = str(e)
                        return res
            return res

        async def delete_one(session, name: str):
            if not name:
                return False
            from urllib.parse import quote
            encoded = quote(name, safe="")
            try:
                async with semaphore:
                    async with session.delete(f"{self.base_url}/v0/management/auth-files?name={encoded}", headers=_mgmt_headers(self.token), timeout=timeout) as resp:
                        text = await resp.text()
                        data = _safe_json(text)
                        return resp.status == 200 and data.get("status") == "ok"
            except Exception:
                return False

        invalid_list = []
        async with aiohttp.ClientSession(connector=connector, timeout=client_timeout, trust_env=True) as session:
            tasks = [asyncio.create_task(probe_one(session, item)) for item in candidates]
            for task in asyncio.as_completed(tasks):
                r = await task
                if r.get("invalid_401") or r.get("invalid_used_percent"):
                    invalid_list.append(r)

            delete_tasks = [asyncio.create_task(delete_one(session, r.get("name"))) for r in invalid_list if r.get("name")]
            deleted_ok = 0
            deleted_fail = 0
            for task in asyncio.as_completed(delete_tasks):
                if await task:
                    deleted_ok += 1
                else:
                    deleted_fail += 1

        return {
            "total": len(files),
            "candidates": len(candidates),
            "invalid_count": len(invalid_list),
            "deleted_ok": deleted_ok,
            "deleted_fail": deleted_fail,
        }

    def probe_and_clean_sync(self, workers: int = 20, timeout: int = 10, retries: int = 1):
        return asyncio.run(self.probe_and_clean_async(workers, timeout, retries))


def _build_cpa_maintainer(args):
    base_url = (args.cpa_base_url or os.getenv("CPA_BASE_URL") or "").strip()
    token = (args.cpa_token or os.getenv("CPA_TOKEN") or "").strip()
    if not base_url or not token:
        print("[CPA] 未提供 cpa_base_url / cpa_token，跳过 CPA 上传/清理")
        return None
    try:
        return MiniPoolMaintainer(
            base_url,
            token,
            target_type="codex",
            used_percent_threshold=args.cpa_used_threshold,
            user_agent=DEFAULT_MGMT_UA,
        )
    except Exception as e:
        print(f"[CPA] 创建维护器失败: {e}")
        return None


def _upload_token_to_cpa(pm, token_json: str, email: str, proxy: str = "") -> bool:
    if not pm:
        return False
    try:
        data = json.loads(token_json)
    except Exception as e:
        print(f"[CPA] 解析 token_json 失败: {e}")
        return False
    fname_email = email.replace("@", "_")
    filename = f"token_{fname_email}_{int(time.time())}.json"
    ok = pm.upload_token(filename=filename, token_data=data, proxy=proxy or "")
    if ok:
        print(f"[CPA] 已上传 {filename} 到 CPA")
    else:
        print("[CPA] 上传失败")
    return ok


def _clean_invalid_in_cpa(pm, args):
    if not pm:
        return None
    try:
        res = pm.probe_and_clean_sync(
            workers=max(1, args.cpa_workers),
            timeout=max(5, args.cpa_timeout),
            retries=max(0, args.cpa_retries),
        )
        print(
            f"[CPA] 清理完成: total={res.get('total')} candidates={res.get('candidates')} "
            f"invalid={res.get('invalid_count')} deleted_ok={res.get('deleted_ok')} deleted_fail={res.get('deleted_fail')}"
        )
        return res
    except Exception as e:
        print(f"[CPA] 清理失败: {e}")
        return None


def _count_valid_cpa_tokens(pm, args):
    if not pm:
        return 0
    try:
        files = pm.fetch_auth_files(timeout=max(5, args.cpa_timeout))
        target = pm.target_type.lower()
        valid = [f for f in files if _get_item_type(f).lower() == target]
        return len(valid)
    except Exception as e:
        print(f"[CPA] 统计 token 失败: {e}")
        return 0


# 账号行清理：上传成功且开启 prune_local 后使用
# 安全处理：文件不存在直接返回，写入保持末尾换行便于追加

def _remove_account_entry(accounts_path: Path, email: str, real_pwd: str):
    if not accounts_path.exists():
        return
    try:
        lines = accounts_path.read_text(encoding="utf-8").splitlines()
        target = f"{email}----{real_pwd}"
        kept = [ln for ln in lines if ln.strip() != target]
        accounts_path.write_text("\n".join(kept) + ("\n" if kept else ""), encoding="utf-8")
        print(f"[本地清理] 已从 accounts.txt 移除: {email}")
    except Exception as e:
        print(f"[本地清理] 移除账号行失败: {e}")

# ========== 主注册流程 (恢复详细日志与异常捕获) ==========

def run(proxy: Optional[str]):
    proxies = {"http": proxy, "https": proxy} if proxy else None
    s = requests.Session(proxies=proxies, impersonate="chrome")
    s.headers.update({"user-agent": UA})

    print(f"\n{'='*20} 开启注册流程 {'='*20}")
    try:
        print("[步骤1] 正在通过 GPTMail 获取邮箱...")
        email, password, code_fetcher = get_email_and_code_fetcher(proxies)
        if not email: return None
        print(f"[成功] 邮箱: {email} | 临时密码: {password}")

        print("[步骤2] 访问 OpenAI 授权页获取 Device ID...")
        oauth = generate_oauth_url()
        s.get(oauth.auth_url, timeout=15)
        did = s.cookies.get("oai-did")
        if not did:
            print("[失败] 未能从 Cookie 获取 oai-did")
            return None
        print(f"[成功] Device ID: {did}")

        print("[步骤3] 正在获取 Sentinel 令牌 (authorize_continue)...")
        sen_token = fetch_sentinel_token(flow="authorize_continue", did=did, proxies=proxies)
        sentinel_header = {"openai-sentinel-token": json.dumps({"p":"", "t":"", "c":sen_token, "id":did, "flow":"authorize_continue"})} if sen_token else {}

        print("[步骤4] 提交注册邮箱表单...")
        signup_res = s.post(
            "https://auth.openai.com/api/accounts/authorize/continue", 
            headers={**sentinel_header, "referer": "https://auth.openai.com/create-account", "content-type": "application/json"},
            data=json.dumps({"username": {"value": email, "kind": "email"}, "screen_hint": "signup"})
        )
        print(f"[日志] 邮箱提交状态: {signup_res.status_code}")
        if signup_res.status_code != 200: return None

        print("[步骤5] 设置账户密码...")
        pwd_res = s.post(
            "https://auth.openai.com/api/accounts/user/register",
            headers={**sentinel_header, "referer": "https://auth.openai.com/create-account/password", "content-type": "application/json"},
            data=json.dumps({"password": password, "username": email})
        )
        print(f"[日志] 密码设置状态: {pwd_res.status_code}")
        if pwd_res.status_code != 200: return None

        print("[步骤6] 触发 OpenAI 发送验证邮件...")
        otp_send_res = s.get("https://auth.openai.com/api/accounts/email-otp/send", 
                             headers={"referer": "https://auth.openai.com/create-account/password"})
        print(f"[日志] 发送指令状态: {otp_send_res.status_code}")
        
        print("[步骤7] 等待邮箱接收 6 位验证码...")
        code = code_fetcher()
        if not code:
            print("[失败] 邮箱长时间未收到验证码")
            return None
        print(f"[成功] 捕获验证码: {code}")

        print("[步骤8] 提交验证码至 OpenAI...")
        val_res = s.post(
            "https://auth.openai.com/api/accounts/email-otp/validate",
            headers={**sentinel_header, "referer": "https://auth.openai.com/email-verification", "content-type": "application/json"},
            data=json.dumps({"code": code})
        )
        print(f"[日志] 验证码校验状态: {val_res.status_code}")
        if val_res.status_code != 200: return None

        print("[步骤9] 完善账户基本信息...")
        so_token = fetch_sentinel_token(flow="oauth_create_account", did=did, proxies=proxies)
        create_headers = {"referer": "https://auth.openai.com/about-you", "content-type": "application/json"}
        if so_token: create_headers["openai-sentinel-so-token"] = so_token
        
        acc_res = s.post("https://auth.openai.com/api/accounts/create_account",
                         headers=create_headers,
                         data=json.dumps({"name": _random_name(), "birthdate": _random_birthdate()}))
        print(f"[日志] 账户创建状态: {acc_res.status_code}")
        if acc_res.status_code != 200: return None

        print("[步骤10] 选择 Workspace 并执行原版无缝 302 重定向...")
        auth_cookie = s.cookies.get("oai-client-auth-session")
        if not auth_cookie:
            print("[失败] 未能获取到 auth-session Cookie")
            return None
            
        ws_id = _decode_jwt_segment(auth_cookie.split(".")[0]).get("workspaces", [{}])[0].get("id")
        select_resp = s.post("https://auth.openai.com/api/accounts/workspace/select",
                      headers={"content-type": "application/json", "referer": "https://auth.openai.com/sign-in-with-chatgpt/codex/consent"},
                      data=json.dumps({"workspace_id": ws_id}))
        
        continue_url = str((select_resp.json() or {}).get("continue_url") or "").strip()
        if not continue_url: 
            print("[错误] 未拿到 continue_url")
            return None

        print(f"[*] 开始重定向追踪 (基于 codex_cli_simplified_flow)...")
        current_url = continue_url
        for i in range(6):
            final_resp = s.get(current_url, allow_redirects=False, timeout=15)
            location = final_resp.headers.get("Location") or ""
            print(f"  -> 重定向 #{i+1} 状态: {final_resp.status_code} | 下一跳: {location[:60] if location else '无'}")
            
            if final_resp.status_code not in [301, 302, 303, 307, 308] or not location:
                break
            
            next_url = urllib.parse.urljoin(current_url, location)
            if "code=" in next_url and "state=" in next_url:
                print("[*] 成功捕获 Code！正在换取最终 Token...")
                token_json = submit_callback_url(
                    callback_url=next_url,
                    expected_state=oauth.state,
                    code_verifier=oauth.code_verifier,
                    redirect_uri=oauth.redirect_uri
                )
                print(f"[大功告成] 账号注册完毕！")
                return token_json, email, password
            current_url = next_url

        print("[失败] 重定向链断裂，未能捕获到 Code。")
        return None
    except Exception as e:
        print(f"[致命错误] 流程崩溃: {e}")
        return None

# ========== Main 保持原版完整结构与输出格式 ==========

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--proxy", help="代理地址")
    # 原始代码
    # parser.add_argument("--once", action="store_true", help="只运行一次")
    
    # 替换为：
    parser.add_argument("--count", type=int, default=1, help="指定运行注册的次数 (0为无限次)")
    parser.add_argument("--sleep-min", type=int, default=5, help="最小间隔(秒)")
    parser.add_argument("--sleep-max", type=int, default=30, help="最大间隔(秒)")

    parser.add_argument("--cpa-base-url", default=os.getenv("CPA_BASE_URL"), help="CPA 基础地址")
    parser.add_argument("--cpa-token", default=os.getenv("CPA_TOKEN"), help="CPA 管理 token (Bearer)")
    parser.add_argument("--cpa-workers", type=int, default=20, help="CPA 清理并发")
    parser.add_argument("--cpa-timeout", type=int, default=12, help="CPA 请求超时")
    parser.add_argument("--cpa-retries", type=int, default=1, help="CPA 清理重试次数")
    parser.add_argument("--cpa-used-threshold", type=int, default=95, help="CPA used_percent 阈值")
    parser.add_argument("--cpa-clean", action="store_true", help="注册后自动清理 CPA 失效账号")
    parser.add_argument("--cpa-upload", action="store_true", help="注册后自动上传 CPA")
    parser.add_argument("--cpa-target-count", type=int, default=300, help="目标 token 数(有效)")
    parser.add_argument("--cpa-prune-local", action="store_true", help="上传成功后删除本地 token 文件与账号行")
    args = parser.parse_args()

    # tokens_dir = OUT_DIR / "tokens"
    # tokens_dir.mkdir(parents=True, exist_ok=True)
    try:
        #修改代码
        os.makedirs("tokens", exist_ok=True) # 确保 Json 文件夹存在
    except Exception:
        pass
    tokens_dir = Path("tokens")

    pm = _build_cpa_maintainer(args)

    count = 0
    while True:
        count += 1
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] >>> 流程 #{count} <<<")

        if pm:
            if args.cpa_clean:
                _clean_invalid_in_cpa(pm, args)
            current_count = _count_valid_cpa_tokens(pm, args)
            print(f"[CPA] 当前有效 token: {current_count} / {args.cpa_target_count}")
            if current_count >= args.cpa_target_count:
                if args.once:
                    break
                wait_time = random.randint(args.sleep_min, args.sleep_max)
                print(f"[*] 随机休息 {wait_time} 秒...")
                time.sleep(wait_time)
                continue

        res = run(args.proxy)
        if res:
            token_json, email, real_pwd = res
            print(f"[🎉] 成功! {email} ---- {real_pwd}")

            # 1. 保存账号密码到 tokens/accounts.txt
            # with open(tokens_dir / "accounts.txt", "a", encoding="utf-8") as f:
            #     f.write(f"{email}----{real_pwd}\n")
            with open(os.path.join("tokens", "accounts.txt"), "a", encoding="utf-8") as f:
                f.write(f"{email}----{real_pwd}\n")

            # 2. 保存详细 Token JSON
            fname_email = email.replace("@", "_")
            # token_file = tokens_dir / f"token_{fname_email}_{int(time.time())}.json"
            # token_file.write_text(token_json, encoding="utf-8")
            file_name = f"token_{fname_email}_{int(time.time())}.json"
            token_file = os.path.join("tokens", file_name)
            try:
                #原始代码
                # file_path.write_text(token_json, encoding="utf-8")
                #修改代码
                with open(token_file, "w", encoding="utf-8") as f:
                    f.write(token_json)
                print(f"[*] 成功! Token 已保存至: {token_file}")
            except Exception as e:
                print(f"[Error] 保存 token 失败: {e}")

            print(f"[*] Token 文件已保存: {token_file}")

            # 3. 上传 CPA（可选）
            upload_ok = False
            if args.cpa_upload:
                upload_ok = _upload_token_to_cpa(pm, token_json, email, proxy=args.proxy or "")

            # 4. 上传成功后按需删除本地文件/账号行
            if upload_ok and args.cpa_prune_local:
                try:
                    if token_file.exists():
                        token_file.unlink()
                        print(f"[本地清理] 已删除 token 文件: {token_file}")
                except Exception as e:
                    print(f"[本地清理] 删除 token 文件失败: {e}")
                _remove_account_entry(tokens_dir / "accounts.txt", email, real_pwd)

            # 5. 注册后再清理一次（可选）
            if pm and args.cpa_clean:
                _clean_invalid_in_cpa(pm, args)
        else:
            print("[-] 本次注册流程未能完成。")

        if args.once:
            break

        wait_time = random.randint(args.sleep_min, args.sleep_max)
        print(f"[*] 随机休息 {wait_time} 秒...")
        time.sleep(wait_time)

if __name__ == "__main__":
    main()
