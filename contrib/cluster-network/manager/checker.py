#!/usr/bin/env python3
"""Task classification, prompt building, and success checking for the cluster manager.

Checks, in order of trust:
  1. tests  - code + provided tests run in a bwrap sandbox (no network, read-only /usr, tmp workdir, timeout)
  2. expect - deterministic text checks (substrings / exact match) given with the task
  3. rubric - desktop checker model (gemma4:12b, think=false, JSON) grades against the instructions
Code without tests: py_compile + sandboxed import/run + rubric.
"""
from __future__ import annotations

import json
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

from clusterlib import PowerSampler, ollama_chat

COMPLEX_KW = (
    "architecture", "design a ", "design an ", "system design", "refactor", "multi-file", "multiple files",
    "prove ", "proof", "trade-off", "tradeoff", "in depth", "in-depth", "analyze ", "analyse ", "compare ",
    "optimi", "concurren", "distributed", "security review", "threat model", "strategy", "research",
    "explain why", "derive ", "step by step", "plan for", "migration", "benchmark plan",
)
TRANSFORM_KW = ("convert", "reformat", "rewrite", "uppercase", "lowercase", "title case", "to json", "to csv",
                "sort ", "extract", "rename", "translate", "fix the grammar", "turn this", "transform")
SUMMARY_KW = ("summarize", "summarise", "tl;dr", "tldr", "one sentence", "bullet points")
CODE_HINTS = ("```", "def ", "python", "function", "script", "regex", "class ", "unit test", "write code", "implement")


def classify(task: dict) -> dict:
    text = task.get("text") or ""
    low = text.lower()
    ttype = task.get("type") or "auto"
    if ttype == "auto":
        ttype = "code" if (task.get("tests") or any(h in low for h in CODE_HINTS)) else "text"
    if ttype == "code":
        category = "code"
    elif any(k in low for k in SUMMARY_KW):
        category = "summarize"
    elif any(k in low for k in TRANSFORM_KW):
        category = "text-transform"
    else:
        category = "qa"
    reasons = []
    forced = task.get("complexity") or "auto"
    if forced in ("simple", "complex"):
        complexity = forced
        reasons.append(f"forced {forced}")
    else:
        hits = [k.strip() for k in COMPLEX_KW if k in low]
        if hits:
            reasons.append("keywords: " + ", ".join(hits[:4]))
        if len(text) > 1500:
            reasons.append(f"long prompt ({len(text)} chars)")
        if ttype == "code" and len((task.get("tests") or "").splitlines()) > 80:
            reasons.append("large test suite")
        complexity = "complex" if reasons else "simple"
        if not reasons:
            reasons.append(f"short {category} task ({len(text)} chars), no complexity markers")
    return {"type": ttype, "category": category, "complexity": complexity, "reason": "; ".join(reasons), "method": "heuristic"}


def system_prompt(cls: dict) -> str:
    if cls["type"] == "code":
        return ("You are a careful Python 3 programmer. Return ONLY one ```python fenced code block containing the "
                "complete, importable solution (functions/classes at top level, no input(), no tests, no prints at import time).")
    return "Follow the instructions exactly. Return only the requested output, with no preamble or explanation."


def build_messages(task: dict, cls: dict, prev_output: str | None, fix_requests: list[str] | None) -> list[dict]:
    user = task["text"]
    if cls["type"] == "code" and task.get("tests") and not task.get("hidden_tests"):
        user += "\n\nYour code must pass these tests (they do `from solution import *`):\n```python\n" + task["tests"] + "\n```"
    msgs = [{"role": "system", "content": system_prompt(cls)}, {"role": "user", "content": user}]
    if prev_output is not None and fix_requests:
        msgs.append({"role": "assistant", "content": prev_output})
        msgs.append({"role": "user", "content": (
            "Your previous answer FAILED the manager's check. Fix requests:\n- " + "\n- ".join(fix_requests)
            + "\n\nReturn the complete corrected answer in the same format.")})
    return msgs


# ------------------------------------------------------------------ code extraction + sandbox
FENCE = re.compile(r"```(?:python|py|python3)?[ \t]*\n(.*?)```", re.S | re.I)


def extract_code(output: str) -> str:
    blocks = FENCE.findall(output or "")
    if blocks:
        return max(blocks, key=len).strip() + "\n"
    return (output or "").strip() + "\n"


def bwrap_argv(workdir: str, cmd: list[str]) -> list[str]:
    if not shutil.which("bwrap"):
        raise RuntimeError("bwrap not installed; refusing to run untrusted code without a sandbox")
    return ["bwrap", "--ro-bind", "/usr", "/usr", "--symlink", "usr/lib", "/lib", "--symlink", "usr/lib64", "/lib64",
            "--symlink", "usr/bin", "/bin", "--symlink", "usr/bin", "/sbin", "--proc", "/proc", "--dev", "/dev",
            "--tmpfs", "/tmp", "--bind", workdir, "/work", "--chdir", "/work", "--unshare-all", "--die-with-parent",
            "--new-session", "--clearenv", "--setenv", "PATH", "/usr/bin", "--setenv", "HOME", "/work",
            "--setenv", "PYTHONDONTWRITEBYTECODE", "1"] + cmd


def sandbox_run(files: dict[str, str], cmd: list[str], timeout_s: int = 20) -> dict:
    d = tempfile.mkdtemp(prefix="cluster-sbx-")
    try:
        for name, content in files.items():
            Path(d, name).write_text(content, encoding="utf-8")
        # prlimit: 2 GiB address space, CPU seconds = timeout (bwrap: no net, ro /usr, tmpfs /tmp)
        argv = ["prlimit", "--as=2147483648", "--cpu=" + str(timeout_s)] + bwrap_argv(d, cmd)
        t0 = time.monotonic()
        try:
            p = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_s + 5)
            rc, out, err = p.returncode, p.stdout, p.stderr
        except subprocess.TimeoutExpired as e:
            rc, out, err = 124, (e.stdout or b"").decode() if isinstance(e.stdout, bytes) else (e.stdout or ""), "TIMEOUT"
        return {"rc": rc, "stdout": out[-3000:], "stderr": err[-3000:], "s": round(time.monotonic() - t0, 2)}
    finally:
        shutil.rmtree(d, ignore_errors=True)


def failure_lines(text: str, limit: int = 6) -> list[str]:
    lines = [l.rstrip() for l in (text or "").splitlines() if l.strip()]
    keep = [l for l in lines if re.search(r"(Error|assert|FAIL|Traceback|expected|!=|==|line \d+)", l)]
    keep = keep or lines
    return [l[:300] for l in keep[-limit:]]


def check_code_tests(task: dict, output: str) -> dict:
    code = extract_code(output)
    tests = task["tests"]
    if "from solution import" not in tests and "import solution" not in tests:
        tests = "from solution import *\n" + tests
    comp = sandbox_run({"solution.py": code}, ["python3", "-E", "-s", "-B", "-m", "py_compile", "solution.py"], 15)
    if comp["rc"] != 0:
        return {"pass": False, "method": "tests", "issues": ["Code does not compile: " + " | ".join(failure_lines(comp["stderr"], 3))],
                "detail": comp}
    cmd = ["python3", "-E", "-s", "-B", "-m", "unittest", "-v", "test_solution"] if "unittest" in tests else ["python3", "-E", "-s", "-B", "test_solution.py"]
    run = sandbox_run({"solution.py": code, "test_solution.py": tests}, cmd, 20)
    if run["rc"] == 0:
        return {"pass": True, "method": "tests", "issues": [], "detail": {"rc": 0, "s": run["s"]}}
    issues = ["Tests failed (rc=%s). Failing output: %s" % (run["rc"], " | ".join(failure_lines(run["stderr"] + "\n" + run["stdout"])))]
    return {"pass": False, "method": "tests", "issues": issues, "detail": run}


def check_expect(task: dict, output: str) -> dict | None:
    exp = task.get("expect") or []
    exact = task.get("expect_exact")
    if not exp and exact is None:
        return None
    issues = []
    out = (output or "").strip()
    norm = lambda s: re.sub(r"\s+", " ", s.strip())
    if exact is not None and norm(out) != norm(exact):
        issues.append(f"Output must be exactly: {exact!r} (got {out[:200]!r}).")
    for s in exp:
        if s not in out:
            issues.append(f"Output must contain {s!r}.")
    return {"pass": not issues, "method": "expect", "issues": issues}


RUBRIC = """You are the cluster manager's strict reviewer. Grade the ANSWER against the TASK.
Criteria: follows every instruction (format, length, constraints), correct, complete, no extra chatter.
Return ONLY JSON: {{"pass": true|false, "score": 0-10, "issues": ["specific, actionable fix request", ...]}}
Pass only if score >= 7 and there are no instruction violations.

TASK:
{task}

ANSWER:
{answer}
"""


def check_rubric(task: dict, output: str, ollama: str, model: str, keep_alive: str, power_kind: str) -> dict:
    msgs = [{"role": "user", "content": RUBRIC.format(task=task["text"][:6000], answer=(output or "")[:8000])}]
    with PowerSampler(power_kind) as ps:
        r = ollama_chat(ollama, model, msgs, options={"temperature": 0, "num_ctx": 8192}, keep_alive=keep_alive,
                        think=False, fmt="json", timeout=600)
    j = ps.joules()
    base = {"method": "rubric", "checker_model": model, "check_energy_j": round(j, 2) if j else None}
    if not r.get("ok"):
        return {**base, "pass": False, "issues": [f"checker error: {r.get('error')}"], "checker_error": True}
    try:
        v = json.loads(r["content"])
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", r["content"], re.S)
        v = json.loads(m.group(0)) if m else {"pass": False, "score": 0, "issues": ["checker returned non-JSON"]}
    score = v.get("score")
    try:
        score = float(score)
    except (TypeError, ValueError):
        score = 0.0
    passed = bool(v.get("pass")) and score >= 7
    issues = [str(i)[:400] for i in (v.get("issues") or [])][:6]
    if not passed and not issues:
        issues = [f"Reviewer score {score}/10; improve correctness and follow the instructions exactly."]
    return {**base, "pass": passed, "score": score, "issues": issues if not passed else [], "notes": issues if passed else []}


def check(task: dict, cls: dict, output: str, ollama: str, checker_model: str, keep_alive: str, power_kind: str) -> dict:
    t0 = time.monotonic()
    if not (output or "").strip():
        v = {"pass": False, "method": "empty", "issues": ["The answer was empty. Produce the full answer."]}
    elif cls["type"] == "code" and task.get("tests"):
        v = check_code_tests(task, output)
    else:
        v = check_expect(task, output)
        if v is None or v["pass"]:
            if cls["type"] == "code":
                code = extract_code(output)
                run = sandbox_run({"solution.py": code}, ["python3", "-E", "-s", "-B", "solution.py"], 15)
                if run["rc"] != 0:
                    v = {"pass": False, "method": "run", "issues": ["Code crashed when run: " + " | ".join(failure_lines(run["stderr"], 4))]}
                else:
                    v = check_rubric(task, output, ollama, checker_model, keep_alive, power_kind)
            elif task.get("skip_rubric") and v is not None:
                pass
            else:
                exp_v = v
                v = check_rubric(task, output, ollama, checker_model, keep_alive, power_kind)
                if exp_v is not None:
                    v["method"] = "expect+rubric"
    v["check_s"] = round(time.monotonic() - t0, 2)
    return v
