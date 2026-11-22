"""核心业务逻辑：CSV 解析、目录批次生效、断链核查、结论落库与待复查标记。

一致性设计（对应需求）：
- 一个批次的目录记录与受影响的结论状态在同一个事务里生效（apply_batch）。
- 核查遍历在同一个只读快照事务中进行（run_check），期间补传可并发提交（WAL）。
- 结论落库时在写事务里复查"快照之后新生效的批次"是否影响本结论
  （save_report 中的 REVALIDATE_SQL），保证交错操作不会留下过时的有效结论。
- 同一 URL 同一时刻、出链不同的记录视为冲突，整批退回，不产生任何效果。
"""
import csv
import hashlib
import io
import json
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

REQUIRED_COLUMNS = ("url", "captured_at", "outlinks")

# 清理试算里的整批分类
KIND_CAPTURE = "capture"    # 可删：已生效，整批抓存都不晚于截止时刻且不被结论依赖
KIND_REJECTED = "rejected"  # 留作审计：整批退回、从未生效的批次
KIND_KEEP = "keep"          # 保留：有晚于截止时刻的抓存，或被已保存的结论依赖

# ---------------------------------------------------------------- 时间工具


def to_us(dt):
    """aware datetime -> UTC 微秒整数。"""
    return int(round((dt - EPOCH).total_seconds() * 1_000_000))


def now_us():
    return to_us(datetime.now(timezone.utc))


def us_to_dt(us):
    return EPOCH + timedelta(microseconds=us)


def fmt_us(us):
    if us is None:
        return "—"
    return us_to_dt(us).strftime("%Y-%m-%d %H:%M:%S")


def parse_timestamp(text):
    """解析 ISO 8601 时间；无时区时按 UTC 处理，返回值统一为 UTC。"""
    s = (text or "").strip()
    if not s:
        raise ValueError("时间为空")
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise ValueError(f"无法解析时间: {text!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


# ---------------------------------------------------------------- CSV 解析


def validate_url(u):
    p = urlparse(u)
    return p.scheme in ("http", "https") and bool(p.netloc)


def parse_outlinks(cell):
    """出链单元格：JSON 数组，或以 | 分隔的多个完整 URL。返回排序去重后的列表。"""
    s = (cell or "").strip()
    if not s:
        return []
    if s.startswith("["):
        try:
            data = json.loads(s)
        except json.JSONDecodeError as e:
            raise ValueError(f"出链 JSON 解析失败: {e}")
        if not isinstance(data, list) or not all(isinstance(x, str) for x in data):
            raise ValueError("出链 JSON 必须是字符串数组")
        links = data
    else:
        links = [x.strip() for x in s.split("|") if x.strip()]
    for link in links:
        if not validate_url(link):
            raise ValueError(f"出链不是完整 URL: {link!r}")
    return sorted(set(links))


def parse_catalog(file_bytes):
    """解析目录 CSV。

    成功返回 (rows, None)，rows 为 [(url, captured_at_us, outlinks)]，
    文件内完全重复的记录已去重；失败返回 (None, [错误信息...])，包括
    文件内同 URL 同时刻但出链不同的冲突。
    """
    try:
        text = file_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        return None, ["文件不是有效的 UTF-8 文本"]
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return None, ["空文件或缺少表头"]
    headers = {(h or "").strip().lower(): h for h in reader.fieldnames}
    missing_cols = [c for c in REQUIRED_COLUMNS if c not in headers]
    if missing_cols:
        return None, [
            "缺少列: " + ", ".join(missing_cols) + "（需要 url, captured_at, outlinks）"
        ]
    rows = []
    errors = []
    seen = {}
    for lineno, raw in enumerate(reader, start=2):
        url = (raw.get(headers["url"]) or "").strip()
        ts = raw.get(headers["captured_at"]) or ""
        cell = raw.get(headers["outlinks"]) or ""
        if not validate_url(url):
            errors.append(f"第{lineno}行: 页面地址不是完整 URL: {url!r}")
            continue
        try:
            us = to_us(parse_timestamp(ts))
        except ValueError as e:
            errors.append(f"第{lineno}行: {e}")
            continue
        try:
            links = parse_outlinks(cell)
        except ValueError as e:
            errors.append(f"第{lineno}行: {e}")
            continue
        key = (url, us)
        if key in seen:
            if seen[key] != links:
                errors.append(
                    f"第{lineno}行: 与文件内先前记录冲突（同 URL 同时刻但出链不同）: "
                    f"{url} @ {fmt_us(us)}"
                )
            continue
        seen[key] = links
        rows.append((url, us, links))
    if errors:
        if len(errors) > 50:
            errors = errors[:50] + [f"……共 {len(errors)} 处问题"]
        return None, errors
    return rows, None


# ---------------------------------------------------------------- 批次生效

# 批次生效的同时，把受影响的"有效"结论标为待复查（同一事务）。
# 触发条件（且仅这两个条件）：
#   1) 结论中某页面出现了更晚但仍不晚于回看时刻的抓存 —— 核查所选抓存会变；
#   2) 结论中的缺档 URL 出现了不晚于回看时刻的抓存 —— 缺档被补上。
INVALIDATE_SQL = """
UPDATE reports SET status = 'needs_recheck'
WHERE status = 'valid' AND EXISTS (
    SELECT 1 FROM report_pages rp
    JOIN captures c ON c.url = rp.url
    WHERE rp.report_id = reports.id
      AND c.batch_id = ?
      AND c.captured_at <= reports.lookback
      AND (rp.captured_at IS NULL OR c.captured_at > rp.captured_at)
)
"""


def _record_rejected_batch(conn, filename, row_count, error, detail=None):
    """退回的批次也留痕（独立事务，便于审计）。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT INTO batches(uploaded_at, filename, row_count, applied, error, detail)"
            " VALUES (?,?,?,0,?,?)",
            (now_us(), filename, row_count, error, detail),
        )
        conn.execute("COMMIT")
        return cur.lastrowid
    except Exception:
        conn.execute("ROLLBACK")
        raise


def record_rejected_batch(conn, filename, errors):
    return _record_rejected_batch(
        conn, filename, 0, "整批退回：" + "；".join(errors[:10])
    )


def apply_batch(conn, filename, rows):
    """把一个已解析的目录批次写入档案库，并同步刷新结论状态。

    返回 dict(batch_id, applied, inserted, skipped, conflicts)。
    若与既有档案存在同 URL 同时刻但出链不同的冲突：整批退回，
    不写入任何记录（仅登记一条"已退回"的批次记录）。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        duplicates = set()
        conflicts = []
        for url, us, links in rows:
            r = conn.execute(
                "SELECT outlinks FROM captures WHERE url=? AND captured_at=?",
                (url, us),
            ).fetchone()
            if r is None:
                continue
            old = json.loads(r["outlinks"])
            if old != links:
                conflicts.append(
                    {"url": url, "captured_at": us, "existing": old, "new": links}
                )
            else:
                duplicates.add((url, us))
        if conflicts:
            conn.execute("ROLLBACK")
            bid = _record_rejected_batch(
                conn,
                filename,
                len(rows),
                f"与既有档案冲突 {len(conflicts)} 处（同 URL 同时刻出链不同），整批退回",
                json.dumps(conflicts[:50], ensure_ascii=False),
            )
            return {
                "batch_id": bid,
                "applied": False,
                "inserted": 0,
                "skipped": 0,
                "conflicts": conflicts,
            }

        cur = conn.execute(
            "INSERT INTO batches(uploaded_at, filename, row_count, applied)"
            " VALUES (?,?,?,1)",
            (now_us(), filename, len(rows)),
        )
        bid = cur.lastrowid
        inserted = 0
        for url, us, links in rows:
            if (url, us) in duplicates:
                continue
            conn.execute(
                "INSERT INTO captures(batch_id, url, captured_at, outlinks)"
                " VALUES (?,?,?,?)",
                (bid, url, us, json.dumps(links, ensure_ascii=False)),
            )
            inserted += 1
        # 目录与结论状态同步生效
        conn.execute(INVALIDATE_SQL, (bid,))
        conn.execute("COMMIT")
        return {
            "batch_id": bid,
            "applied": True,
            "inserted": inserted,
            "skipped": len(rows) - inserted,
            "conflicts": [],
        }
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise


# ---------------------------------------------------------------- 断链核查


def _walk_check(conn, entry_url, lookback_us, step_delay=0.0):
    """在调用方已开启的事务内完成遍历（不自行 BEGIN/COMMIT）。"""
    base_batch = conn.execute(
        "SELECT COALESCE(MAX(id), 0) FROM batches WHERE applied = 1"
    ).fetchone()[0]
    visited = {}
    missing = {}
    queue = deque([(entry_url, [entry_url])])
    enqueued = {entry_url}
    while queue:
        url, path = queue.popleft()
        if url in visited or url in missing:
            continue
        row = conn.execute(
            "SELECT id, captured_at, outlinks FROM captures"
            " WHERE url=? AND captured_at<=?"
            " ORDER BY captured_at DESC, id DESC LIMIT 1",
            (url, lookback_us),
        ).fetchone()
        if row is None:
            missing[url] = path
        else:
            visited[url] = (row["id"], row["captured_at"])
            for link in json.loads(row["outlinks"]):
                if link not in enqueued and link not in visited and link not in missing:
                    enqueued.add(link)
                    queue.append((link, path + [link]))
        if step_delay:
            time.sleep(step_delay)
    return base_batch, visited, missing


def run_check(conn, entry_url, lookback_us, step_delay=0.0):
    """在同一目录快照上从入口沿出链逐页核查（BFS，互链只查一遍）。

    返回 (base_batch, visited, missing)：
    - base_batch: 快照可见的最大已生效批次号；
    - visited:    {url: (capture_id, captured_at)}，每个 URL 取不晚于
                  回看时刻的最近一条抓存；
    - missing:    {url: [入口, ..., 该URL]}，缺档 URL 及到达路径。
    """
    conn.execute("BEGIN")  # 只读快照事务；WAL 下不阻塞并发补传
    try:
        result = _walk_check(conn, entry_url, lookback_us, step_delay)
        conn.execute("COMMIT")
        return result
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise


# 结论落库时复查：快照之后新生效的批次是否已影响本结论，
# 若是则直接标为待复查，避免交错操作留下过时的"有效"结论。
REVALIDATE_SQL = """
UPDATE reports SET status = 'needs_recheck'
WHERE id = ? AND EXISTS (
    SELECT 1 FROM report_pages rp
    JOIN captures c ON c.url = rp.url
    WHERE rp.report_id = ?
      AND c.batch_id > ?
      AND c.captured_at <= ?
      AND (rp.captured_at IS NULL OR c.captured_at > rp.captured_at)
)
"""


def save_report(conn, entry_url, lookback_us, base_batch, visited, missing):
    """把核查结论及其逐页明细落库（单个写事务），返回报告 id。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            "INSERT INTO reports(created_at, entry_url, lookback, base_batch,"
            " status, visited_count, missing_count) VALUES (?,?,?,?,'valid',?,?)",
            (now_us(), entry_url, lookback_us, base_batch, len(visited), len(missing)),
        )
        rid = cur.lastrowid
        for url, (capture_id, captured_at) in visited.items():
            conn.execute(
                "INSERT INTO report_pages(report_id, url, captured_at, capture_id, path)"
                " VALUES (?,?,?,?,NULL)",
                (rid, url, captured_at, capture_id),
            )
        for url, path in missing.items():
            conn.execute(
                "INSERT INTO report_pages(report_id, url, captured_at, capture_id, path)"
                " VALUES (?,?,NULL,NULL,?)",
                (rid, url, json.dumps(path, ensure_ascii=False)),
            )
        conn.execute(REVALIDATE_SQL, (rid, rid, base_batch, lookback_us))
        conn.execute("COMMIT")
        return rid
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise


# ---------------------------------------------------------------- 清理试算与确认
#
# 目标：馆员清理旧网站抓存时，已保存的断链结论必须仍能复查。
#
# 清理粒度是"整批"——一个批次要么整批删除，要么整批保留。只有同时满足以下
# 条件的已生效批次才可删：
#   1. 批内每一条抓存都不晚于清理截止时刻（截止后的抓存不动）；
#   2. 批内没有任何一条抓存是任一已保存结论的"所选抓存"。结论的逐页明细
#      记录着 capture_id；同一 URL 在不同回看时刻可能选中不同批次的抓存，
#      因此不能按 URL 一概而论，必须逐份核对每个结论实际选中的抓存。
# 缺档 URL 和到达路径不需要额外保护：删除抓存只会让可用抓存变少，不会凭空
# 补上缺口；已查页面所选抓存都保留时，遍历结果（含路径）必然不变。
#
# 确认时先比对试算指纹：试算后目录或结论有任何变化（补传、重查、状态变化、
# 新增结论）就拒绝。否则在同一个写事务里删批次+抓存、写清理结果，并比较
# 删除前后"每条结论按原入口/回看时刻重查"的结果；任何一步失败整体回滚。

KIND_CAPTURE = "capture"    # 可删：整批可删的已生效批次
KIND_REJECTED = "rejected"  # 留作审计：整批退回、从未生效的批次
KIND_KEEP = "keep"          # 保留：截止后的抓存、被结论依赖，或空批次留痕

CLEANUP_PLAN_VERSION = 1


def _fingerprint(conn):
    """目录（批次 + 抓存）与结论（报告 + 逐页明细）的摘要。

    任何可能影响"哪些批次可删"或"结论能否复查"的变化都会改变摘要：
    补传、批次退回、新增/重查结论、结论状态变化、逐页明细变化。
    """
    h = hashlib.sha256()

    def add(tag, value):
        h.update(tag.encode("utf-8"))
        h.update(b"\x1f")
        h.update(str(value).encode("utf-8"))
        h.update(b"\x1e")

    for b in conn.execute(
        "SELECT id, applied, row_count, error FROM batches ORDER BY id"
    ):
        add("b", (b["id"], b["applied"], b["row_count"], b["error"]))
    for c in conn.execute(
        "SELECT id, batch_id, url, captured_at, outlinks FROM captures ORDER BY id"
    ):
        add("c", (c["id"], c["batch_id"], c["url"], c["captured_at"], c["outlinks"]))
    for r in conn.execute(
        "SELECT id, entry_url, lookback, base_batch, status, visited_count,"
        " missing_count FROM reports ORDER BY id"
    ):
        add("r", tuple(r))
    for p in conn.execute(
        "SELECT report_id, url, captured_at, capture_id, path FROM report_pages"
        " ORDER BY report_id, url"
    ):
        add("p", tuple(p))
    return h.hexdigest()


def fingerprint_of(conn):
    """当前目录与结论的指纹（供页面判断试算是否已过时）。"""
    return _fingerprint(conn)


def _batch_plans(conn, cutoff_us):
    """按截止时刻给每个批次分类，返回 {batch_id: 计划项}。"""
    plans = {}
    for b in conn.execute(
        "SELECT id, uploaded_at, filename, row_count, applied, error"
        " FROM batches ORDER BY id"
    ):
        plans[b["id"]] = {
            "batch_id": b["id"],
            "filename": b["filename"],
            "uploaded_at": b["uploaded_at"],
            "row_count": b["row_count"],
            "applied": bool(b["applied"]),
            "error": b["error"],
            "capture_count": 0,
            "deletable_count": 0,
            "late_count": 0,
            "latest_capture": None,    # 批次内最晚抓存（UTC 微秒）
            "kind": KIND_REJECTED if not b["applied"] else KIND_CAPTURE,
            "reasons": (
                ["整批退回的批次留作审计，不参与清理"] if not b["applied"] else []
            ),
            # [{report_id, lookback, url, capture_id, captured_at, source}]
            "dependencies": [],
        }
    for c in conn.execute("SELECT batch_id, captured_at FROM captures ORDER BY id"):
        p = plans[c["batch_id"]]
        p["capture_count"] += 1
        cat = c["captured_at"]
        if p["latest_capture"] is None or cat > p["latest_capture"]:
            p["latest_capture"] = cat
        if cat <= cutoff_us:
            p["deletable_count"] += 1
        else:
            p["late_count"] += 1
    for p in plans.values():
        if p["kind"] == KIND_CAPTURE and p["late_count"]:
            p["kind"] = KIND_KEEP
            p["reasons"].append(
                f"批次内有 {p['late_count']} 条抓存晚于截止时刻，不能整批删除"
            )
        if p["kind"] == KIND_CAPTURE and p["capture_count"] == 0:
            p["kind"] = KIND_KEEP
            p["reasons"].append("批次没有抓存记录，保留批次留痕")
    return plans


def _walk_for_reports(conn):
    """按每条已保存结论的原入口/回看时刻重新遍历，返回 {report_id: (visited, missing)}。

    这是"结论此刻再查一遍会得到什么"的基线：清理只允许删除不会改变这份基线的
    抓存，因此同时也覆盖了同一 URL 在不同回看时刻选中不同替代抓存的情形。
    """
    out = {}
    for rep in conn.execute(
        "SELECT id, entry_url, lookback FROM reports ORDER BY id"
    ):
        _base, visited, missing = _walk_check(conn, rep["entry_url"], rep["lookback"])
        out[rep["id"]] = (visited, missing)
    return out


def _apply_dependencies(conn, plans, rechecks):
    """标出每个批次仍被哪些已保存结论依赖。

    保护集合取并集：
    - 已保存结论逐页明细里记录的 capture_id（结论当时所选抓存，删了明细指空）；
    - 按每条结论原入口/回看时刻重查时实际会选中的 capture_id（含同一 URL 在
      不同回看时刻各自选中的替代抓存）。
    两类抓存所在批次都不可删，并在页面上逐条标出依赖它的结论。
    """
    reports = {
        r["id"]: r
        for r in conn.execute(
            "SELECT id, entry_url, lookback, status FROM reports ORDER BY id"
        )
    }
    capture_info = {}
    for c in conn.execute(
        "SELECT id, batch_id, url, captured_at FROM captures ORDER BY id"
    ):
        capture_info[c["id"]] = (c["batch_id"], c["url"], c["captured_at"])

    def add(capture_id, report_id, source):
        info = capture_info.get(capture_id)
        if info is None:
            return
        bid, url, cat = info
        p = plans.get(bid)
        rep = reports.get(report_id)
        if p is None or rep is None:
            return
        if any(
            d["report_id"] == report_id and d["capture_id"] == capture_id
            for d in p["dependencies"]
        ):
            return
        p["dependencies"].append(
            {
                "report_id": report_id,
                "entry_url": rep["entry_url"],
                "lookback": rep["lookback"],
                "status": rep["status"],
                "url": url,
                "capture_id": capture_id,
                "captured_at": cat,
                "source": source,
            }
        )

    # 1) 结论落库时选中的抓存
    for page in conn.execute(
        "SELECT report_id, url, captured_at, capture_id FROM report_pages"
        " WHERE capture_id IS NOT NULL ORDER BY report_id, url"
    ):
        add(page["capture_id"], page["report_id"], "stored")
    # 2) 此刻重查会选中的抓存（含同一 URL 在不同回看时刻的替代抓存）
    for rid, (visited, _missing) in rechecks.items():
        for url, (cid, _cat) in visited.items():
            add(cid, rid, "recheck")

    for p in plans.values():
        if not p["dependencies"]:
            continue
        if p["kind"] == KIND_CAPTURE:
            p["kind"] = KIND_KEEP
        p["reasons"].append(
            "被已保存结论依赖：" + "、".join(
                sorted({f"结论 #{d['report_id']}" for d in p["dependencies"]})
            )
        )


def _compute_plan(conn, cutoff_us):
    """在当前事务内计算清理计划（不自起事务、不写库）。

    返回 (plan, rechecks)；rechecks 是每条结论此刻重查会得到的逐页结果，
    既用于标注依赖，也可直接作为确认时的删除前基线，避免重复遍历。
    """
    plans = _batch_plans(conn, cutoff_us)
    rechecks = _walk_for_reports(conn)
    _apply_dependencies(conn, plans, rechecks)
    deletable = []
    kept = []
    for bid in sorted(plans):
        p = plans[bid]
        if p["kind"] == KIND_CAPTURE:
            p["deletable"] = True
            deletable.append(p)
        else:
            p["deletable"] = False
            kept.append(p)
    return {
        "version": CLEANUP_PLAN_VERSION,
        "cutoff": cutoff_us,
        "deletable": deletable,
        "kept": kept,
    }, rechecks


def create_cleanup_preview(conn, cutoff_us):
    """计算试算并落库，返回 (preview_id, plan)。

    先在只读快照里计算（不阻塞并发补传）；落库前在写事务里核对指纹，
    若期间目录或结论发生了变化就整份重算，保证保存的试算与其指纹一致。
    """
    conn.execute("BEGIN")
    try:
        plan, _rechecks = _compute_plan(conn, cutoff_us)
        fp = _fingerprint(conn)
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    conn.execute("BEGIN IMMEDIATE")
    try:
        if _fingerprint(conn) != fp:
            # 计算之后有变化：在写事务里重算，任何并发写都无法再插入。
            plan, _rechecks = _compute_plan(conn, cutoff_us)
            fp = _fingerprint(conn)
        cur = conn.execute(
            "INSERT INTO cleanup_previews(created_at, cutoff, fingerprint, plan)"
            " VALUES (?,?,?,?)",
            (now_us(), cutoff_us, fp, json.dumps(plan, ensure_ascii=False)),
        )
        pid = cur.lastrowid
        conn.execute("COMMIT")
        return pid, plan
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise


class CleanupRejected(Exception):
    """确认清理被拒绝；调用方已回滚，不产生任何副作用。"""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def _recheck_diff(before, after):
    """比较清理前后的"重查结果"，返回发生变化的结论 id。

    before/after 均为 {report_id: (visited, missing)}。visited 里含抓存 id 与
    时刻，missing 里含到达路径；任何一项不同都说明结论不再能原样复查。
    """
    changed = []
    for rid in sorted(set(before) | set(after)):
        if rid not in before or rid not in after:
            changed.append(rid)
            continue
        b_visited, b_missing = _normalize_check(*before[rid])
        a_visited, a_missing = _normalize_check(*after[rid])
        if b_visited != a_visited or b_missing != a_missing:
            changed.append(rid)
    return changed


def _normalize_check(visited, missing):
    """把一次遍历结果规范化，便于比较（字典顺序与 JSON 序列化一致）。"""
    return (
        {url: tuple(v) for url, v in visited.items()},
        {url: json.dumps(path, ensure_ascii=False) for url, path in missing.items()},
    )


def _missing_capture_references(conn):
    """已保存结论明细引用的抓存是否仍然存在。返回指向缺失抓存的结论 id。"""
    rows = conn.execute(
        "SELECT DISTINCT rp.report_id FROM report_pages rp"
        " LEFT JOIN captures c ON c.id = rp.capture_id"
        " WHERE rp.capture_id IS NOT NULL AND c.id IS NULL"
    ).fetchall()
    return [r["report_id"] for r in rows]


def confirm_cleanup(conn, preview_id):
    """按已保存的试算确认清理，返回结果摘要。

    - 试算不存在 / 已确认过 / 试算后目录或结论变化 → CleanupRejected；
    - 删除批次与抓存、写清理结果在同一写事务内提交；
    - 提交前重核全部有效结论，任何变化都整体回滚，原档案保持原样。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        prev = conn.execute(
            "SELECT * FROM cleanup_previews WHERE id=?", (preview_id,)
        ).fetchone()
        if prev is None:
            raise CleanupRejected("清理试算不存在")
        if prev["cleanup_id"] is not None:
            raise CleanupRejected("该清理试算已确认过，不能重复执行")
        cutoff = prev["cutoff"]
        if _fingerprint(conn) != prev["fingerprint"]:
            raise CleanupRejected(
                "试算后目录或结论已发生变化，请重新试算后再确认"
            )
        saved = json.loads(prev["plan"])
        current, baseline = _compute_plan(conn, cutoff)
        expected = [p["batch_id"] for p in saved["deletable"]]
        actual = [p["batch_id"] for p in current["deletable"]]
        if expected != actual:
            raise CleanupRejected(
                "试算后目录或结论已发生变化，可删批次不再一致，请重新试算"
            )

        deleted_batches = []
        deleted_capture_count = 0
        for p in current["deletable"]:
            bid = p["batch_id"]
            rows = conn.execute(
                "SELECT id, url, captured_at, outlinks FROM captures"
                " WHERE batch_id=? ORDER BY captured_at, id",
                (bid,),
            ).fetchall()
            if any(r["captured_at"] > cutoff for r in rows):
                raise CleanupRejected(
                    f"批次 #{bid} 含晚于截止时刻的抓存，不能整批删除"
                )
            conn.executemany(
                "DELETE FROM captures WHERE id=?", [(r["id"],) for r in rows]
            )
            conn.execute("DELETE FROM batches WHERE id=?", (bid,))
            deleted_capture_count += len(rows)
            deleted_batches.append(
                {
                    "batch_id": bid,
                    "filename": p["filename"],
                    "captures": [
                        {"url": r["url"], "captured_at": r["captured_at"]}
                        for r in rows
                    ],
                }
            )
        broken = _missing_capture_references(conn)
        if broken:
            raise CleanupRejected(
                "清理会使已保存结论 #" + "、#".join(str(r) for r in broken)
                + " 的明细指向已删除抓存，已整体取消"
            )
        changed = _recheck_diff(baseline, _walk_for_reports(conn))
        if changed:
            raise CleanupRejected(
                "清理会改变已保存的结论 "
                + "、".join(f"#{rid}" for rid in changed)
                + "，已整体取消"
            )

        detail = {
            "cutoff": cutoff,
            "deletable": saved.get("deletable", []),
            "kept": saved.get("kept", []),
            "batches": deleted_batches,
        }
        cur = conn.execute(
            "INSERT INTO cleanups(preview_id, created_at, cutoff,"
            " deleted_batch_count, deleted_capture_count, detail)"
            " VALUES (?,?,?,?,?,?)",
            (
                preview_id,
                now_us(),
                cutoff,
                len(deleted_batches),
                deleted_capture_count,
                json.dumps(detail, ensure_ascii=False),
            ),
        )
        cid = cur.lastrowid
        conn.execute(
            "UPDATE cleanup_previews SET cleanup_id=? WHERE id=?",
            (cid, preview_id),
        )
        conn.execute("COMMIT")
        return {
            "cleanup_id": cid,
            "cutoff": cutoff,
            "deleted_batch_count": len(deleted_batches),
            "deleted_capture_count": deleted_capture_count,
            "batch_ids": [b["batch_id"] for b in deleted_batches],
        }
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
