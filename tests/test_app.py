"""断链核查应用的自测：断链识别、补传标复查、交错核查、冲突退回、重启持久化。"""
import csv
import io
import json
import os
import sys
import threading
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app  # noqa: E402
from app import logic  # noqa: E402
from app.db import get_db  # noqa: E402

T1 = "2024-01-01T00:00:00Z"
T2 = "2024-02-01T00:00:00Z"
T3 = "2024-03-01T00:00:00Z"
LOOKBACK = "2024-06-01T00:00"  # datetime-local 格式，按 UTC


def make_csv(rows):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["url", "captured_at", "outlinks"])
    for url, ts, links in rows:
        w.writerow([url, ts, "|".join(links)])
    return buf.getvalue().encode()


def upload(client, rows, filename="catalog.csv"):
    return client.post(
        "/uploads",
        data={"file": (io.BytesIO(make_csv(rows)), filename)},
        content_type="multipart/form-data",
    )


def check(client, entry, lookback=LOOKBACK):
    resp = client.post(
        "/checks", data={"entry_url": entry, "lookback": lookback}
    )
    assert resp.status_code == 302
    return int(resp.headers["Location"].rstrip("/").split("/")[-1])


def q(app, sql, args=()):
    with app.app_context():
        return get_db().execute(sql, args).fetchall()


def one(app, sql, args=()):
    rows = q(app, sql, args)
    assert len(rows) == 1
    return rows[0]


def report(app, rid):
    return one(app, "SELECT * FROM reports WHERE id=?", (rid,))


def pages(app, rid):
    return q(app, "SELECT * FROM report_pages WHERE report_id=?", (rid,))


def missing_map(app, rid):
    return {
        p["url"]: json.loads(p["path"])
        for p in pages(app, rid)
        if p["captured_at"] is None
    }


def resolved_map(app, rid):
    return {p["url"]: p["captured_at"] for p in pages(app, rid) if p["captured_at"] is not None}


@pytest.fixture()
def app(tmp_path):
    return create_app(
        {"DATABASE": str(tmp_path / "test.db"), "TESTING": True, "SECRET_KEY": "t"}
    )


@pytest.fixture()
def client(app):
    return app.test_client()


# ------------------------------------------------------------ 基本核查


def test_missing_and_paths(app, client):
    upload(client, [
        ("http://s/a", T1, ["http://s/b", "http://s/c"]),
        ("http://s/b", T1, ["http://s/d"]),
        ("http://s/c", T1, []),
    ])
    rid = check(client, "http://s/a")
    rep = report(app, rid)
    assert rep["status"] == "valid"
    assert rep["visited_count"] == 3
    assert rep["missing_count"] == 1
    assert missing_map(app, rid) == {"http://s/d": ["http://s/a", "http://s/b", "http://s/d"]}
    assert set(resolved_map(app, rid)) == {"http://s/a", "http://s/b", "http://s/c"}


def test_latest_capture_not_after_lookback(app, client):
    # A 在 T1 的抓存指向 B，在 T2 的抓存指向 C
    upload(client, [
        ("http://s/a", T1, ["http://s/b"]),
        ("http://s/a", T2, ["http://s/c"]),
        ("http://s/b", T1, []),
        ("http://s/c", T1, []),
    ])
    # 回看时刻在 T1 与 T2 之间：应选 T1 的抓存，沿 B 走
    rid = check(client, "http://s/a", "2024-01-15T00:00")
    assert set(resolved_map(app, rid)) == {"http://s/a", "http://s/b"}
    # 回看时刻晚于 T2：应选 T2 的抓存，沿 C 走
    rid2 = check(client, "http://s/a", LOOKBACK)
    assert set(resolved_map(app, rid2)) == {"http://s/a", "http://s/c"}


def test_cycle_checked_once(app, client):
    upload(client, [
        ("http://s/a", T1, ["http://s/b"]),
        ("http://s/b", T1, ["http://s/a", "http://s/c"]),
        ("http://s/c", T1, ["http://s/a"]),
    ])
    rid = check(client, "http://s/a")
    rep = report(app, rid)
    assert rep["visited_count"] == 3
    assert rep["missing_count"] == 0


def test_entry_itself_missing(app, client):
    upload(client, [("http://s/other", T1, [])])
    rid = check(client, "http://s/gone")
    assert missing_map(app, rid) == {"http://s/gone": ["http://s/gone"]}
    assert report(app, rid)["visited_count"] == 0


def test_json_outlinks_cell(app, client):
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["url", "captured_at", "outlinks"])
    w.writerow(["http://s/a", T1, '["http://s/b", "http://s/c"]'])
    w.writerow(["http://s/b", T1, ""])
    w.writerow(["http://s/c", T1, ""])
    client.post(
        "/uploads",
        data={"file": (io.BytesIO(buf.getvalue().encode()), "j.csv")},
        content_type="multipart/form-data",
    )
    rid = check(client, "http://s/a")
    assert report(app, rid)["visited_count"] == 3


# ------------------------------------------------------------ 补传 → 待复查


def test_upload_changing_selected_capture_flags_recheck(app, client):
    upload(client, [("http://s/a", T1, ["http://s/b"]), ("http://s/b", T1, [])])
    rid = check(client, "http://s/a")
    assert report(app, rid)["status"] == "valid"
    # 补传：A 出现更晚但仍不晚于回看时刻的抓存 → 核查所选会变
    upload(client, [("http://s/a", T2, ["http://s/b"])])
    assert report(app, rid)["status"] == "needs_recheck"


def test_upload_filling_missing_flags_recheck(app, client):
    upload(client, [("http://s/a", T1, ["http://s/b"])])
    rid = check(client, "http://s/a")
    assert missing_map(app, rid) == {"http://s/b": ["http://s/a", "http://s/b"]}
    # 补传：为缺档的 B 补上不晚于回看时刻的抓存
    upload(client, [("http://s/b", T2, [])])
    assert report(app, rid)["status"] == "needs_recheck"


def test_irrelevant_uploads_keep_valid(app, client):
    upload(client, [("http://s/a", T1, ["http://s/b"]), ("http://s/b", T1, [])])
    rid = check(client, "http://s/a")
    # 晚于回看时刻的抓存：不影响
    upload(client, [("http://s/a", "2024-12-01T00:00:00Z", ["http://s/x"])])
    assert report(app, rid)["status"] == "valid"
    # 无关 URL：不影响
    upload(client, [("http://s/unrelated", T1, [])])
    assert report(app, rid)["status"] == "valid"
    # 缺档 URL 的抓存晚于回看时刻：不影响
    upload(client, [("http://s/a2", T1, ["http://s/c"])])
    rid2 = check(client, "http://s/a2")
    assert "http://s/c" in missing_map(app, rid2)
    upload(client, [("http://s/c", "2024-12-01T00:00:00Z", [])])
    assert report(app, rid2)["status"] == "valid"


def test_recheck_after_fix_is_valid_and_updated(app, client):
    upload(client, [("http://s/a", T1, ["http://s/b"])])
    rid = check(client, "http://s/a")
    upload(client, [("http://s/b", T2, [])])
    assert report(app, rid)["status"] == "needs_recheck"
    resp = client.post(f"/reports/{rid}/recheck")
    assert resp.status_code == 302
    new_rid = int(resp.headers["Location"].rstrip("/").split("/")[-1])
    rep = report(app, new_rid)
    assert rep["status"] == "valid"
    assert rep["missing_count"] == 0
    assert rep["visited_count"] == 2


# ------------------------------------------------------------ 冲突与退回


def test_conflict_with_existing_rejects_whole_batch(app, client):
    upload(client, [("http://s/a", T1, ["http://s/b"])])
    before = one(app, "SELECT COUNT(*) AS c FROM captures")["c"]
    # 同 URL 同时刻但出链不同 → 冲突；同批还有新记录，必须一并退回
    upload(client, [
        ("http://s/a", T1, ["http://s/CHANGED"]),
        ("http://s/new", T1, []),
    ])
    after = one(app, "SELECT COUNT(*) AS c FROM captures")["c"]
    assert after == before
    b = one(app, "SELECT * FROM batches ORDER BY id DESC LIMIT 1")
    assert b["applied"] == 0
    assert "冲突" in b["error"]
    assert q(app, "SELECT * FROM captures WHERE url='http://s/new'") == []


def test_conflict_within_file_rejects(app, client):
    upload(client, [
        ("http://s/a", T1, ["http://s/b"]),
        ("http://s/a", T1, ["http://s/c"]),
    ])
    assert one(app, "SELECT COUNT(*) AS c FROM captures")["c"] == 0
    b = one(app, "SELECT * FROM batches ORDER BY id DESC LIMIT 1")
    assert b["applied"] == 0


def test_exact_duplicate_skipped(app, client):
    upload(client, [("http://s/a", T1, ["http://s/b"])])
    upload(client, [("http://s/a", T1, ["http://s/b"]), ("http://s/c", T1, [])])
    b = one(app, "SELECT * FROM batches ORDER BY id DESC LIMIT 1")
    assert b["applied"] == 1
    assert one(app, "SELECT COUNT(*) AS c FROM captures")["c"] == 2
    assert one(app, "SELECT COUNT(*) AS c FROM captures WHERE url='http://s/a'")["c"] == 1


def test_invalid_rows_rejected(app, client):
    upload(client, [("not-a-url", T1, [])])
    upload(client, [("http://s/a", "not-a-time", [])])
    upload(client, [("http://s/a", T1, ["relative/path"])])
    assert one(app, "SELECT COUNT(*) AS c FROM captures")["c"] == 0
    assert one(app, "SELECT COUNT(*) AS c FROM batches WHERE applied=0")["c"] == 3


# ------------------------------------------------------------ 交错操作


def _delayed_app(tmp_path, delay_ms):
    return create_app({
        "DATABASE": str(tmp_path / "t.db"),
        "TESTING": True,
        "SECRET_KEY": "t",
        "CHECK_STEP_DELAY_MS": delay_ms,
    })


def _chain_rows(n, tail_links):
    rows = []
    for i in range(n):
        nxt = f"http://s/p{i+1}" if i < n - 1 else None
        links = [nxt] if nxt else tail_links
        rows.append((f"http://s/p{i}", T1, links))
    return rows


def _run_check_thread(app, result):
    c = app.test_client()
    resp = c.post("/checks", data={"entry_url": "http://s/p0", "lookback": LOOKBACK})
    result["rid"] = int(resp.headers["Location"].rstrip("/").split("/")[-1])


def test_interleaved_upload_leaves_no_stale_valid_report(tmp_path):
    """核查遍历进行中补传缺档记录：新结论必须直接标为待复查。"""
    app = _delayed_app(tmp_path, delay_ms=50)
    upload(app.test_client(), _chain_rows(15, ["http://s/z"]))
    result = {}
    t = threading.Thread(target=_run_check_thread, args=(app, result))
    t.start()
    time.sleep(0.3)  # 遍历进行中（15 页 × 50ms ≈ 750ms）
    upload(app.test_client(), [("http://s/z", T2, [])])  # 补上缺档 z
    t.join(timeout=60)
    assert t.is_alive() is False
    rep = report(app, result["rid"])
    assert rep["status"] == "needs_recheck"
    # 结论内容基于其快照（z 仍缺档），但状态已正确标记
    assert "http://s/z" in missing_map(app, result["rid"])


def test_interleaved_irrelevant_upload_keeps_valid(tmp_path):
    """核查遍历进行中的无关补传：结论保持有效。"""
    app = _delayed_app(tmp_path, delay_ms=50)
    upload(app.test_client(), _chain_rows(15, []))
    result = {}
    t = threading.Thread(target=_run_check_thread, args=(app, result))
    t.start()
    time.sleep(0.3)
    upload(app.test_client(), [("http://s/unrelated", T1, [])])
    t.join(timeout=60)
    assert report(app, result["rid"])["status"] == "valid"


def test_upload_during_check_not_blocked(tmp_path):
    """遍历期间补传请求能快速完成（不被长核查阻塞）。"""
    app = _delayed_app(tmp_path, delay_ms=100)
    upload(app.test_client(), _chain_rows(20, []))
    result = {}
    t = threading.Thread(target=_run_check_thread, args=(app, result))
    t.start()
    time.sleep(0.3)
    start = time.monotonic()
    resp = upload(app.test_client(), [("http://s/new", T1, [])])
    elapsed = time.monotonic() - start
    t.join(timeout=60)
    assert resp.status_code == 302
    assert elapsed < 5  # 远小于核查总时长 ~2s


# ------------------------------------------------------------ 持久化


def test_reports_survive_restart(tmp_path):
    db = str(tmp_path / "persist.db")
    app1 = create_app({"DATABASE": db, "TESTING": True, "SECRET_KEY": "t"})
    c1 = app1.test_client()
    upload(c1, [("http://s/a", T1, ["http://s/b"])])
    rid = check(c1, "http://s/a")
    # 模拟重启：全新应用实例指向同一数据库文件
    app2 = create_app({"DATABASE": db, "TESTING": True, "SECRET_KEY": "t"})
    rep = report(app2, rid)
    assert rep["entry_url"] == "http://s/a"
    assert rep["status"] == "valid"
    assert missing_map(app2, rid) == {"http://s/b": ["http://s/a", "http://s/b"]}
    resp = app2.test_client().get(f"/reports/{rid}")
    assert resp.status_code == 200
    assert "http://s/b" in resp.get_data(as_text=True)


# ------------------------------------------------------------ 页面冒烟


def test_pages_render(app, client):
    upload(client, [("http://s/a", T1, ["http://s/b"])])
    rid = check(client, "http://s/a")
    assert client.get("/").status_code == 200
    assert client.get(f"/reports/{rid}").status_code == 200
    assert client.get("/batches/1").status_code == 200
    assert client.get("/healthz").data == b"ok"
    html = client.get(f"/reports/{rid}").get_data(as_text=True)
    assert "缺档" in html and "http://s/b" in html


# ------------------------------------------------------------ 清理试算与确认

CUTOFF = "2024-06-01T00:00"  # 晚于 T3，覆盖全部示例抓存


def preview(client, cutoff=CUTOFF):
    resp = client.post("/cleanup/preview", data={"cutoff": cutoff})
    assert resp.status_code == 302
    return int(resp.headers["Location"].rstrip("/").split("/")[-1])


def preview_plan(app, pid):
    return json.loads(
        one(app, "SELECT plan FROM cleanup_previews WHERE id=?", (pid,))["plan"]
    )


def confirm(client, pid):
    return client.post(f"/cleanup/{pid}/confirm")


def deletable_ids(app, pid):
    return [p["batch_id"] for p in preview_plan(app, pid)["deletable"]]


def capture_count(app):
    return one(app, "SELECT COUNT(*) AS c FROM captures")["c"]


def test_cleanup_deletes_unreferenced_batch_whole(app, client):
    upload(client, [("http://s/a", T1, ["http://s/b"]), ("http://s/b", T1, [])])
    rid = check(client, "http://s/a")
    upload(client, [("http://s/old1", T2, []), ("http://s/old2", T2, [])])
    # 批次 2 不被任何结论引用 → 可删；批次 1 被结论选中 → 保留
    pid = preview(client)
    assert deletable_ids(app, pid) == [2]
    kept = {p["batch_id"]: p for p in preview_plan(app, pid)["kept"]}
    assert 1 in kept
    assert any("结论 #%d" % rid in r for r in kept[1]["reasons"])
    assert kept[1]["dependencies"][0]["report_id"] == rid

    resp = confirm(client, pid)
    assert resp.status_code == 302
    assert one(app, "SELECT COUNT(*) AS c FROM batches WHERE id=2")["c"] == 0
    assert one(app, "SELECT COUNT(*) AS c FROM captures WHERE url='http://s/old1'")["c"] == 0
    assert one(app, "SELECT COUNT(*) AS c FROM batches WHERE id=1")["c"] == 1
    assert report(app, rid)["status"] == "valid"
    # 结论仍可复查
    assert client.get(f"/reports/{rid}").status_code == 200


def test_cleanup_keeps_batch_with_capture_after_cutoff(app, client):
    upload(client, [("http://s/a", T1, []), ("http://s/future", "2024-12-01T00:00:00Z", [])])
    pid = preview(client)  # 截止 2024-06-01，批次 1 里有一条未来抓存 → 整批保留
    assert deletable_ids(app, pid) == []
    kept = preview_plan(app, pid)["kept"]
    assert any("晚于截止时刻" in r for r in kept[0]["reasons"])


def test_cleanup_marks_dependency_for_alternate_capture_at_other_lookback(app, client):
    """同一 URL 在不同回看时刻的替代抓存要分别核对。"""
    upload(client, [
        ("http://s/a", T1, ["http://s/b"]),
        ("http://s/a", T2, ["http://s/b"]),  # A 的第二份抓存（替代）
        ("http://s/b", T1, []),
    ])
    early = check(client, "http://s/a", "2024-01-15T00:00")  # 选 T1 的 A
    late = check(client, "http://s/a", LOOKBACK)             # 选 T2 的 A
    assert resolved_map(app, early)["http://s/a"] < resolved_map(app, late)["http://s/a"]
    pid = preview(client)
    # 两份 A 抓存同属批次 1；被两条结论分别引用，整批保留
    assert deletable_ids(app, pid) == []
    deps = preview_plan(app, pid)["kept"][0]["dependencies"]
    assert {d["report_id"] for d in deps} == {early, late}


def test_cleanup_keeps_missing_url_dependencies(app, client):
    """缺档 URL 不额外依赖，但该 URL 现有抓存若被缺档结论依赖也必须保留。"""
    upload(client, [("http://s/a", T1, ["http://s/gone"])])
    rid = check(client, "http://s/a")
    assert "http://s/gone" in missing_map(app, rid)
    upload(client, [("http://s/a", T2, ["http://s/gone"])])  # A 的第二份抓存
    pid = preview(client)
    # 新批次里的 A 抓存是"更晚回看时刻的替代抓存"，被结论依赖，保留
    assert 2 not in deletable_ids(app, pid)
    assert any(
        "结论 #%d" % rid in r or any(d["report_id"] == rid for d in p["dependencies"])
        for p in preview_plan(app, pid)["kept"] for r in p["reasons"]
    )


def test_cleanup_confirm_rejected_when_catalog_changes_after_preview(app, client):
    upload(client, [("http://s/a", T1, [])])
    upload(client, [("http://s/unrelated", T1, [])])
    pid = preview(client)
    upload(client, [("http://s/new", T2, [])])  # 试算后目录变化
    before_caps = capture_count(app)
    before_batches = one(app, "SELECT COUNT(*) AS c FROM batches")["c"]
    resp = confirm(client, pid)
    assert resp.status_code == 302
    assert capture_count(app) == before_caps
    assert one(app, "SELECT COUNT(*) AS c FROM batches")["c"] == before_batches
    assert one(app, "SELECT COUNT(*) AS c FROM cleanups")["c"] == 0
    html = client.get(f"/cleanup/{pid}").get_data(as_text=True)
    assert "拒绝" in html
    assert "已过时" in html


def test_cleanup_confirm_rejected_when_report_changes_after_preview(app, client):
    upload(client, [("http://s/a", T1, ["http://s/b"]), ("http://s/b", T1, [])])
    rid = check(client, "http://s/a")
    pid = preview(client)
    # 试算后再生成一条结论（甚至无关）→ 结论发生变化，拒绝
    check(client, "http://s/a", LOOKBACK)
    before = capture_count(app)
    confirm(client, pid)
    assert capture_count(app) == before
    assert one(app, "SELECT COUNT(*) AS c FROM cleanups")["c"] == 0
    assert report(app, rid)["status"] == "valid"


def test_cleanup_confirm_once_only(app, client):
    upload(client, [("http://s/a", T1, [])])
    upload(client, [("http://s/old", T2, [])])
    pid = preview(client)
    assert deletable_ids(app, pid) == [1, 2]
    confirm(client, pid)
    assert one(app, "SELECT COUNT(*) AS c FROM cleanups")["c"] == 1
    before = capture_count(app)
    confirm(client, pid)  # 重复确认 → 拒绝且无副作用
    assert capture_count(app) == before
    assert one(app, "SELECT COUNT(*) AS c FROM cleanups")["c"] == 1


def test_cleanup_keeps_rejected_batches(app, client):
    upload(client, [("not-a-url", T1, [])])  # 整批退回
    upload(client, [("http://s/a", T1, [])])
    pid = preview(client)
    assert deletable_ids(app, pid) == [2]
    confirm(client, pid)
    assert one(app, "SELECT COUNT(*) AS c FROM batches WHERE applied=0")["c"] == 1


def test_cleanup_rejects_bad_cutoff(app, client):
    resp = client.post("/cleanup/preview", data={"cutoff": "nope"})
    assert resp.status_code == 302
    assert one(app, "SELECT COUNT(*) AS c FROM cleanup_previews")["c"] == 0


def test_cleanup_failure_leaves_archive_and_can_retry(app, client, monkeypatch):
    """确认过程抛错时不留任何清理结果；修复后可重试成功。"""
    upload(client, [("http://s/a", T1, []), ("http://s/old", T2, [])])
    pid = preview(client)
    before = capture_count(app)
    real = logic.confirm_cleanup

    def boom(conn, preview_id):
        raise RuntimeError("模拟写清理结果失败")

    monkeypatch.setattr(logic, "confirm_cleanup", boom)
    resp = client.post(f"/cleanup/{pid}/confirm")
    assert resp.status_code == 302  # 应用内报错被捕获并回滚，不向用户抛 500
    html = client.get(f"/cleanup/{pid}").get_data(as_text=True)
    assert "清理失败" in html
    assert capture_count(app) == before
    monkeypatch.setattr(logic, "confirm_cleanup", real)
    # 失败后可重试，且清理结果与删除一起生效
    confirm(client, pid)
    assert one(app, "SELECT COUNT(*) AS c FROM cleanups")["c"] == 1
    assert one(app, "SELECT COUNT(*) AS c FROM batches WHERE id=2")["c"] == 0


def test_cleanup_survives_restart(tmp_path):
    db = str(tmp_path / "cleanup.db")
    app1 = create_app({"DATABASE": db, "TESTING": True, "SECRET_KEY": "t"})
    c1 = app1.test_client()
    upload(c1, [("http://s/a", T1, ["http://s/b"]), ("http://s/b", T1, [])])
    rid = check(c1, "http://s/a")
    upload(c1, [("http://s/old", T2, [])])
    pid = preview(c1)
    confirm(c1, pid)
    app2 = create_app({"DATABASE": db, "TESTING": True, "SECRET_KEY": "t"})
    assert one(app2, "SELECT COUNT(*) AS c FROM cleanups")["c"] == 1
    assert one(app2, "SELECT COUNT(*) AS c FROM batches WHERE id=2")["c"] == 0
    assert report(app2, rid)["status"] == "valid"
    assert missing_map(app2, rid) == {}
    html = app2.test_client().get(f"/reports/{rid}").get_data(as_text=True)
    assert "http://s/b" in html


def test_cleanup_pages_render(app, client):
    upload(client, [("http://s/a", T1, [])])
    pid = preview(client)
    assert client.get("/cleanup").status_code == 200
    assert client.get(f"/cleanup/{pid}").status_code == 200
    confirm(client, pid)
    assert client.get("/cleanups/1").status_code == 200
    assert client.get("/cleanup/99999").status_code == 404


def test_cleanup_rejects_report_created_between_preview_and_confirm(tmp_path):
    """试算后即使并发新增结论，确认也必须因指纹变化被拒（不留过时清理）。"""
    db = str(tmp_path / "race.db")
    app = create_app({"DATABASE": db, "TESTING": True, "SECRET_KEY": "t"})
    c = app.test_client()
    upload(c, [("http://s/a", T1, ["http://s/b"]), ("http://s/b", T1, [])])
    upload(c, [("http://s/old", T2, [])])
    pid = preview(c)
    before = capture_count(app)
    # 直接通过业务层在确认前补一条结论（模拟另一会话）
    with app.app_context():
        conn = get_db()
        logic.save_report(conn, "http://s/a", logic.to_us(logic.parse_timestamp(LOOKBACK)),
                          2, {}, {})
    resp = confirm(c, pid)
    assert resp.status_code == 302
    assert capture_count(app) == before
    assert one(app, "SELECT COUNT(*) AS c FROM cleanups")["c"] == 0
    assert one(app, "SELECT COUNT(*) AS c FROM batches WHERE id=2")["c"] == 1


def test_cleanup_keeps_only_batch_with_selected_capture(app, client):
    """同一 URL 三份抓存：删中间那份不影响结论，仍应只保护真正会被选中的。"""
    upload(client, [
        ("http://s/a", T1, ["http://s/b"]),
        ("http://s/a", T2, ["http://s/b"]),
        ("http://s/b", T1, []),
    ])
    # 结论在回看 T3 之前选 T2 的 A；另传一份更早的 A 抓存不会被任何结论选中
    upload(client, [("http://s/x", "2023-01-01T00:00:00Z", [])])
    rid = check(client, "http://s/a", LOOKBACK)
    pid = preview(client)
    # 批次 1 含结论所选抓存 → 保留；批次 2 与结论无关 → 可删
    assert deletable_ids(app, pid) == [2]
    deps = preview_plan(app, pid)["kept"][0]["dependencies"]
    assert deps and deps[0]["report_id"] == rid


def test_cleanup_keeps_whole_batch_if_any_capture_needed(app, client):
    """批次里只要有一条抓存被结论选中，整批都不能删。"""
    upload(client, [("http://s/a", T1, ["http://s/b"]), ("http://s/b", T1, [])])
    check(client, "http://s/a")
    # 同一批次里同时有结论用到的 A/B 和完全无关的 C
    upload(client, [("http://s/c", T2, []), ("http://s/b", T2, [])])
    pid = preview(client)
    # 批次 2 含 B 的替代抓存（更晚回看时刻会选中），整批保留，C 也不能删
    assert 2 not in deletable_ids(app, pid)
    assert one(app, "SELECT COUNT(*) AS c FROM captures WHERE url='http://s/c'")["c"] == 1


def test_cleanup_cutoff_inclusive(app, client):
    upload(client, [("http://s/old", T2, [])])
    pid = preview(client, cutoff="2024-02-01T00:00")
    assert deletable_ids(app, pid) == [1]
    pid2 = preview(client, cutoff="2024-01-31T23:59")
    assert deletable_ids(app, pid2) == []


def test_cleanup_can_delete_capture_after_missing_report_lookback(app, client):
    """缺档 URL 在回看之后才有抓存：删掉它不会改变"缺档"结论，可以删。"""
    upload(client, [("http://s/a", T1, ["http://s/gone"])])
    rid = check(client, "http://s/a", "2024-01-15T00:00")
    assert "http://s/gone" in missing_map(app, rid)
    upload(client, [("http://s/gone", T2, [])])  # 晚于回看时刻，缺档结论不受影响
    pid = preview(client)
    # 批次 1 含结论选中的 A，保留；批次 2 的抓存在回看之后，删掉不影响缺档结论
    assert deletable_ids(app, pid) == [2]
    confirm(client, pid)
    assert "http://s/gone" in missing_map(app, rid)
    assert report(app, rid)["status"] == "valid"


def test_cleanup_rolls_back_deletes_when_final_check_fails(app, client, monkeypatch):
    """删除之后、提交之前的复核失败 → 删除与清理结果一起回滚。"""
    upload(client, [("http://s/a", T1, [])])
    upload(client, [("http://s/old", T2, [])])
    pid = preview(client)
    before_caps = capture_count(app)
    before_batches = one(app, "SELECT COUNT(*) AS c FROM batches")["c"]

    def boom(before, after):
        raise RuntimeError("模拟复核失败")

    monkeypatch.setattr(logic, "_recheck_diff", boom)
    resp = client.post(f"/cleanup/{pid}/confirm")
    assert resp.status_code == 302
    assert capture_count(app) == before_caps
    assert one(app, "SELECT COUNT(*) AS c FROM batches")["c"] == before_batches
    assert one(app, "SELECT COUNT(*) AS c FROM cleanups")["c"] == 0
    assert one(app, "SELECT cleanup_id FROM cleanup_previews WHERE id=?", (pid,))[0] is None


def test_cleanup_marks_alternate_captures_in_separate_batches(app, client):
    """同一 URL 的两份抓存在不同批次、分别被不同回看时刻的结论选中：
    两个批次都要保留，并各自标注依赖它的结论。"""
    upload(client, [("http://s/a", T1, ["http://s/b"]), ("http://s/b", T1, [])])
    early = check(client, "http://s/a", "2024-01-15T00:00")  # 选批次 1 的 A
    upload(client, [("http://s/a", T2, ["http://s/b"])])      # 批次 2：A 的替代抓存
    late = check(client, "http://s/a", LOOKBACK)             # 选批次 2 的 A
    pid = preview(client)
    plan = preview_plan(app, pid)
    assert [p["batch_id"] for p in plan["deletable"]] == []
    by_batch = {p["batch_id"]: p for p in plan["kept"]}
    deps1 = {(d["report_id"], d["url"]) for d in by_batch[1]["dependencies"]}
    deps2 = {(d["report_id"], d["url"]) for d in by_batch[2]["dependencies"]}
    assert (early, "http://s/a") in deps1
    assert (late, "http://s/a") in deps2
    # 两次回看选中的 A 抓存不同，按批次分别核对
    assert resolved_map(app, early)["http://s/a"] != resolved_map(app, late)["http://s/a"]


def test_cleanup_protects_needs_recheck_report_dependencies(app, client):
    """待复查结论引用的抓存与其重查会选中的替代抓存都要保留。"""
    upload(client, [("http://s/a", T1, ["http://s/b"]), ("http://s/b", T1, [])])
    rid = check(client, "http://s/a", LOOKBACK)
    upload(client, [("http://s/a", "2024-05-01T00:00:00Z", ["http://s/b"])])  # 待复查
    assert report(app, rid)["status"] == "needs_recheck"
    pid = preview(client)
    plan = preview_plan(app, pid)
    assert plan["deletable"] == []
    kept = {p["batch_id"] for p in plan["kept"]}
    assert kept == {1, 2}
    # 依赖里明确标注了该结论
    deps = [d for p in plan["kept"] for d in p["dependencies"]]
    assert any(d["report_id"] == rid for d in deps)
