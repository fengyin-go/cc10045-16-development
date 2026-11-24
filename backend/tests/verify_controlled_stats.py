"""受控统计场景验证：分群对比 → 协作部门的受控发布。

场景（与需求一致）：
  - 同一侧分群有两个被授权渠道：门店 2 人（属性 2、4），线上 4 人（10、12、14、16）；
  - 另有 5 名总部人员从未授权给接收部门，属性设为极大值；
  - 每个发布的统计至少三名授权成员；门店两人不能单独可见；
  - 线上平均值 = 13，合并授权平均值 ≈ 9.67，两种发布方式每期都只交一份；
  - 更改总部属性后重开本期报告，数值不变，拥有方仍可核查纳入的授权成员。

运行：cd backend && python3 tests/verify_controlled_stats.py
（脚本使用独立临时数据库，不影响运行中的服务数据。）
"""
from __future__ import annotations

import os
import sys
import tempfile

# 必须在导入 app 之前指定独立数据库目录（db 是导入期初始化的单例）
os.environ["CC10045_DB_DIR"] = tempfile.mkdtemp(prefix="controlled_stats_")
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json  # noqa: E402

from app.database import db  # noqa: E402
from app.services import service_for  # noqa: E402

ATTR = "资源属性"
RECIPIENT = "协作部门"
HQ_BIG = 10**9

_failures: list[str] = []


def check(name: str, condition: bool, detail: str = "") -> None:
    mark = "✓" if condition else "✗"
    print(f"  {mark} {name}" + (f" —— {detail}" if detail else ""))
    if not condition:
        _failures.append(name)


def section(title: str) -> None:
    print(f"\n【{title}】")


def main() -> int:
    engine = service_for("segment_engine")
    cohort = service_for("cohort_compare")

    # ------------------------------------------------------------------
    section("准备：归属分群、成员属性、授权与业务范围")
    seg_store = db.insert("segments", {
        "segment_name": "门店渠道", "conditions": "{}",
        "population_attr": "渠道归属", "estimated_size": 0, "status": "已生成"})
    seg_online = db.insert("segments", {
        "segment_name": "线上渠道", "conditions": "{}",
        "population_attr": "渠道归属", "estimated_size": 0, "status": "已生成"})
    seg_hq = db.insert("segments", {
        "segment_name": "总部人员", "conditions": "{}",
        "population_attr": "渠道归属", "estimated_size": 0, "status": "已生成"})

    def add_member(segment_id: int, key: str, value: float) -> None:
        db.insert("segment_members", {
            "segment_id": segment_id,
            "member_key": key,
            "member_attrs": json.dumps({ATTR: value}, ensure_ascii=False),
        })

    add_member(seg_store, "S1", 2)
    add_member(seg_store, "S2", 4)
    for key, value in [("O1", 10), ("O2", 12), ("O3", 14), ("O4", 16)]:
        add_member(seg_online, key, value)
    for i in range(1, 6):  # 五名总部人员：属性极大，且从不授权
        add_member(seg_hq, f"H{i}", HQ_BIG)

    # 授权维度：仅门店 + 线上授权给协作部门；总部从未授权
    engine.grant_members(RECIPIENT, seg_store, ["S1", "S2"])
    engine.grant_members(RECIPIENT, seg_online, ["O1", "O2", "O3", "O4"])
    # 业务范围：协作部门只覆盖门店、线上两个归属
    engine.set_recipient_scope(RECIPIENT, [seg_store, seg_online])
    print("  门店 2 人(2,4) 线上 4 人(10,12,14,16) 总部 5 人(1e9，未授权)")

    # ------------------------------------------------------------------
    section("发布方式一（2026-11 期）：先交线上结果")
    r_online = cohort.request_controlled_stat(RECIPIENT, "2026-11", [seg_online], ATTR)
    check("线上报告交付", r_online.get("delivered") is True and r_online.get("snapshot") is False)
    check("线上平均值 = 13", r_online.get("stat_value") == 13.0,
          f"实际 {r_online.get('stat_value')}")
    check("线上授权人数 = 4", r_online.get("authorized_count") == 4)
    check("报告不含个人成员明细",
          set(r_online.keys()) == {"ok", "delivered", "snapshot", "recipient", "period",
                                   "segment_ids", "attr_key", "authorized_count", "stat_value"})

    r_combo_same = cohort.request_controlled_stat(RECIPIENT, "2026-11", [seg_store, seg_online], ATTR)
    check("同期再领整体报告被拒绝（会反推门店两人）", r_combo_same.get("delivered") is False,
          r_combo_same.get("reason", ""))

    r_store = cohort.request_controlled_stat(RECIPIENT, "2026-11", [seg_store], ATTR)
    check("门店两人不能单独可见（少于三名授权成员）", r_store.get("delivered") is False)
    check("拒绝仍说明缺少覆盖：授权 2 人 < 3",
          r_store.get("authorized_count") == 2 and "3" in r_store.get("reason", ""))

    releases_p1 = cohort.list_releases(RECIPIENT, "2026-11")
    check("本期只交付一份", len(releases_p1) == 1, f"已交付 {len(releases_p1)} 份")

    # ------------------------------------------------------------------
    section("发布方式二（2026-12 期）：先交整体结果")
    r_combo = cohort.request_controlled_stat(RECIPIENT, "2026-12", [seg_store, seg_online], ATTR)
    check("整体报告交付", r_combo.get("delivered") is True)
    check("合并授权平均值 ≈ 9.67", abs(r_combo.get("stat_value", 0) - 9.67) < 0.005,
          f"实际 {r_combo.get('stat_value')}（精确 58/6=9.6667）")
    check("整体授权人数 = 6（越权总部 5 人未补足样本）",
          r_combo.get("authorized_count") == 6)

    r_online_same = cohort.request_controlled_stat(RECIPIENT, "2026-12", [seg_online], ATTR)
    check("同期再领线上报告被拒绝（会反推门店两人）", r_online_same.get("delivered") is False,
          r_online_same.get("reason", ""))

    releases_p2 = cohort.list_releases(RECIPIENT, "2026-12")
    check("本期只交付一份", len(releases_p2) == 1, f"已交付 {len(releases_p2)} 份")

    # ------------------------------------------------------------------
    section("越权与业务范围")
    r_hq = cohort.request_controlled_stat(RECIPIENT, "2026-11", [seg_hq], ATTR)
    check("总部不在业务范围，拒绝交付", r_hq.get("delivered") is False,
          r_hq.get("reason", ""))

    data = engine.comparison_data(RECIPIENT, [seg_store, seg_online, seg_hq], ATTR)
    check("分群引擎的对比数据不含越权总部成员",
          len(data) == 6 and all(not d["member_key"].startswith("H") for d in data),
          f"纳入 {len(data)} 行")

    r_other = cohort.request_controlled_stat("其他部门", "2026-11", [seg_online], ATTR)
    check("未授权接收方不能领取", r_other.get("delivered") is False)

    # ------------------------------------------------------------------
    section("总部极大属性不影响两侧统计")
    check("线上统计精确等于 13（未受 1e9 影响）", r_online.get("stat_value") == 13.0)
    check("整体统计精确等于 58/6（未受 1e9 影响）",
          r_combo.get("stat_value") == round(58 / 6, 4))

    # ------------------------------------------------------------------
    section("更改总部属性后：重开本期报告数值不变，拥有方可核查")
    for i in range(1, 6):
        engine.update_member_attr(seg_hq, f"H{i}", ATTR, -(10**12))
    hq1 = db.fetchone("SELECT member_attrs FROM segment_members WHERE segment_id=? AND member_key='H1'",
                      [seg_hq])
    check("总部属性已变更", json.loads(hq1["member_attrs"])[ATTR] == -(10**12))

    r_reopen = cohort.request_controlled_stat(RECIPIENT, "2026-11", [seg_online], ATTR)
    check("重开本期线上报告为快照且数值不变",
          r_reopen.get("snapshot") is True and r_reopen.get("stat_value") == 13.0)
    r_reopen2 = cohort.request_controlled_stat(RECIPIENT, "2026-12", [seg_store, seg_online], ATTR)
    check("重开本期整体报告为快照且数值不变",
          r_reopen2.get("snapshot") is True and r_reopen2.get("stat_value") == round(58 / 6, 4))

    r_new_period = cohort.request_controlled_stat(RECIPIENT, "2026-13", [seg_online], ATTR)
    check("新发布期重新计算仍不受总部影响（=13）", r_new_period.get("stat_value") == 13.0)

    audit_online = cohort.audit_release("拥有方", RECIPIENT, "2026-11", [seg_online], ATTR)
    expect_online = sorted(f"{seg_online}:{k}" for k in ["O1", "O2", "O3", "O4"])
    check("拥有方核查：线上报告纳入的授权成员",
          audit_online.get("included_members") == expect_online,
          f"{audit_online.get('included_members')}")

    audit_combo = cohort.audit_release("拥有方", RECIPIENT, "2026-12", [seg_store, seg_online], ATTR)
    expect_combo = sorted([f"{seg_store}:S1", f"{seg_store}:S2"] +
                          [f"{seg_online}:{k}" for k in ["O1", "O2", "O3", "O4"]])
    check("拥有方核查：整体报告纳入的授权成员（含门店两人，不含总部）",
          audit_combo.get("included_members") == expect_combo)

    audit_denied = cohort.audit_release(RECIPIENT, RECIPIENT, "2026-11", [seg_online], ATTR)
    check("接收方无权核查纳入样本", audit_denied.get("ok") is False)

    # ------------------------------------------------------------------
    section("验证结论")
    if _failures:
        print(f"  未通过 {len(_failures)} 项：{'; '.join(_failures)}")
        return 1
    print("  全部通过：受控统计按归属+授权维度发布，接收方只见聚合结果，")
    print("  小样本细组不可单独可见也不可被拼回，快照数值稳定，拥有方可审计。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
