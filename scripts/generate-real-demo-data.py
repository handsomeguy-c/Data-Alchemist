#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
SEED = 20260628


def main() -> int:
    random.seed(SEED)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "kb").mkdir(parents=True, exist_ok=True)
    (DATA_DIR / "training").mkdir(parents=True, exist_ok=True)

    users = _users(1200)
    devices = _devices(520)
    merchants = _merchants(160)
    transactions = _transactions(users, devices, merchants, 36000)
    events = _events(users, devices, 18000)
    edges = _graph_edges(transactions, events)
    labels = _risk_labels(users, transactions, events)

    _write_csv(DATA_DIR / "users.csv", users)
    _write_csv(DATA_DIR / "devices.csv", devices)
    _write_csv(DATA_DIR / "merchants.csv", merchants)
    _write_csv(DATA_DIR / "transactions.csv", transactions)
    _write_csv(DATA_DIR / "user_events.csv", events)
    _write_csv(DATA_DIR / "graph_edges.csv", edges)
    _write_csv(DATA_DIR / "risk_labels.csv", labels)
    _write_kb_doc(DATA_DIR / "kb" / "risk_playbook.md")
    _write_training_jsonl(DATA_DIR / "training" / "risk_cases.jsonl", labels)
    _write_manifest(users, devices, merchants, transactions, events, edges, labels)

    print("Generated real demo data:")
    for path in [
        "users.csv",
        "devices.csv",
        "merchants.csv",
        "transactions.csv",
        "user_events.csv",
        "graph_edges.csv",
        "risk_labels.csv",
        "kb/risk_playbook.md",
        "training/risk_cases.jsonl",
        "demo_manifest.json",
    ]:
        print(f"  data/{path}")
    return 0


def _users(count: int) -> list[dict[str, object]]:
    cities = ["Shanghai", "Beijing", "Shenzhen", "Hangzhou", "Chengdu", "Wuhan"]
    channels = ["organic", "ads", "partner", "referral", "offline"]
    users = []
    risk_group = set(random.sample(range(1, count + 1), 38))
    for index in range(1, count + 1):
        signup = date(2025, 1, 1) + timedelta(days=random.randint(0, 520))
        risky = index in risk_group
        users.append(
            {
                "user_id": f"u{index:05d}",
                "city": random.choice(cities),
                "age": random.randint(18, 62),
                "signup_channel": random.choice(channels),
                "status": "risk" if risky else random.choices(
                    ["active", "active", "active", "frozen"], [80, 12, 6, 2]
                )[0],
                "created_at": signup.isoformat(),
                "dt": signup.isoformat(),
            }
        )
    return users


def _devices(count: int) -> list[dict[str, object]]:
    models = ["iPhone15", "iPhone14", "Pixel8", "GalaxyS24", "Mate60", "RedmiK70"]
    os_names = ["iOS", "Android", "HarmonyOS"]
    devices = []
    hot_devices = set(random.sample(range(1, count + 1), 22))
    for index in range(1, count + 1):
        first_seen = date(2025, 1, 1) + timedelta(days=random.randint(0, 540))
        risk_level = "high" if index in hot_devices else random.choices(
            ["low", "medium", "high"], [78, 18, 4]
        )[0]
        devices.append(
            {
                "device_id": f"d{index:05d}",
                "os": random.choice(os_names),
                "device_model": random.choice(models),
                "first_seen_at": first_seen.isoformat(),
                "risk_level": risk_level,
                "dt": first_seen.isoformat(),
            }
        )
    return devices


def _merchants(count: int) -> list[dict[str, object]]:
    categories = ["grocery", "travel", "game", "electronics", "finance", "education"]
    cities = ["Shanghai", "Beijing", "Shenzhen", "Hangzhou", "Chengdu"]
    return [
        {
            "merchant_id": f"m{index:04d}",
            "merchant_name": f"merchant_{index:04d}",
            "category": random.choice(categories),
            "city": random.choice(cities),
            "risk_tier": random.choices(["A", "B", "C", "D"], [55, 25, 15, 5])[0],
            "dt": "2026-06-28",
        }
        for index in range(1, count + 1)
    ]


def _transactions(users, devices, merchants, count: int) -> list[dict[str, object]]:
    start = datetime(2026, 4, 1, 0, 0, 0)
    high_risk_users = [row for row in users if row["status"] == "risk"][:38]
    high_risk_devices = [row for row in devices if row["risk_level"] == "high"][:22]
    channels = ["app", "web", "mini_program", "pos"]
    rows = []
    for index in range(1, count + 1):
        collusive = random.random() < 0.11
        user = random.choice(high_risk_users if collusive else users)
        device = random.choice(high_risk_devices if collusive else devices)
        merchant = random.choice(merchants)
        created = start + timedelta(
            days=random.randint(0, 88),
            seconds=random.randint(0, 86399),
        )
        base_amount = random.lognormvariate(4.1, 0.72)
        if collusive:
            base_amount *= random.uniform(2.2, 5.0)
        amount = round(min(base_amount, 19999.0), 2)
        score = min(
            0.99,
            0.04
            + (0.55 if collusive else 0)
            + (0.22 if device["risk_level"] == "high" else 0)
            + random.random() * 0.24,
        )
        chargeback = int(score > 0.72 and random.random() < 0.55)
        rows.append(
            {
                "transaction_id": f"t{index:08d}",
                "user_id": user["user_id"],
                "device_id": device["device_id"],
                "merchant_id": merchant["merchant_id"],
                "ip": _ip(collusive),
                "amount": amount,
                "currency": "CNY",
                "channel": random.choice(channels),
                "risk_score": round(score, 4),
                "is_chargeback": chargeback,
                "created_at": created.isoformat(sep=" "),
                "dt": created.date().isoformat(),
            }
        )
    return rows


def _events(users, devices, count: int) -> list[dict[str, object]]:
    start = datetime(2026, 4, 1, 0, 0, 0)
    event_types = ["login", "view_product", "add_card", "checkout", "refund", "password_reset"]
    rows = []
    for index in range(1, count + 1):
        created = start + timedelta(
            days=random.randint(0, 88),
            seconds=random.randint(0, 86399),
        )
        user = random.choice(users)
        device = random.choice(devices)
        event_type = random.choices(event_types, [40, 25, 8, 18, 5, 4])[0]
        rows.append(
            {
                "event_id": f"e{index:08d}",
                "user_id": user["user_id"],
                "device_id": device["device_id"],
                "event_type": event_type,
                "ip": _ip(event_type in {"refund", "password_reset"}),
                "session_id": f"s{random.randint(1, 9000):06d}",
                "event_time": created.isoformat(sep=" "),
                "dt": created.date().isoformat(),
            }
        )
    return rows


def _graph_edges(transactions, events) -> list[dict[str, object]]:
    weights: dict[tuple[str, str, str, str], float] = {}
    for row in transactions:
        day = row["dt"]
        _inc(weights, (row["user_id"], row["device_id"], "user_device", day), 1.0)
        _inc(weights, (row["user_id"], row["merchant_id"], "user_merchant", day), 1.0)
        _inc(weights, (row["device_id"], row["ip"], "device_ip", day), 0.5)
    for row in events:
        _inc(weights, (row["user_id"], row["device_id"], "event_device", row["dt"]), 0.4)
        if row["event_type"] in {"refund", "password_reset"}:
            _inc(weights, (row["user_id"], row["ip"], "risk_ip", row["dt"]), 0.8)
    return [
        {"src": src, "dst": dst, "edge_type": edge_type, "weight": round(weight, 3), "dt": dt}
        for (src, dst, edge_type, dt), weight in sorted(weights.items())
    ]


def _risk_labels(users, transactions, events) -> list[dict[str, object]]:
    by_user = {row["user_id"]: {"amount": 0.0, "cnt": 0, "chargeback": 0, "refund": 0} for row in users}
    for row in transactions:
        metrics = by_user[row["user_id"]]
        metrics["amount"] += float(row["amount"])
        metrics["cnt"] += 1
        metrics["chargeback"] += int(row["is_chargeback"])
    for row in events:
        if row["event_type"] == "refund":
            by_user[row["user_id"]]["refund"] += 1
    labels = []
    for user in users:
        metrics = by_user[user["user_id"]]
        risk = (
            user["status"] == "risk"
            or metrics["chargeback"] >= 2
            or metrics["amount"] > 12000
            or metrics["refund"] >= 6
        )
        labels.append(
            {
                "user_id": user["user_id"],
                "label": int(risk),
                "label_reason": _label_reason(user, metrics),
                "tx_count_90d": metrics["cnt"],
                "amount_sum_90d": round(metrics["amount"], 2),
                "chargeback_count_90d": metrics["chargeback"],
                "refund_count_90d": metrics["refund"],
                "dt": "2026-06-28",
            }
        )
    return labels


def _label_reason(user, metrics) -> str:
    if user["status"] == "risk":
        return "account_status_risk"
    if metrics["chargeback"] >= 2:
        return "chargeback_cluster"
    if metrics["amount"] > 12000:
        return "high_amount_velocity"
    if metrics["refund"] >= 6:
        return "refund_abuse"
    return "normal"


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _write_kb_doc(path: Path) -> None:
    path.write_text(
        """# 风控数据处理与图分析手册

## 数据表

- transactions: 支付交易流水，包含用户、设备、商户、金额、风险分、拒付标签和业务日期。
- user_events: 用户行为事件，包含登录、绑卡、下单、退款和密码重置。
- graph_edges: 从交易和行为中抽取的关系边，可用于团伙社区发现和关键节点识别。
- risk_labels: 以用户为粒度的 90 天风险标签，可用于训练数据和特征工程验证。

## 推荐检查

1. 对 user_id、device_id、transaction_id 做非空和重复校验。
2. 对 amount、risk_score 做范围检查和分布漂移检查。
3. 对 user_device、device_ip、user_merchant 边做 connected components。
4. 对高风险组件输出成员数、总金额、拒付数和共享设备数。

## 性能建议

交易表按 dt 过滤后再聚合。大表 join 前先过滤时间范围，商户表和设备表适合广播。
图边构建时先做去重和权重聚合，避免重复边放大 shuffle。
""",
        encoding="utf-8",
    )


def _write_training_jsonl(path: Path, labels: list[dict[str, object]]) -> None:
    risky = [row for row in labels if row["label"] == 1][:120]
    normal = [row for row in labels if row["label"] == 0][:120]
    with path.open("w", encoding="utf-8") as file:
        for row in risky + normal:
            record = {
                "instruction": "根据用户 90 天交易统计判断是否存在风险，并说明原因。",
                "input": {
                    "user_id": row["user_id"],
                    "tx_count_90d": row["tx_count_90d"],
                    "amount_sum_90d": row["amount_sum_90d"],
                    "chargeback_count_90d": row["chargeback_count_90d"],
                    "refund_count_90d": row["refund_count_90d"],
                },
                "output": {
                    "label": row["label"],
                    "reason": row["label_reason"],
                },
            }
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def _write_manifest(*tables: list[dict[str, object]]) -> None:
    manifest = {
        "seed": SEED,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "tables": {
            "users": len(tables[0]),
            "devices": len(tables[1]),
            "merchants": len(tables[2]),
            "transactions": len(tables[3]),
            "user_events": len(tables[4]),
            "graph_edges": len(tables[5]),
            "risk_labels": len(tables[6]),
        },
    }
    (DATA_DIR / "demo_manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _ip(risky: bool = False) -> str:
    if risky:
        return f"10.66.{random.randint(1, 12)}.{random.randint(1, 254)}"
    return f"10.{random.randint(1, 220)}.{random.randint(1, 254)}.{random.randint(1, 254)}"


def _inc(weights: dict[tuple[str, str, str, str], float], key, value: float) -> None:
    weights[key] = weights.get(key, 0.0) + value


if __name__ == "__main__":
    raise SystemExit(main())
