import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone


BASE_DIR = os.path.join(os.path.dirname(__file__), "syslog1년치")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "stats.json")


def iter_syslog_files(base_dir: str):
    """
    syslog1년치 폴더 아래의 모든 .txt 파일을 (월, 파일경로) 형태로 반환.
    월 정보는 1단계 하위 폴더 이름(01, 02, ..., 12)에서 가져온다.
    """
    for root, _dirs, files in os.walk(base_dir):
        rel = os.path.relpath(root, base_dir)
        if rel == ".":
            month_str = None
        else:
            month_str = rel.split(os.sep)[0]

        for fname in files:
            if not fname.lower().endswith(".txt"):
                continue
            yield month_str, os.path.join(root, fname)


def aggregate():
    total_errors = 0
    # 월별 전체 로그(에러 여부 상관없이) 건수
    monthly_total_logs: defaultdict[int, int] = defaultdict(int)
    monthly_total_errors: defaultdict[int, int] = defaultdict(int)
    monthly_type_counter: defaultdict[int, Counter] = defaultdict(Counter)

    for month_str, path in iter_syslog_files(BASE_DIR):
        if month_str is None:
            continue
        try:
            month_int = int(month_str)
        except ValueError:
            continue

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = line.strip().split()
                # 총 로그 건수 집계 (빈 줄 제외)
                if len(parts) == 0:
                    continue
                monthly_total_logs[month_int] += 1

                if len(parts) < 3:
                    continue
                third_col = parts[2]
                if "err" not in third_col.lower():
                    continue

                total_errors += 1
                monthly_total_errors[month_int] += 1
                monthly_type_counter[month_int][third_col] += 1

    monthly_stats = []
    for month in range(1, 13):
        total = monthly_total_errors.get(month, 0)
        type_counter = monthly_type_counter.get(month, Counter())
        distinct = len(type_counter)
        top_types = type_counter.most_common(5)
        denom = monthly_total_logs.get(month, 0)
        percentage = (total / denom * 100.0) if denom else 0.0
        monthly_stats.append(
            {
                "month": month,
                "errors": total,
                "distinct_types": distinct,
                "percentage": round(percentage, 2),
                "top_types": top_types,
            }
        )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_errors": total_errors,
        "total_logs_by_month": monthly_total_logs,
        "monthly": monthly_stats,
    }
    return payload


def main():
    data = aggregate()
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"stats.json saved to {OUTPUT_PATH}")
    print(f"총 장애 건수: {data['total_errors']}")


if __name__ == "__main__":
    main()
