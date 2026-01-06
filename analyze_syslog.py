import os
from collections import Counter, defaultdict


BASE_DIR = os.path.join(os.path.dirname(__file__), "syslog1년치")


def iter_syslog_files(base_dir: str):
    """
    syslog1년치 폴더 아래의 모든 .txt 파일을 (월, 파일경로) 형태로 반환.
    월 정보는 1단계 하위 폴더 이름(01, 02, ..., 12)에서 가져온다.
    """
    for root, _dirs, files in os.walk(base_dir):
        # root 예시: syslog1년치/01, syslog1년치/02 ...
        rel = os.path.relpath(root, base_dir)
        if rel == ".":
            # 최상위 디렉터리 자체는 건너뜀
            month_str = None
        else:
            # 첫 번째 디렉터리 이름이 월(01~12)라고 가정
            month_str = rel.split(os.sep)[0]

        for fname in files:
            if not fname.lower().endswith(".txt"):
                continue
            path = os.path.join(root, fname)
            yield month_str, path


def analyze_syslog():
    # 전체 장애 타입별 카운트
    global_error_type_counter: Counter[str] = Counter()

    # 월별 통계: month -> {"total_errors": int, "type_counter": Counter}
    monthly_total_errors: defaultdict[str, int] = defaultdict(int)
    monthly_type_counter: defaultdict[str, Counter] = defaultdict(Counter)

    for month_str, path in iter_syslog_files(BASE_DIR):
        # 월 정보가 없으면 스킵 (이론상 없겠지만 방어 코드)
        if month_str is None:
            continue

        # "01" -> 1, "12" -> 12 형태로 변환 시도
        try:
            month_int = int(month_str)
        except ValueError:
            # 월이 아닌 폴더일 경우 스킵
            continue

        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split()
                # 3번째 컬럼이 없으면 스킵
                if len(parts) < 3:
                    continue

                third_col = parts[2]

                # 3번째 컬럼에 "err" 문자열이 포함되어 있으면 장애 레코드로 간주
                if "err" not in third_col.lower():
                    continue

                # 장애 타입: 3번째 컬럼 전체 문자열을 타입으로 사용
                error_type = third_col

                global_error_type_counter[error_type] += 1
                monthly_total_errors[month_int] += 1
                monthly_type_counter[month_int][error_type] += 1

    # ===== 전체 데이터에서 타입별 장애 종류와 건수 출력 =====
    print("=== 전체 장애 타입별 건수 ===")
    for error_type, cnt in global_error_type_counter.most_common():
        print(f"{error_type:30s} : {cnt:8d}")

    print()

    # ===== 월별 통계 출력 =====
    print("=== 월별 장애 통계 ===")
    # 1~12월 순서대로 출력
    for month in range(1, 13):
        total = monthly_total_errors.get(month, 0)
        type_counter = monthly_type_counter.get(month, Counter())
        distinct_types = len(type_counter)

        print(f"[{month:02d}월]")
        print(f"  장애 건수(total errors)      : {total}")
        print(f"  서로 다른 장애 종류 개수    : {distinct_types}")

        # 필요하다면 상위 몇 개 타입을 같이 보여준다 (예: 상위 5개)
        if total > 0:
            print("  상위 장애 타입 (Top 5):")
            for etype, cnt in type_counter.most_common(5):
                print(f"    - {etype:30s} : {cnt:8d}")

        print()


if __name__ == "__main__":
    analyze_syslog()

