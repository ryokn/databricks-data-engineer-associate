#!/usr/bin/env python3
"""questions.json を検証し、assets/quiz-template.html に埋め込んで単体のHTMLを出力する。

使い方:
    python build_quiz.py questions.json output.html

questions.json の形式は SKILL.md / references/question-design.md を参照。
検証に失敗した場合は問題点を全部表示して終了コード1で止まる(HTMLは出力しない)。
壊れた問題集を学習者に渡さないための関門なので、警告ではなくエラーとして扱う。
"""
import json
import sys
from collections import Counter
from pathlib import Path

TEMPLATE = Path(__file__).resolve().parent.parent / "assets" / "quiz-template.html"
PLACEHOLDER = '/*__QUIZ_DATA__*/{"meta":{"title":"サンプル","exam":"","storage_key":"sample"},"questions":[]}'


def validate(data):
    errors, warnings = [], []
    meta = data.get("meta")
    qs = data.get("questions")
    if not isinstance(meta, dict) or not meta.get("title"):
        errors.append("meta.title がありません")
    if not isinstance(qs, list) or not qs:
        errors.append("questions が空、または配列ではありません")
        return errors, warnings, {}

    ps = (meta or {}).get("passing_score")
    if ps is not None and not (isinstance(ps, (int, float)) and 0 < ps <= 1):
        errors.append("meta.passing_score は 0〜1 の数値にしてください(例: 0.7)")

    seen = set()
    for n, q in enumerate(qs, 1):
        tag = f"問{n}(id={q.get('id')!r})"
        qid = q.get("id")
        if not qid:
            errors.append(f"{tag}: id がありません")
        elif qid in seen:
            errors.append(f"{tag}: id が重複しています")
        seen.add(qid)

        if not str(q.get("question", "")).strip():
            errors.append(f"{tag}: question が空です")
        opts = q.get("options")
        if not isinstance(opts, list) or len(opts) < 2:
            errors.append(f"{tag}: options は2つ以上の配列にしてください")
            continue
        if len(set(map(str, opts))) != len(opts):
            errors.append(f"{tag}: 同じ文面の選択肢があります")

        ans = q.get("answer")
        if not isinstance(ans, list) or not ans or not all(isinstance(a, int) for a in ans):
            errors.append(f"{tag}: answer は整数の配列にしてください(例: [2])")
            continue
        if any(a < 0 or a >= len(opts) for a in ans):
            errors.append(f"{tag}: answer が options の範囲外です")
            continue
        if len(set(ans)) != len(ans):
            errors.append(f"{tag}: answer に重複があります")
        if len(ans) >= len(opts):
            errors.append(f"{tag}: すべての選択肢が正解になっています")

        if not str(q.get("explanation", "")).strip():
            errors.append(f"{tag}: explanation(解説)が空です。解説は学習効果の中心なので必須です")
        elif len(str(q["explanation"])) < 30:
            warnings.append(f"{tag}: 解説が短すぎます(なぜ正解か・なぜ他が誤りかを書いてください)")

        # 正解の位置・長さの偏りは「消去法で解ける」問題集になる原因
        if len(ans) == 1:
            lens = [len(str(o)) for o in opts]
            if lens[ans[0]] == max(lens) and max(lens) > 1.6 * (sum(lens) / len(lens)):
                warnings.append(f"{tag}: 正解の選択肢だけ極端に長いです(長さで当てられます)")

    singles = [q["answer"][0] for q in qs if isinstance(q.get("answer"), list) and len(q["answer"]) == 1
               and isinstance(q["answer"][0], int)]
    if len(singles) >= 8:
        c = Counter(singles)
        top, cnt = c.most_common(1)[0]
        if cnt / len(singles) > 0.5:
            warnings.append(f"単一選択の正解の{cnt}/{len(singles)}問が同じ位置({top})です。"
                            "実行時にシャッフルされますが、元データの偏りも直してください")

    doms = Counter(q.get("domain", "全般") for q in qs)
    return errors, warnings, doms


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(2)
    src, dst = Path(sys.argv[1]), Path(sys.argv[2])
    try:
        data = json.loads(src.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        print(f"読み込みエラー: {e}")
        sys.exit(1)

    errors, warnings, doms = validate(data)
    for w in warnings:
        print("警告:", w)
    if errors:
        for e in errors:
            print("エラー:", e)
        print(f"\n{len(errors)} 件のエラーがあるためHTMLを出力しません。")
        sys.exit(1)

    tpl = TEMPLATE.read_text(encoding="utf-8")
    if PLACEHOLDER not in tpl:
        print("エラー: テンプレートのプレースホルダが見つかりません")
        sys.exit(1)
    payload = json.dumps(data, ensure_ascii=False)
    # </script> や <!-- でインライン script が途中で閉じられないようにする
    payload = payload.replace("</", "<\\/").replace("<!--", "<\\!--")
    title = str(data["meta"]["title"]).replace("&", "&amp;").replace("<", "&lt;")
    html = tpl.replace(PLACEHOLDER, payload).replace("<title>問題集</title>", f"<title>{title}</title>")
    dst.parent.mkdir(parents=True, exist_ok=True)
    dst.write_text(html, encoding="utf-8")

    print(f"OK: {len(data['questions'])}問 -> {dst}")
    for d, n in doms.items():
        print(f"  {d}: {n}問")


if __name__ == "__main__":
    main()
