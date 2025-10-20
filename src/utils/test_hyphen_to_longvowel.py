#!/usr/bin/env python3
"""
ハイフン→長音の修正機能のテストスクリプト
"""
from normalization import fix_hyphen_to_longvowel, normalize_text

# テストケース
test_cases = [
    # (入力, 期待される出力)
    ("リスクコミュニケ-ション等の推進", "リスクコミュニケーション等の推進"),
    ("エヌ・ティ・ティ・コミュニケ-ションズ㈱", "エヌ・ティ・ティ・コミュニケーションズ㈱"),
    ("エネルギ-政策", "エネルギー政策"),
    ("スポ-ツ振興", "スポーツ振興"),
    ("デ-タベース", "データベース"),
    ("ニ-ズ調査", "ニーズ調査"),
    ("フォロ-アップ会議", "フォローアップ会議"),
    ("イノベ-ション推進", "イノベーション推進"),
    ("テクノロジ-", "テクノロジー"),
    ("センタ-", "センター"),
    ("マレ-シア", "マレーシア"),
    ("ル-タ設定", "ルータ設定"),
    # 変換対象外（辞書にない単語）
    ("テスト-ケース", "テスト-ケース"),  # そのまま
    ("サンプル-データ", "サンプル-データ"),  # そのまま
]

print("=" * 80)
print("ハイフン→長音の修正機能テスト")
print("=" * 80)
print()

print("## 1. fix_hyphen_to_longvowel() 単体テスト\n")

passed = 0
failed = 0

for input_text, expected_output in test_cases:
    result = fix_hyphen_to_longvowel(input_text)
    status = "✓" if result == expected_output else "✗"

    if result == expected_output:
        passed += 1
    else:
        failed += 1

    print(f"{status} 入力: {input_text}")
    print(f"  期待: {expected_output}")
    print(f"  結果: {result}")

    if result != expected_output:
        print(f"  ⚠️  不一致!")

    print()

print(f"テスト結果: {passed}件成功, {failed}件失敗\n")

print("=" * 80)
print("## 2. normalize_text() 統合テスト\n")

# normalize_text() を通した場合の動作確認
integration_tests = [
    ("リスクコミュニケ-ション等の推進", "リスクコミュニケーション等の推進"),
    ("エネルギ-政策の見直し", "エネルギー政策の見直し"),
    ("スポ-ツ振興　基本方針", "スポーツ振興 基本方針"),  # 全角スペースも半角に
]

for input_text, expected_output in integration_tests:
    result = normalize_text(input_text)
    status = "✓" if result == expected_output else "✗"

    print(f"{status} 入力: {input_text}")
    print(f"  期待: {expected_output}")
    print(f"  結果: {result}")
    print()

print("=" * 80)
print("## 3. 処理順序の確認\n")

# 処理順序が正しいことを確認
# ハイフン→長音の修正 → ハイフン統一
test_text = "コミュニケ-ション（リスク‐マネジメント）"

print(f"入力: {test_text}")
print()
print("期待される処理:")
print("  1. コミュニケ-ション → コミュニケーション（修正）")
print("  2. リスク‐マネジメント → リスク-マネジメント（統一）")
print()

result = normalize_text(test_text)
print(f"結果: {result}")
print()

if "コミュニケーション" in result and "リスク-マネジメント" in result:
    print("✓ 処理順序は正しい")
else:
    print("✗ 処理順序に問題あり")

print()
print("=" * 80)
print("テスト完了")
