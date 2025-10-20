#!/usr/bin/env python3
"""
neologdnライブラリの長音記号変換動作を確認するテストスクリプト
"""

try:
    import neologdn
    HAS_NEOLOGDN = True
except ImportError:
    HAS_NEOLOGDN = False
    print("neologdnがインストールされていません")
    exit(1)

# テストケース
test_cases = [
    "フォローアップ",
    "サーバー",
    "コンピューター",
    "データベース",
    "ユーザー",
    "マネージャー",
    "プロバイダー",
]

print("=" * 80)
print("neologdn.normalize() の動作確認")
print("=" * 80)
print()
print("| 元の文字列 | neologdn変換後 | 長音記号は保持される? |")
print("|------------|---------------|---------------------|")

for text in test_cases:
    normalized = neologdn.normalize(text)
    has_long_vowel_before = 'ー' in text
    has_long_vowel_after = 'ー' in normalized
    has_hyphen_after = '-' in normalized

    if has_long_vowel_before and has_long_vowel_after:
        status = "✓ 保持"
    elif has_long_vowel_before and has_hyphen_after:
        status = "✗ ハイフンに変換"
    elif has_long_vowel_before and not has_long_vowel_after and not has_hyphen_after:
        status = "✗ 削除"
    else:
        status = "? 不明"

    print(f"| {text:12s} | {normalized:15s} | {status:19s} |")

print()
print("=" * 80)
print("バイトコード確認")
print("=" * 80)
print()

original = "フォローアップ"
normalized = neologdn.normalize(original)

print(f"元の文字列: {original}")
print(f"バイト列: {original.encode('utf-8').hex()}")
print()
print(f"neologdn変換後: {normalized}")
print(f"バイト列: {normalized.encode('utf-8').hex()}")
print()

# 長音記号とハイフンのUnicodeコードポイント
print("参考: Unicodeコードポイント")
print(f"  長音記号（ー）: U+30FC = {chr(0x30FC).encode('utf-8').hex()}")
print(f"  ハイフン（-）:   U+002D = {chr(0x002D).encode('utf-8').hex()}")
