#!/usr/bin/env python3
"""
私たちのnormalization.pyの動作を段階的に確認するテストスクリプト
"""

import sys
from pathlib import Path

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.utils.normalization import normalize_text, normalize_hyphens
import neologdn
import unicodedata

test_text = "フォローアップ"

print("=" * 80)
print("normalization.pyの処理フロー確認")
print("=" * 80)
print()
print(f"元のテキスト: {test_text}")
print(f"バイト列: {test_text.encode('utf-8').hex()}")
print()

# Step 1: neologdn
step1 = neologdn.normalize(test_text)
print(f"Step 1 (neologdn): {step1}")
print(f"バイト列: {step1.encode('utf-8').hex()}")
print(f"長音記号は保持? {'✓' if 'ー' in step1 else '✗'}")
print()

# Step 2: NFKC正規化
step2 = unicodedata.normalize('NFKC', step1)
print(f"Step 2 (NFKC): {step2}")
print(f"バイト列: {step2.encode('utf-8').hex()}")
print(f"長音記号は保持? {'✓' if 'ー' in step2 else '✗'}")
print()

# Step 3: ハイフン統一（normalize_hyphens）
step3 = normalize_hyphens(step2)
print(f"Step 3 (normalize_hyphens): {step3}")
print(f"バイト列: {step3.encode('utf-8').hex()}")
print(f"長音記号は保持? {'✓' if 'ー' in step3 else '✗'}")
print(f"ハイフンに変換? {'✓' if '-' in step3 else '✗'}")
print()

# 最終結果（normalize_text）
final = normalize_text(test_text)
print(f"最終結果 (normalize_text): {final}")
print(f"バイト列: {final.encode('utf-8').hex()}")
print(f"長音記号は保持? {'✓' if 'ー' in final else '✗'}")
print(f"ハイフンに変換? {'✓' if '-' in final else '✗'}")
print()

print("=" * 80)
print("結論")
print("=" * 80)
print()
print("neologdn: 長音記号（ー）を保持 ✓")
print("NFKC正規化: 長音記号（ー）を保持 ✓")
print("normalize_hyphens: 長音記号（ー）をハイフン（-）に変換 ✗")
print()
print("→ 長音→ハイフン変換は、neologdnではなく、")
print("  私たちのコード（normalize_hyphens関数）で実施されています。")
