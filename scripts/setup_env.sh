#!/bin/bash
# 実行環境セットアップスクリプト

set -e

echo "=== RS System Pipeline - 環境セットアップ ==="

# Python バージョン確認
echo ""
echo "1. Python バージョン確認"
python3 --version

# venv 仮想環境の作成
echo ""
echo "2. venv 仮想環境の作成"
if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo "   ✓ venv 作成完了"
else
    echo "   ✓ venv は既に存在します"
fi

# 仮想環境をアクティベート
source venv/bin/activate

# pip アップグレード
echo ""
echo "3. pip のアップグレード"
pip install --upgrade pip --quiet

# 依存パッケージのインストール
echo ""
echo "4. 依存パッケージのインストール"
pip install -r requirements.txt --quiet

# インストール確認
echo ""
echo "5. インストール済みパッケージ"
pip list | grep -E "(pandas|openpyxl|duckdb|neologdn|fastapi|uvicorn)"

# 必要なディレクトリ作成
echo ""
echo "6. 必要なディレクトリの作成"
mkdir -p data/download
mkdir -p output/{raw,normalized,processed,schema}
mkdir -p scripts
mkdir -p tests
mkdir -p docs
echo "   ✓ ディレクトリ作成完了"

# 簡単な動作確認
echo ""
echo "7. 動作確認テスト"
python3 -c "from src.utils.normalization import normalize_text; result = normalize_text('平成25年度①事業概要'); print(f'   正規化テスト: {result}')"
python3 -c "import sys; sys.path.insert(0, '.'); from config import MINISTRY_MASTER; print(f'   府省庁マスター: {len(MINISTRY_MASTER)}件')"

echo ""
echo "=== セットアップ完了 ==="
echo ""
echo "仮想環境を有効化するには:"
echo "  source venv/bin/activate"
echo ""
echo "パイプラインを実行するには:"
echo "  python main.py --stage 1"
echo ""
echo "APIサーバーを起動するには:"
echo "  python main.py --server --port 8000"
