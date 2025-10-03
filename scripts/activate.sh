#!/bin/bash
# venv仮想環境をアクティベート

if [ ! -d "venv" ]; then
    echo "エラー: venv が見つかりません"
    echo "まず scripts/setup_env.sh を実行してください"
    exit 1
fi

source venv/bin/activate
echo "✓ venv 仮想環境をアクティベートしました"
echo ""
echo "Python: $(which python)"
echo "pip: $(which pip)"
