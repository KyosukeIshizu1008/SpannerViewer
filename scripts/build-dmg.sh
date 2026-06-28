#!/usr/bin/env bash
#
# macOS 用の配布 DMG を生成する。
#
#   scripts/build-dmg.sh
#
# 出力: target/dist/Spanner Viewer <version>.dmg
#
# 標準の hdiutil / iconutil のみ使用（外部 brew 依存なし）。
# .app バンドルを作り、ドラッグ&ドロップ用に /Applications へのリンクを
# 同梱した圧縮 DMG を作成する。
set -euo pipefail
cd "$(dirname "$0")/.."

APP_NAME="Spanner Viewer"
BIN="spanner-viewer"
BUNDLE_ID="co.oracleberry.spannerviewer"
VERSION="$(awk -F'"' '/^version/{print $2; exit}' Cargo.toml)"

DIST="target/dist"
APP="$DIST/$APP_NAME.app"
STAGE="$DIST/stage"
DMG="$DIST/$APP_NAME $VERSION.dmg"

echo "==> リリースビルド ($BIN)"
cargo build --release --bin "$BIN"

echo "==> アプリアイコン生成"
ICNS="assets/AppIcon.icns"
if [ ! -f "$ICNS" ]; then
  if command -v python3 >/dev/null 2>&1; then
    python3 scripts/make-icon.py "$ICNS" || echo "   (アイコン生成をスキップ)"
  fi
fi

echo "==> .app バンドル作成"
rm -rf "$APP"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"
cp "target/release/$BIN" "$APP/Contents/MacOS/$BIN"
[ -f "$ICNS" ] && cp "$ICNS" "$APP/Contents/Resources/AppIcon.icns"

cat > "$APP/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>CFBundleName</key><string>$APP_NAME</string>
  <key>CFBundleDisplayName</key><string>$APP_NAME</string>
  <key>CFBundleIdentifier</key><string>$BUNDLE_ID</string>
  <key>CFBundleVersion</key><string>$VERSION</string>
  <key>CFBundleShortVersionString</key><string>$VERSION</string>
  <key>CFBundleExecutable</key><string>$BIN</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleIconFile</key><string>AppIcon</string>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <key>NSHighResolutionCapable</key><true/>
  <key>LSApplicationCategoryType</key><string>public.app-category.developer-tools</string>
</dict>
</plist>
PLIST

echo "==> コード署名"
# CODESIGN_IDENTITY に Developer ID を指定すれば正式署名（公証も可能）。
# 未指定ならアドホック署名 (-)。アドホックでも「壊れているため開けません・ゴミ箱へ」
# という強制ブロックは避けられ、「開発元未確認」（右クリック→開くで許可可）になる。
SIGN_ID="${CODESIGN_IDENTITY:--}"
if [ "$SIGN_ID" = "-" ]; then
  codesign --force --deep --sign - "$APP" \
    && echo "   アドホック署名 OK（未公証: 受け取り側で隔離解除が必要）" \
    || echo "   署名に失敗。未署名のまま続行します。"
else
  codesign --force --deep --options runtime --timestamp --sign "$SIGN_ID" "$APP" \
    && echo "   署名: $SIGN_ID" \
    || echo "   署名に失敗。未署名のまま続行します。"
fi

echo "==> DMG 構成 ($DMG)"
rm -rf "$STAGE"; mkdir -p "$STAGE"
cp -R "$APP" "$STAGE/"
ln -s /Applications "$STAGE/Applications"

rm -f "$DMG"
hdiutil create \
  -volname "$APP_NAME" \
  -srcfolder "$STAGE" \
  -fs HFS+ \
  -format UDZO \
  -ov \
  "$DMG" >/dev/null

rm -rf "$STAGE"
echo "==> 完成: $DMG"
echo
if [ "$SIGN_ID" = "-" ]; then
  cat <<'NOTE'
注意: 公証(notarization)していないため、ダウンロード/共有して受け取った Mac では
      macOS が隔離属性を付け「壊れているため開けません・ゴミ箱へ」と出ることがあります
      （実際に壊れているわけではなく、署名/公証が無いためです）。

  受け取った人の回避手順（どれか）:
    1) アプリを /Applications にコピー後、ターミナルで隔離属性を消す:
         xattr -dr com.apple.quarantine "/Applications/Spanner Viewer.app"
       （または DMG/.app に対して: xattr -cr "Spanner Viewer.app"）
    2) システム設定 → プライバシーとセキュリティ → 下の「このまま開く」を押す
       （アドホック署名済みなので右クリック→「開く」でも許可できる場合があります）

  クリーンに開かせるには Apple Developer ID 署名＋公証が必要です:
    CODESIGN_IDENTITY="Developer ID Application: 名前 (TEAMID)" scripts/build-dmg.sh
    のあと xcrun notarytool submit / stapler staple を実行してください。
NOTE
else
  echo "署名済み: $SIGN_ID（必要なら notarytool で公証してください）"
fi
