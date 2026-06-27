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
echo "注意: このアプリは署名されていません。初回起動はFinderでアプリを右クリック→「開く」"
echo "      で許可してください (Gatekeeper の警告回避)。"
