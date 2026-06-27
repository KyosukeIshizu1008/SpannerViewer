#!/usr/bin/env python3
"""アプリアイコン (.icns) を生成する。

外部ライブラリ非依存（標準ライブラリのみ）。VS Code 風の青いスクイクル
背景に、アプリのアクティビティバーと同じデータベース(シリンダー)を白で描く。
アンチエイリアスは解析的なカバレッジ計算で行う。

  python3 scripts/make-icon.py assets/AppIcon.icns
"""
import math
import os
import struct
import sys
import zlib

# --- 色 (VS Code Dark+ 系) ---
BG_TOP = (0x0a, 0x4f, 0x86)     # 上側の濃い青
BG_BOT = (0x00, 0x7a, 0xcc)     # 下側の明るい青 (ACCENT)
CYL = (0xff, 0xff, 0xff)        # シリンダー本体 (白)
RING = (0x0a, 0x4f, 0x86)       # リング(溝)の線色


def clamp01(x):
    return 0.0 if x < 0 else (1.0 if x > 1 else x)


def ellipse_cov(x, y, cx, cy, rx, ry):
    """楕円内のカバレッジ(0..1)。境界を約1px幅でアンチエイリアス。"""
    nx = (x - cx) / rx
    ny = (y - cy) / ry
    d = (math.sqrt(nx * nx + ny * ny) - 1.0) * min(rx, ry)
    return clamp01(0.5 - d)


def ellipse_ring_cov(x, y, cx, cy, rx, ry, half):
    """楕円の輪郭線(リング)のカバレッジ。線の半幅 half[px]。"""
    nx = (x - cx) / rx
    ny = (y - cy) / ry
    d = abs((math.sqrt(nx * nx + ny * ny) - 1.0) * min(rx, ry))
    return clamp01(half + 0.5 - d)


def rrect_cov(x, y, w, h, r):
    """角丸四角形(スクイクル近似)内のカバレッジ。"""
    qx = abs(x - w / 2) - (w / 2 - r)
    qy = abs(y - h / 2) - (h / 2 - r)
    ax = max(qx, 0.0)
    ay = max(qy, 0.0)
    d = math.hypot(ax, ay) + min(max(qx, qy), 0.0) - r
    return clamp01(0.5 - d)


def over(dst, src, a):
    """src を係数 a で dst にアルファ合成。"""
    return tuple(int(round(s * a + d * (1 - a))) for s, d in zip(src, dst))


def render(size):
    """size×size の RGBA バイト列を生成する。"""
    W = float(size)
    cx = W / 2
    rx = 0.30 * W
    ry = 0.11 * W
    cyt = 0.31 * W          # 上面の中心 y
    cyb = 0.69 * W          # 底面の中心 y
    corner = 0.2237 * W     # macOS スクイクル相当
    ring_half = max(0.8, 0.012 * W)
    rings = [cyt, cyt + 0.135 * W, cyt + 0.270 * W]

    out = bytearray(size * size * 4)
    for py in range(size):
        y = py + 0.5
        row = py * size * 4
        for px in range(size):
            x = px + 0.5
            mask = rrect_cov(x, y, W, W, corner)
            if mask <= 0.0:
                continue  # 透明のまま

            # 背景: 上下グラデーション
            t = y / W
            base = tuple(int(round(a + (b - a) * t)) for a, b in zip(BG_TOP, BG_BOT))
            col = base

            # シリンダー本体 = 上下楕円 ∪ 胴(矩形)
            top = ellipse_cov(x, y, cx, cyt, rx, ry)
            bot = ellipse_cov(x, y, cx, cyb, rx, ry)
            in_body = 1.0 if (cyt <= y <= cyb and (cx - rx) <= x <= (cx + rx)) else 0.0
            # 胴の左右端をAA
            if cyt <= y <= cyb:
                ex = (min(x - (cx - rx), (cx + rx) - x))
                in_body = clamp01(ex + 0.5)
            cyl = max(top, bot, in_body)
            if cyl > 0.0:
                col = over(col, CYL, cyl)
                # リング(溝)を描く
                ringc = 0.0
                for cyr in rings:
                    ringc = max(ringc, ellipse_ring_cov(x, y, cx, cyr, rx, ry, ring_half))
                if ringc > 0.0:
                    col = over(col, RING, ringc * cyl)

            i = row + px * 4
            out[i] = col[0]
            out[i + 1] = col[1]
            out[i + 2] = col[2]
            out[i + 3] = int(round(255 * mask))
    return bytes(out)


def write_png(path, size, rgba):
    def chunk(tag, data):
        return (struct.pack(">I", len(data)) + tag + data
                + struct.pack(">I", zlib.crc32(tag + data) & 0xffffffff))

    raw = bytearray()
    stride = size * 4
    for y in range(size):
        raw.append(0)  # filter: none
        raw.extend(rgba[y * stride:(y + 1) * stride])
    ihdr = struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0)
    with open(path, "wb") as f:
        f.write(b"\x89PNG\r\n\x1a\n")
        f.write(chunk(b"IHDR", ihdr))
        f.write(chunk(b"IDAT", zlib.compress(bytes(raw), 9)))
        f.write(chunk(b"IEND", b""))


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "assets/AppIcon.icns"
    os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
    iconset = out.replace(".icns", "") + ".iconset"
    os.makedirs(iconset, exist_ok=True)

    # iconutil が要求するサイズ群 (通常 + @2x)
    specs = [
        (16, "icon_16x16.png"), (32, "icon_16x16@2x.png"),
        (32, "icon_32x32.png"), (64, "icon_32x32@2x.png"),
        (128, "icon_128x128.png"), (256, "icon_128x128@2x.png"),
        (256, "icon_256x256.png"), (512, "icon_256x256@2x.png"),
        (512, "icon_512x512.png"), (1024, "icon_512x512@2x.png"),
    ]
    cache = {}
    for size, name in specs:
        if size not in cache:
            cache[size] = render(size)
            sys.stderr.write(f"  rendered {size}px\n")
        write_png(os.path.join(iconset, name), size, cache[size])

    rc = os.system(f'iconutil -c icns -o "{out}" "{iconset}"')
    if rc != 0:
        sys.stderr.write("iconutil に失敗しました\n")
        sys.exit(1)
    sys.stderr.write(f"created {out}\n")


if __name__ == "__main__":
    main()
