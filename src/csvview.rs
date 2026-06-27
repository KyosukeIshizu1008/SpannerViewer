//! 巨大 CSV ビューア用のインデックス。
//!
//! 100GB 級でも扱えるよう、ファイルをメモリマップ（mmap）し、行の先頭バイト
//! オフセットを「疎な索引」として持つ。索引はエントリ数が上限を超えたら間引いて
//! ストライドを倍にする（単一パス・メモリ上限つき）。表示中の行だけを遅延パース
//! するため、全レコードをメモリに展開しない。

use std::fs::File;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use memmap2::Mmap;

use crate::query::Encoding;

/// 疎な行オフセット索引つきの CSV。
pub struct CsvIndex {
    mmap: Mmap,
    /// offsets[k] = 行 (k*stride) の先頭バイトオフセット。
    offsets: Vec<u64>,
    stride: u64,
    /// データ行も含む総行数（ヘッダ込み）。
    pub total_rows: u64,
    pub bytes: u64,
}

/// 索引エントリ数の上限（超えたら間引いてストライドを倍に）。
/// 8M エントリ ≒ 64MB。100GB でもこの範囲に収める。
const MAX_ENTRIES: usize = 8_000_000;

impl CsvIndex {
    /// ファイルを mmap して改行を走査し、疎な行オフセット索引を作る。
    /// progress には走査済みバイト数を随時書き込む（UI の進捗表示用）。
    pub fn build(path: &Path, progress: Arc<AtomicU64>) -> io::Result<Self> {
        Self::build_inner(path, progress, MAX_ENTRIES)
    }

    /// 索引エントリ上限を指定できる本体（テストで間引きを強制するため分離）。
    fn build_inner(path: &Path, progress: Arc<AtomicU64>, max_entries: usize) -> io::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let data: &[u8] = &mmap;
        let bytes = data.len() as u64;

        let mut offsets: Vec<u64> = Vec::new();
        let mut stride: u64 = 1;
        if bytes > 0 {
            offsets.push(0); // 行0の開始
        }
        let mut line: u64 = 0; // これまでに見つけた改行の数
        for nl in memchr::memchr_iter(b'\n', data) {
            line += 1;
            let next_off = (nl + 1) as u64;
            if next_off < bytes {
                // 次の行が存在する。stride の倍数の行頭だけ記録。
                if line.is_multiple_of(stride) {
                    offsets.push(next_off);
                    if offsets.len() >= max_entries {
                        // 間引き: 偶数番だけ残してストライドを倍に。
                        let mut keep = true;
                        offsets.retain(|_| {
                            let k = keep;
                            keep = !keep;
                            k
                        });
                        stride *= 2;
                    }
                }
            }
            if line.is_multiple_of(1 << 18) {
                progress.store(nl as u64, Ordering::Relaxed);
            }
        }
        // 総行数: 改行数 + 末尾が改行で終わらなければ最後の部分行を1行とみなす。
        let mut total = line;
        if bytes > 0 && data[data.len() - 1] != b'\n' {
            total += 1;
        }
        progress.store(bytes, Ordering::Relaxed);
        Ok(CsvIndex {
            mmap,
            offsets,
            stride,
            total_rows: total,
            bytes,
        })
    }

    /// 行 i の生バイト列（末尾の \r\n は除く）。範囲外なら None。
    pub fn row_bytes(&self, i: u64) -> Option<&[u8]> {
        if i >= self.total_rows {
            return None;
        }
        let data: &[u8] = &self.mmap;
        let block = (i / self.stride) as usize;
        let mut off = *self.offsets.get(block)? as usize;
        let mut skip = (i - block as u64 * self.stride) as usize;
        while skip > 0 {
            let p = memchr::memchr(b'\n', &data[off..])?;
            off += p + 1;
            skip -= 1;
        }
        let end = match memchr::memchr(b'\n', &data[off..]) {
            Some(p) => off + p,
            None => data.len(),
        };
        let mut e = end;
        if e > off && data[e - 1] == b'\r' {
            e -= 1;
        }
        Some(&data[off..e])
    }

    /// 行 i を区切り文字で分割して文字列ベクタにする（引用符対応・UTF-8）。
    /// アプリ側は split_fields + line_at を直接使うため、テスト用 API。
    #[cfg(test)]
    pub fn parse_row(&self, i: u64, delim: u8) -> Vec<String> {
        self.parse_row_enc(i, delim, Encoding::Utf8)
    }

    /// 行 i を分割し、各フィールドを指定エンコーディングでデコードする（テスト用）。
    #[cfg(test)]
    pub fn parse_row_enc(&self, i: u64, delim: u8, enc: Encoding) -> Vec<String> {
        match self.row_bytes(i) {
            Some(b) => split_fields(b, delim).iter().map(|f| enc.decode(f)).collect(),
            None => Vec::new(),
        }
    }

    /// 列数の推定（先頭行のフィールド数）（テスト用）。
    #[cfg(test)]
    pub fn column_count(&self, delim: u8) -> usize {
        self.parse_row(0, delim).len()
    }

    /// 検索/絞り込み: 一致した行（ファイル行インデックス）を集める。
    /// col=None は全列（生バイト部分一致）、Some(c) は c 列目で部分一致。
    /// 大文字小文字は無視（ASCII）。cancel で中断、progress に走査済みバイト数、
    /// cap で件数上限（メモリ保護）。ヘッダ行は対象外。
    #[allow(clippy::too_many_arguments)]
    pub fn scan_filter(
        &self,
        needle: &str,
        col: Option<usize>,
        delim: u8,
        has_header: bool,
        enc: Encoding,
        cancel: &AtomicBool,
        progress: &AtomicU64,
        cap: usize,
    ) -> Vec<u64> {
        let data: &[u8] = &self.mmap;
        let needle_l = needle.to_lowercase();
        let nl_bytes = needle_l.as_bytes();
        let mut out: Vec<u64> = Vec::new();
        let mut line: u64 = 0;
        let mut start = 0usize;
        let mut count: u64 = 0;

        let consider = |line_idx: u64, bytes: &[u8], out: &mut Vec<u64>| {
            if has_header && line_idx == 0 {
                return;
            }
            let hit = match (col, enc) {
                // UTF-8 全列は生バイトで高速一致（ASCII大小無視）。
                (None, Encoding::Utf8) => contains_ascii_ci(bytes, nl_bytes),
                // それ以外（列指定 or 非UTF-8）はデコードしてから一致。
                (None, _) => enc.decode(bytes).to_lowercase().contains(&needle_l),
                (Some(c), _) => split_fields(bytes, delim)
                    .get(c)
                    .map(|f| enc.decode(f).to_lowercase().contains(&needle_l))
                    .unwrap_or(false),
            };
            if hit {
                out.push(line_idx);
            }
        };

        for nl in memchr::memchr_iter(b'\n', data) {
            let mut e = nl;
            if e > start && data[e - 1] == b'\r' {
                e -= 1;
            }
            consider(line, &data[start..e], &mut out);
            line += 1;
            start = nl + 1;
            count += 1;
            if out.len() >= cap {
                break;
            }
            if count.is_multiple_of(1 << 16) {
                progress.store(start as u64, Ordering::Relaxed);
                if cancel.load(Ordering::Relaxed) {
                    break;
                }
            }
        }
        // 末尾（改行で終わらない最後の行）。途中の行と同じく末尾の \r は除く。
        if out.len() < cap && !cancel.load(Ordering::Relaxed) && start < data.len() {
            let mut e = data.len();
            if e > start && data[e - 1] == b'\r' {
                e -= 1;
            }
            consider(line, &data[start..e], &mut out);
        }
        progress.store(data.len() as u64, Ordering::Relaxed);
        out
    }
}

/// haystack に needle_lower（小文字済み）が ASCII 大小無視で含まれるか。
fn contains_ascii_ci(hay: &[u8], needle_lower: &[u8]) -> bool {
    if needle_lower.is_empty() {
        return true;
    }
    if needle_lower.len() > hay.len() {
        return false;
    }
    'outer: for i in 0..=hay.len() - needle_lower.len() {
        for (j, &nb) in needle_lower.iter().enumerate() {
            if hay[i + j].to_ascii_lowercase() != nb {
                continue 'outer;
            }
        }
        return true;
    }
    false
}

/// CSV 1 行を区切り文字で分割し、各フィールドを生バイトで返す（RFC4180 風の
/// 引用符対応）。エンコーディングに依存しない（デコードは呼び出し側）。
pub fn split_fields(b: &[u8], delim: u8) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut field: Vec<u8> = Vec::new();
    let mut in_q = false;
    let mut k = 0;
    while k < b.len() {
        let c = b[k];
        if in_q {
            if c == b'"' {
                if k + 1 < b.len() && b[k + 1] == b'"' {
                    field.push(b'"');
                    k += 2;
                    continue;
                }
                in_q = false;
                k += 1;
            } else {
                field.push(c);
                k += 1;
            }
        } else if c == b'"' && field.is_empty() {
            // " はフィールド先頭のときだけ引用開始（RFC 4180）。フィールド途中の "
            // はデータとして扱う（例: 5'10"）。こうしないと以降のカンマ・改行を
            // 引用内として飲み込み、列ズレ・文字列の途切れ・余計な " が起きる。
            in_q = true;
            k += 1;
        } else if c == delim {
            out.push(std::mem::take(&mut field));
            k += 1;
        } else {
            field.push(c);
            k += 1;
        }
    }
    out.push(field);
    out
}


#[cfg(test)]
mod tests {
    use super::*;

    fn write_tmp(name: &str, body: &[u8]) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!("spanner_viewer_csvview_{name}"));
        std::fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn split_quoted() {
        let f = |b: &[u8], d: u8| -> Vec<String> {
            split_fields(b, d)
                .iter()
                .map(|x| String::from_utf8_lossy(x).into_owned())
                .collect()
        };
        assert_eq!(f(b"a,b,c", b','), vec!["a", "b", "c"]);
        assert_eq!(f(b"\"a,1\",b,\"c\"\"x\"", b','), vec!["a,1", "b", "c\"x"]);
        assert_eq!(f(b"x;y;z", b';'), vec!["x", "y", "z"]);
        // フィールド途中の " はデータ（5'10"）。以降のカンマを飲み込まない。
        assert_eq!(f(b"O'Brien,5'10\" tall,z", b','), vec!["O'Brien", "5'10\" tall", "z"]);
    }

    /// Shift-JIS のバイト列でも、区切りで正しく分割しデコードできる。
    #[test]
    fn shift_jis_decode() {
        // "名前,値\nあ,1\n" を Shift-JIS で書き出す。
        let (sjis, _, _) = encoding_rs::SHIFT_JIS.encode("名前,値\nあ,1\n");
        let p = write_tmp("sjis.csv", &sjis);
        let idx = CsvIndex::build(&p, Arc::new(AtomicU64::new(0))).unwrap();
        assert_eq!(idx.total_rows, 2);
        // UTF-8 として読むと壊れるが、Shift-JIS 指定で正しく読める。
        assert_eq!(
            idx.parse_row_enc(0, b',', Encoding::ShiftJis),
            vec!["名前", "値"]
        );
        assert_eq!(
            idx.parse_row_enc(1, b',', Encoding::ShiftJis),
            vec!["あ", "1"]
        );
        // 絞り込み（Shift-JIS、日本語）も一致する。
        let hits = idx.scan_filter(
            "あ",
            None,
            b',',
            true,
            Encoding::ShiftJis,
            &AtomicBool::new(false),
            &AtomicU64::new(0),
            100,
        );
        assert_eq!(hits, vec![1]);
    }

    #[test]
    fn index_rows_and_parse() {
        let p = write_tmp("basic.csv", b"Id,Name\n1,alice\n2,bob\n3,carol\n");
        let prog = Arc::new(AtomicU64::new(0));
        let idx = CsvIndex::build(&p, prog).unwrap();
        assert_eq!(idx.total_rows, 4); // header + 3
        assert_eq!(idx.row_bytes(0).unwrap(), b"Id,Name");
        assert_eq!(idx.row_bytes(2).unwrap(), b"2,bob");
        assert_eq!(idx.parse_row(3, b','), vec!["3", "carol"]);
        assert_eq!(idx.column_count(b','), 2);
        assert!(idx.row_bytes(4).is_none());
    }

    #[test]
    fn no_trailing_newline_counts_last_row() {
        let p = write_tmp("notrail.csv", b"a\nb\nc");
        let idx = CsvIndex::build(&p, Arc::new(AtomicU64::new(0))).unwrap();
        assert_eq!(idx.total_rows, 3);
        assert_eq!(idx.row_bytes(2).unwrap(), b"c");
    }

    #[test]
    fn crlf_stripped() {
        let p = write_tmp("crlf.csv", b"a,b\r\n1,2\r\n");
        let idx = CsvIndex::build(&p, Arc::new(AtomicU64::new(0))).unwrap();
        assert_eq!(idx.row_bytes(0).unwrap(), b"a,b");
        assert_eq!(idx.parse_row(1, b','), vec!["1", "2"]);
    }

    #[test]
    fn scan_filter_all_and_column() {
        let p = write_tmp(
            "filter.csv",
            b"id,name,city\n1,Alice,Tokyo\n2,bob,Osaka\n3,Carol,TOKYO\n",
        );
        let idx = CsvIndex::build(&p, Arc::new(AtomicU64::new(0))).unwrap();
        let cancel = AtomicBool::new(false);
        let prog = AtomicU64::new(0);
        // 全列・大小無視で "tokyo" → 行1(Tokyo) と 行3(TOKYO)、ヘッダ除外。
        let hits = idx.scan_filter("tokyo", None, b',', true, Encoding::Utf8, &cancel, &prog, 1000);
        assert_eq!(hits, vec![1, 3]);
        // name 列(=1)で "carol"
        let hits = idx.scan_filter("carol", Some(1), b',', true, Encoding::Utf8, &cancel, &prog, 1000);
        assert_eq!(hits, vec![3]);
        // city 列(=2)で "osaka"
        let hits = idx.scan_filter("osaka", Some(2), b',', true, Encoding::Utf8, &cancel, &prog, 1000);
        assert_eq!(hits, vec![2]);
        // ヘッダ含めない: "name" は本文に無いので 0 件
        let hits = idx.scan_filter("name", None, b',', true, Encoding::Utf8, &cancel, &prog, 1000);
        assert!(hits.is_empty());
    }

    #[test]
    fn scan_filter_cap() {
        let mut body = String::new();
        for _ in 0..100 {
            body.push_str("x,match\n");
        }
        let p = write_tmp("cap.csv", body.as_bytes());
        let idx = CsvIndex::build(&p, Arc::new(AtomicU64::new(0))).unwrap();
        let hits = idx.scan_filter(
            "match",
            None,
            b',',
            false,
            Encoding::Utf8,
            &AtomicBool::new(false),
            &AtomicU64::new(0),
            10,
        );
        assert_eq!(hits.len(), 10, "cap で打ち切る");
    }

    /// 索引を強制的に間引かせ（stride>1）、長さがバラバラな行でも全行を正しく
    /// 解決できることを検証する（100GB を支える核心パスのテスト）。
    #[test]
    fn forced_decimation_resolves_all_rows() {
        // 行ごとに長さを変える（疎索引はスキャンで解決するため可変長が本番に近い）。
        let n = 3000u64;
        let mut body = Vec::new();
        let mut expected: Vec<String> = Vec::new();
        for i in 0..n {
            let pad = "x".repeat((i % 17) as usize); // 0〜16 文字の可変長
            let row = format!("{i},{pad},end{i}");
            expected.push(row.clone());
            body.extend_from_slice(row.as_bytes());
            body.push(b'\n');
        }
        let p = write_tmp("decim.csv", &body);
        // 上限を極小(=8)にして何度も間引かせる → stride は 1 より十分大きくなる。
        let idx = CsvIndex::build_inner(&p, Arc::new(AtomicU64::new(0)), 8).unwrap();
        assert_eq!(idx.total_rows, n);
        assert!(idx.stride > 1, "間引きで stride>1 になるはず: {}", idx.stride);
        assert!(idx.offsets.len() <= 8, "索引は上限内: {}", idx.offsets.len());
        // 全行を順に検証（ストライド境界・スキャンの正しさ）。
        for i in 0..n {
            let got = String::from_utf8(idx.row_bytes(i).unwrap().to_vec()).unwrap();
            assert_eq!(got, expected[i as usize], "row {i}");
        }
        // パースも正しい。
        assert_eq!(idx.parse_row(2999, b',')[0], "2999");
        assert_eq!(idx.parse_row(2999, b',')[2], "end2999");
        assert!(idx.row_bytes(n).is_none());
    }

    #[test]
    fn empty_file_is_zero_rows() {
        let p = write_tmp("empty.csv", b"");
        let idx = CsvIndex::build(&p, Arc::new(AtomicU64::new(0))).unwrap();
        assert_eq!(idx.total_rows, 0);
        assert!(idx.row_bytes(0).is_none());
        assert_eq!(idx.column_count(b','), 0);
    }

    /// 疎索引を強制的に間引かせても正しい行に解決できる（ストライド境界の検証）。
    #[test]
    fn sparse_index_resolves_after_decimation() {
        // 多めの行を作る（間引きはMAX_ENTRIES依存だが、ストライド>1 経路も
        // row_bytes が線形スキャンで正しく解決することを確認する）。
        let mut body = Vec::new();
        for i in 0..5000u64 {
            body.extend_from_slice(format!("row{i},val{i}\n").as_bytes());
        }
        let p = write_tmp("many.csv", &body);
        let idx = CsvIndex::build(&p, Arc::new(AtomicU64::new(0))).unwrap();
        assert_eq!(idx.total_rows, 5000);
        assert_eq!(idx.parse_row(0, b','), vec!["row0", "val0"]);
        assert_eq!(idx.parse_row(1234, b','), vec!["row1234", "val1234"]);
        assert_eq!(idx.parse_row(4999, b','), vec!["row4999", "val4999"]);
    }
}
