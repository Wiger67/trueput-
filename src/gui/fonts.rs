//! 中文字型。
//!
//! egui 內建字型沒有中文。我們先找系統上的繁體中文字型（Windows 的微軟正黑體），
//! 讓字形符合台灣習慣；找不到時，eframe 的 `system_fonts` 會在缺字時自動向系統借字型。
//! 不把字型打包進 exe，執行檔才能維持輕量。

use std::path::PathBuf;

use eframe::egui;
use eframe::epaint::text::{FontInsert, FontPriority, InsertFontFamily};

/// 候選字型（路徑, 字型集合中的索引），依偏好順序。
fn candidates() -> Vec<(PathBuf, u32)> {
    let mut list = Vec::new();

    // Windows：微軟正黑體 → 新細明體 → 微軟雅黑
    let windir =
        std::env::var_os("WINDIR").map_or_else(|| PathBuf::from(r"C:\Windows"), PathBuf::from);
    let fonts = windir.join("Fonts");
    for name in ["msjh.ttc", "msjh.ttf", "mingliu.ttc", "msyh.ttc"] {
        list.push((fonts.join(name), 0));
    }

    // macOS
    list.push(("/System/Library/Fonts/PingFang.ttc".into(), 0));

    // Linux：Noto Sans CJK 的第 3 個字型是繁中（TC）
    for path in [
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/google-noto-cjk/NotoSansCJK-Regular.ttc",
    ] {
        list.push((path.into(), 3));
    }
    list.push(("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc".into(), 0));
    list
}

/// 安裝找到的第一個中文字型，放在內建字型之後（英數字仍用內建字型）。
/// 回傳使用的字型路徑。
pub fn install(ctx: &egui::Context) -> Option<PathBuf> {
    for (path, index) in candidates() {
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let mut data = egui::FontData::from_owned(bytes);
        data.index = index;
        ctx.add_font(FontInsert::new(
            "cjk",
            data,
            vec![
                InsertFontFamily {
                    family: egui::FontFamily::Proportional,
                    priority: FontPriority::Lowest,
                },
                InsertFontFamily {
                    family: egui::FontFamily::Monospace,
                    priority: FontPriority::Lowest,
                },
            ],
        ));
        return Some(path);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefers_traditional_chinese_on_windows() {
        let list = candidates();
        assert!(list[0].0.ends_with("msjh.ttc"));
    }
}
