//! 視窗圖示。繪圖程式在 `icon_pixels.rs`，與 exe 檔案圖示（build.rs）共用。

use eframe::egui::IconData;

// ico() 只給 build.rs 與測試用，視窗本身只需要 render()
#[allow(dead_code)]
#[path = "icon_pixels.rs"]
mod pixels;

pub const SIZE: u32 = 64;

pub fn app_icon() -> IconData {
    IconData {
        rgba: pixels::render(SIZE),
        width: SIZE,
        height: SIZE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn px(rgba: &[u8], size: u32, x: u32, y: u32) -> [u8; 4] {
        let i = ((y * size + x) * 4) as usize;
        [rgba[i], rgba[i + 1], rgba[i + 2], rgba[i + 3]]
    }

    #[test]
    fn icon_has_expected_shape() {
        let icon = app_icon();
        assert_eq!(icon.rgba.len(), (SIZE * SIZE * 4) as usize);
        assert_eq!(px(&icon.rgba, SIZE, 0, 0)[3], 0, "圓角外要透明");
        assert_eq!(
            px(&icon.rgba, SIZE, SIZE / 2, SIZE / 2),
            pixels::WHITE,
            "中心是白色指針軸"
        );
        assert_eq!(
            px(&icon.rgba, SIZE, SIZE / 2, SIZE - 6),
            pixels::BLUE,
            "下方是藍色背景"
        );
    }

    #[test]
    fn edges_are_antialiased() {
        // 超取樣後，圓角邊緣會有半透明像素
        let rgba = pixels::render(SIZE);
        let partial = rgba.chunks(4).filter(|p| p[3] > 0 && p[3] < 255).count();
        assert!(partial > 0);
    }

    #[test]
    fn ico_structure() {
        let sizes = [16u32, 32, 256];
        let ico = pixels::ico(&sizes);
        let u16_at = |i: usize| u16::from_le_bytes([ico[i], ico[i + 1]]);
        let u32_at = |i: usize| u32::from_le_bytes(ico[i..i + 4].try_into().unwrap());
        assert_eq!(u16_at(2), 1, "type = icon");
        assert_eq!(u16_at(4), 3, "三個尺寸");
        // 每個 entry 的 offset + size 都在檔案範圍內，且第一張緊接在目錄後面
        let mut expected_offset = 6 + 16 * 3;
        for (k, &s) in sizes.iter().enumerate() {
            let e = 6 + 16 * k;
            assert_eq!(ico[e], if s == 256 { 0 } else { s as u8 });
            let (len, off) = (u32_at(e + 8) as usize, u32_at(e + 12) as usize);
            assert_eq!(off, expected_offset);
            assert_eq!(u32_at(off), 40, "BITMAPINFOHEADER");
            assert_eq!(u32_at(off + 8), 2 * s, "高度含遮罩");
            expected_offset += len;
        }
        assert_eq!(expected_offset, ico.len());
    }
}
