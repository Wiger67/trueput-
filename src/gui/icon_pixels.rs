// 圖示的像素繪製與 .ico 編碼，只用標準函式庫。
//
// 視窗圖示（src/gui/icon.rs）與 exe 檔案圖示（build.rs 透過 include! 使用）共用這份程式，
// 兩者永遠一致，也不需要在 repo 裡放二進位圖檔。
//
// 圖案：藍色圓角方塊上的白色速度表（刻度弧 + 指向右上的指針）。

/// 背景藍色
pub const BLUE: [u8; 4] = [37, 99, 235, 255];
pub const WHITE: [u8; 4] = [255, 255, 255, 255];

/// 每個像素超取樣 4×4 次，邊緣才不會有鋸齒
const SUPERSAMPLE: u32 = 4;

/// 單一取樣點的顏色；座標以圖示邊長正規化到 0..1。
fn shade(x: f32, y: f32) -> Option<[u8; 4]> {
    let corner = 0.22;
    let cx = x.clamp(corner, 1.0 - corner);
    let cy = y.clamp(corner, 1.0 - corner);
    if (x - cx).powi(2) + (y - cy).powi(2) > corner * corner {
        return None; // 圓角外：透明
    }

    let (dx, dy) = (x - 0.5, 0.5 - y); // y 軸朝上
    let r = (dx * dx + dy * dy).sqrt();
    let angle = dy.atan2(dx).to_degrees(); // 0° 在右邊

    // 刻度弧：-30° 到 210°（上半圈再往下延伸）
    let on_arc = (0.30..0.38).contains(&r) && (angle >= -30.0 || angle <= -150.0);
    // 指針：從中心指向右上 45°
    let k = std::f32::consts::FRAC_1_SQRT_2;
    let along = (dx + dy) * k;
    let across = (dx - dy).abs() * k;
    let needle = (0.0..0.30).contains(&along) && across < 0.045;
    let hub = r < 0.08;

    Some(if on_arc || needle || hub { WHITE } else { BLUE })
}

/// 繪製 `size`×`size` 的 RGBA 圖示（非預乘 alpha）。
pub fn render(size: u32) -> Vec<u8> {
    let n = size as usize;
    let ss = SUPERSAMPLE;
    let mut rgba = vec![0u8; n * n * 4];
    for py in 0..size {
        for px in 0..size {
            let mut acc = [0u32; 4];
            for sy in 0..ss {
                for sx in 0..ss {
                    let x = (px * ss + sx) as f32 + 0.5;
                    let y = (py * ss + sy) as f32 + 0.5;
                    let full = (size * ss) as f32;
                    if let Some(c) = shade(x / full, y / full) {
                        for i in 0..3 {
                            acc[i] += c[i] as u32;
                        }
                        acc[3] += 255;
                    }
                }
            }
            // 顏色取有覆蓋到的取樣點平均；alpha 是覆蓋比例
            let covered = acc[3] / 255;
            let i = (py as usize * n + px as usize) * 4;
            for c in 0..3 {
                if let Some(v) = acc[c].checked_div(covered) {
                    rgba[i + c] = v as u8;
                }
            }
            rgba[i + 3] = (acc[3] / (ss * ss)) as u8;
        }
    }
    rgba
}

/// 編碼成 Windows .ico（每個尺寸一張 32 位元 BMP，含 alpha）。
pub fn ico(sizes: &[u32]) -> Vec<u8> {
    let images: Vec<Vec<u8>> = sizes.iter().map(|&s| bmp_entry(s, &render(s))).collect();

    let mut out = Vec::new();
    // ICONDIR
    out.extend_from_slice(&0u16.to_le_bytes()); // reserved
    out.extend_from_slice(&1u16.to_le_bytes()); // type = icon
    out.extend_from_slice(&(sizes.len() as u16).to_le_bytes());

    let mut offset = 6 + 16 * sizes.len() as u32;
    for (&size, img) in sizes.iter().zip(&images) {
        // ICONDIRENTRY；256 寫成 0
        let dim = if size >= 256 { 0 } else { size as u8 };
        out.extend_from_slice(&[dim, dim, 0, 0]);
        out.extend_from_slice(&1u16.to_le_bytes()); // color planes
        out.extend_from_slice(&32u16.to_le_bytes()); // bits per pixel
        out.extend_from_slice(&(img.len() as u32).to_le_bytes());
        out.extend_from_slice(&offset.to_le_bytes());
        offset += img.len() as u32;
    }
    for img in images {
        out.extend_from_slice(&img);
    }
    out
}

/// ICO 內的 BMP：BITMAPINFOHEADER + 由下往上的 BGRA 像素 + AND 遮罩。
fn bmp_entry(size: u32, rgba: &[u8]) -> Vec<u8> {
    let n = size as usize;
    let mask_row = n.div_ceil(32) * 4; // 每列 1 bit，補齊到 4 bytes
    let mut out = Vec::new();
    out.extend_from_slice(&40u32.to_le_bytes()); // header size
    out.extend_from_slice(&(size as i32).to_le_bytes()); // width
    out.extend_from_slice(&(2 * size as i32).to_le_bytes()); // height（含遮罩，所以 ×2）
    out.extend_from_slice(&1u16.to_le_bytes()); // planes
    out.extend_from_slice(&32u16.to_le_bytes()); // bpp
    out.extend_from_slice(&0u32.to_le_bytes()); // BI_RGB
    out.extend_from_slice(&((n * n * 4 + mask_row * n) as u32).to_le_bytes());
    out.extend_from_slice(&[0u8; 16]); // 解析度與色盤：不用

    for y in (0..n).rev() {
        for x in 0..n {
            let i = (y * n + x) * 4;
            out.extend_from_slice(&[rgba[i + 2], rgba[i + 1], rgba[i], rgba[i + 3]]);
        }
    }
    // AND 遮罩全 0：透明度由 alpha 通道決定
    out.extend(std::iter::repeat_n(0u8, mask_row * n));
    out
}
