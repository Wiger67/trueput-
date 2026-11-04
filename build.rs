//! 編譯 Windows 版時，把圖示與版本資訊嵌進 exe（檔案總管、工作列看到的圖示）。
//! 其他平台什麼都不做。

#[allow(dead_code)]
mod icon_pixels {
    include!("src/gui/icon_pixels.rs");
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/gui/icon_pixels.rs");

    let windows = std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows");
    if windows {
        embed_windows_resources();
    }
}

fn embed_windows_resources() {
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));
    let ico = out_dir.join("trueput.ico");
    std::fs::write(&ico, icon_pixels::ico(&[16, 24, 32, 48, 64, 128, 256])).expect("寫入 .ico");

    let mut res = winresource::WindowsResource::new();
    res.set_icon(ico.to_str().expect("路徑"));
    res.set("FileDescription", "trueput 網路速度測試");
    res.set("ProductName", "trueput");
    res.set("LegalCopyright", "MIT License");
    // 嵌入失敗（例如沒裝 Windows SDK 的資源編譯器）不影響程式本身，只是 exe 沒有圖示
    if let Err(e) = res.compile() {
        println!("cargo:warning=無法嵌入 exe 圖示與版本資訊：{e}");
    }
}
