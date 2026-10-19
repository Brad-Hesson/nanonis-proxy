// build.rs
use winresource::WindowsResource;

fn main() {
    if std::env::var_os("CARGO_CFG_WINDOWS").is_some() {
        let mut res = WindowsResource::new();

        res.set_icon("assets/icon-256.ico"); // Path to your .ico file
        res.compile().unwrap();
    }
}
