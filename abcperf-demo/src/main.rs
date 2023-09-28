use abcperf_minbft::ABCperfMinbft;

use abcperf_generic_client::cs::{http_warp::HttpWarp, typed::TypedCSTrait};
use anyhow::Result;
use usig::signature::new_ed25519;

const NAME: &str = "minbft";
const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-", env!("VERGEN_GIT_SHA"));

fn main() -> Result<()> {
    abcperf::main(
        NAME,
        VERSION,
        || ABCperfMinbft::new(new_ed25519()),
        || abcperf_noop::server_no_proxy(TypedCSTrait::new(HttpWarp::default())),
        || abcperf_noop::client_emulator_no_proxy(TypedCSTrait::new(HttpWarp::default())),
    )?;

    Ok(())
}
