//! Generate an agent identity.
//!
//! Writes the secret key (mode 0600) and prints the public key, which *is* the
//! agent's id. Share that with peers out of band; they add it to their
//! `peers.json` under whatever petname they like.

use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use clap::Parser;
use interlink::identity::AgentKey;

#[derive(Parser)]
#[command(about = "Generate an Ed25519 agent identity")]
struct Args {
    /// Where to write the secret key.
    #[arg(long, env = "INTERLINK_KEY")]
    out: PathBuf,
    /// Overwrite an existing key file.
    #[arg(long)]
    force: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Silently clobbering a private key would be unrecoverable.
    if args.out.exists() && !args.force {
        bail!(
            "{} already exists; refusing to overwrite (use --force)",
            args.out.display()
        );
    }

    let key = AgentKey::generate()?;
    std::fs::write(&args.out, format!("{}\n", key.to_b64()))
        .with_context(|| format!("writing {}", args.out.display()))?;
    restrict(&args.out)?;

    let id = key.id();
    println!("secret key : {}", args.out.display());
    println!("public key : {}", id.to_b64());
    println!("fingerprint: {}", id.fingerprint());
    println!();
    println!("Share the public key with peers. They add it to peers.json:");
    println!(
        "  {{ \"your-petname-for-me\": {{ \"key\": \"{}\" }} }}",
        id.to_b64()
    );
    Ok(())
}

#[cfg(unix)]
fn restrict(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .context("restricting key file permissions")
}

#[cfg(not(unix))]
fn restrict(_path: &std::path::Path) -> Result<()> {
    // Windows inherits the parent directory's ACL; nothing portable to do here.
    Ok(())
}
