//! Generate an agent identity.
//!
//! Writes the secret key (mode 0600) and prints the public key, which *is* the
//! agent's id. Share that with peers out of band; they add it to their
//! `peers.json` under whatever petname they like.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use interlink::identity::AgentKey;
#[cfg(unix)]
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

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
    write_secret(&args.out, &format!("{}\n", key.to_b64()))
        .with_context(|| format!("writing {}", args.out.display()))?;

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

/// Write the secret key so it is *never* momentarily world-readable: the file is
/// created 0600 in a single step, not written then chmodded (which leaves a TOCTOU
/// window at the default umask, and a permanent 0644 if the chmod fails).
#[cfg(unix)]
fn write_secret(path: &Path, contents: &str) -> Result<()> {
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    // An existing file keeps its old mode, so tighten it explicitly for --force too.
    f.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    f.write_all(contents.as_bytes())?;
    Ok(())
}

#[cfg(not(unix))]
fn write_secret(path: &Path, contents: &str) -> Result<()> {
    // Windows inherits the parent directory's ACL; nothing portable to do here.
    std::fs::write(path, contents)?;
    Ok(())
}
