# Signature interop vectors (plan 14-05, SEC-03)

Used by `crates/ostro-server/tests/p14_signing.rs` (`interop_*` tests).

| File | Content |
|------|---------|
| `blob.bin` | 4096 fixed bytes: `sha256("ostro-14-05-blob-0") ‖ … ‖ sha256("ostro-14-05-blob-127")`, sha256 `d3f017628c3a4f096f90b049d02d0a94eaf65a1752e144acdfc36a04aad913c6` |
| `minisign.pub` | Public key of a throwaway rsign2 key pair, key id `EE7DBB47A76AEB52` |
| `blob.bin.minisig` | Prehashed `ED` signature made by the real `rsign` CLI (rsign2 0.6.7) |
| `blob.bin.legacy.minisig` | Legacy `Ed` signature by the **test signer** with the same key (see below) |

## How they were made (2026-11-26)

`rsign` came from `bash scripts/fetch-tools.sh rsign2` (rsign2 0.6.7, `cargo install --locked`
into `target/tools/cargo`). Everything ran in a scratch directory outside the repository:

```sh
python3 -c "import hashlib; open('blob.bin','wb').write(b''.join(hashlib.sha256(b'ostro-14-05-blob-%d' % i).digest() for i in range(128)))"
rsign generate -W --unencrypted -p minisign.pub -s minisign.key -c "ostro 14-05 throwaway test key (secret discarded)"
rsign sign -W -s minisign.key -p minisign.pub -x blob.bin.minisig -t "ostro 14-05 interop vector" blob.bin
rsign verify -p minisign.pub -x blob.bin.minisig blob.bin   # "Signature and comment signature verified"
```

rsign2 0.6.7 has no legacy signing mode (`rsign sign` only writes prehashed `ED` signatures and
`rsign verify` answers "Legacy signatures are not accepted"). The legacy vector was therefore
produced by the test signer: the Ed25519 seed was read from the unencrypted `minisign.key`
(offset 54: key id, then the 64-byte secret key = seed ‖ public key, checked against
`minisign.pub`), and the signature was written in minisign's legacy format with Python
`cryptography`'s Ed25519:

- line 2 = base64(`"Ed"` ‖ key id ‖ Ed25519(blob))
- line 3 = `trusted comment: ostro 14-05 legacy interop vector`
- line 4 = base64(Ed25519(signature ‖ trusted comment text))

The secret key `minisign.key` was then destroyed (`shred -u`); it was never inside the
repository and is not committed. Regenerating the vectors needs a new key pair, which changes
`minisign.pub`, both signatures and the key id asserted by the tests.
