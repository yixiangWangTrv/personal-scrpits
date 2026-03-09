import os
import re
import base64
import binascii
import argparse

def analyze_base64(s: str) -> dict:
    s_stripped = s.strip()
    length = len(s_stripped)

    charset_ok = bool(re.fullmatch(r"[A-Za-z0-9+/]+={0,2}", s_stripped))
    len_mod4_ok = (length % 4 == 0)

    decoded = None
    decode_ok = False
    decode_error = None

    try:
        decoded = base64.b64decode(s_stripped, validate=True)
        decode_ok = True
    except (binascii.Error, ValueError) as e:
        decode_error = str(e)

    if decode_ok and charset_ok and len_mod4_ok:
        verdict = "LIKELY_BASE64"
    elif decode_ok:
        verdict = "DECODEABLE_BUT_NOT_CANONICAL_BASE64"
    else:
        verdict = "NOT_BASE64"

    return {
        "secret_char_length": length,
        "base64_charset_ok": charset_ok,
        "base64_length_mod4_ok": len_mod4_ok,
        "base64_decode_ok": decode_ok,
        "decoded_bytes": decoded,
        "decoded_byte_length": None if decoded is None else len(decoded),
        "decode_error": decode_error,
        "verdict": verdict,
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--print-decoded", action="store_true",
                    help="Print decoded secret content (SENSITIVE).")
    ap.add_argument("--format", choices=["utf8", "hex", "base64"], default="hex",
                    help="How to print decoded bytes if --print-decoded is set.")
    args = ap.parse_args()

    secret = os.getenv("WC1_SECRET")
    if not secret:
        raise SystemExit("Missing env var WC1_SECRET")

    r = analyze_base64(secret)

    # Always print diagnostics (no secret)
    for k in [
        "secret_char_length",
        "base64_charset_ok",
        "base64_length_mod4_ok",
        "base64_decode_ok",
        "decoded_byte_length",
        "decode_error",
        "verdict",
    ]:
        v = r.get(k)
        if v is not None:
            print(f"{k}={v}")

    if args.print_decoded:
        decoded = r["decoded_bytes"]
        if decoded is None:
            raise SystemExit("Not decodable; nothing to print.")

        print("\n--- DECODED SECRET (SENSITIVE) ---")
        if args.format == "hex":
            print(decoded.hex())
        elif args.format == "base64":
            # re-encode to normalized base64 (mainly for verification)
            print(base64.b64encode(decoded).decode("ascii"))
        else:  # utf8
            try:
                print(decoded.decode("utf-8"))
            except UnicodeDecodeError:
                print("(not valid UTF-8; use --format hex)")
        print("--- END ---")

if __name__ == "__main__":
    main()