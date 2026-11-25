#!/usr/bin/env python3
"""cardano public-data chunk fetcher. No private keys. urllib only."""
import argparse, gzip, hashlib, json, os, sys, time, urllib.error, urllib.request

CHAIN = "cardano"
UA = {"User-Agent": "chain-block-chunks/1.0", "Accept": "application/json"}


def fetch(url, payload=None, retries=4):
    data = None
    headers = dict(UA)
    if payload is not None:
        data = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, data=data, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            last = e
            if e.code not in (429, 500, 502, 503, 504):
                raise
        except Exception as e:
            last = e
        time.sleep(2 * (i + 1))
    raise last


def rpc(url, method, params, retries=4):
    return fetch(url, {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, retries)["result"]


def save(rows, first, last, tx_count, source):
    raw = json.dumps({"chain": CHAIN, "first": first, "last": last,
                      "block_count": len(rows), "tx_count": tx_count,
                      "source": source, "blocks": rows},
                     ensure_ascii=False, separators=(",", ":")).encode()
    gz = gzip.compress(raw, mtime=0)
    base = "%s_%d_%d" % (CHAIN, first, last)
    gz_path = base + ".json.gz"
    mf_path = base + ".manifest.json"
    with open(gz_path, "wb") as fh:
        fh.write(gz)
    manifest = {"chain": CHAIN, "first": first, "last": last,
                "block_count": len(rows), "tx_count": tx_count,
                "bytes": len(gz), "sha256": hashlib.sha256(gz).hexdigest(),
                "asset": gz_path, "source": source}
    with open(mf_path, "w") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)
    print("wrote", gz_path, mf_path, "tx_count", tx_count, "bytes", len(gz))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--first", type=int, required=True)
    ap.add_argument("--count", type=int, required=True)
    a = ap.parse_args()
    first, last = a.first, a.first + a.count - 1

    base = "https://api.koios.rest/api/v1"
    rows, tx_total = [], 0
    for h in range(first, last + 1):
        q = "%s/blocks?select=hash,tx_count&block_height=eq.%d" % (base, h)
        meta = fetch(q)
        if not meta:
            raise RuntimeError("no koios block at height %d" % h)
        bh = meta[0]["hash"]
        info = fetch(base + "/block_info", {"_block_hashes": [bh]})[0]
        cbor = fetch(base + "/block_tx_cbor", {"_block_hashes": [bh]})
        count = int(info.get("tx_count") or 0)
        rows.append({"height": h, "hash": bh, "block_info": info, "tx_cbor": cbor})
        tx_total += count
        time.sleep(0.3)
    save(rows, first, last, tx_total, base)


if __name__ == '__main__':
    main()
