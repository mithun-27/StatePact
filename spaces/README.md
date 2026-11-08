# Static Space sources

This directory holds the files deployed to the public Hugging Face Space
[`Mithun-2714/StatePact-Concurrency-Playground`](https://huggingface.co/spaces/Mithun-2714/StatePact-Concurrency-Playground).

The Space is `sdk: static`, so Hugging Face serves
`StatePact-Concurrency-Playground/index.html` directly. There is no build step
and no server-side code.

## Why every external link opens in a new tab

Hugging Face renders the Space inside a sandboxed `<iframe>`. That iframe does
not grant `allow-top-navigation`, and `github.com` answers with
`X-Frame-Options: deny` plus `Content-Security-Policy: frame-ancestors 'none'`.
If a link inside the page navigates the preview iframe itself, Chrome replaces
the playground with `github.com refused to connect`.

Two defences keep that from happening:

1. Every off-site `<a>` carries `target="_blank" rel="noopener noreferrer"`.
2. `app.js` installs a capture-phase click guard (`installExternalLinkGuard`)
   that opens any remaining cross-origin link in a new tab instead of letting
   the iframe navigate away.

The footer's benchmark artifact is served from this directory as
`benchmark-latest.json`, so it stays on the Space's origin.

## Deploying

```bash
python -c "from huggingface_hub import HfApi; HfApi().upload_folder(repo_id='Mithun-2714/StatePact-Concurrency-Playground', repo_type='space', folder_path='spaces/StatePact-Concurrency-Playground')"
```

`benchmark-latest.json` is a copy of the newest artifact in
`benchmarks/results/`; refresh it before uploading when benchmarks change.

## Checking a deploy

```bash
curl -s https://mithun-2714-statepact-concurrency-playground.static.hf.space/index.html
```

The page must contain no `<iframe>` elements, and every absolute `http(s)` link
must include `target="_blank"`.
