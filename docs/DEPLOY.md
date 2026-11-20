# Sharing StreamGuard: links, hosting and custom addresses

There are three ways to give people a link. Pick by what you want to show.

| Goal | Option | Always on? | Custom link |
| --- | --- | --- | --- |
| A link anyone can open **any time** | A. Live demo on Streamlit Community Cloud | Yes (free apps sleep when idle and wake on a visit) | `https://<name>.streamlit.app` |
| Nice names for the **full stack on your PC** | B. Custom hostnames (Caddy, built in) | Only while your PC runs it | `http://streamguard.localhost` or your domain |
| Let someone see your **running local stack** for a while | C. Cloudflare Tunnel | Only while your PC runs it | Random URL, or your own domain |

## A. Always-on public demo (recommended for your portfolio)

The dashboard has a **demo mode**: it needs no Kafka and no database. A background thread generates a
simulated sensor stream, including late, duplicate and malformed events, and pushes it through the same
`Pipeline` class the real processor uses. Whoever opens the link sees a live, moving dashboard.
It starts automatically whenever `POSTGRES_DSN` is not set (force it with `STREAMGUARD_DEMO=1`).

Try it locally first:

```powershell
pip install -e ".[dashboard]"
Remove-Item Env:POSTGRES_DSN -ErrorAction SilentlyContinue
streamlit run dashboard/app.py          # http://localhost:8501 -> "Live demo" banner in the sidebar
```

Deploy:

1. Push the repo to GitHub (public is simplest).
2. Go to <https://share.streamlit.io>, sign in with GitHub and choose **Create app**.
3. Repository `Ravindi373/StreamGuard`, branch `main`, **Main file path `dashboard/app.py`**.
4. Under **App URL** pick your own subdomain, for example `streamguard`, which gives
   `https://streamguard.streamlit.app` if the name is free.
5. In *Advanced settings* choose Python 3.12, then **Deploy**.

Notes:
- Dependencies come from `requirements.txt` (repo root) / `dashboard/requirements.txt`; no secrets are needed.
- Free apps go to sleep after a period without visitors and wake when someone opens the link (a few
  seconds; the app back-fills 15 minutes of history instantly, so it never looks empty). Check
  Streamlit's current free-tier limits before relying on it.
- Optional env vars (Settings, Secrets): `DEMO_RATE` (events/s, default 10), `REPO_URL` (target of the
  "Source code" button).
- The demo is intentionally a single-process version. Say so on your portfolio card: the Kafka, TimescaleDB,
  Prometheus and Grafana architecture runs locally via Docker Compose.

## B. Custom hostnames for the full stack

`docker compose up -d` also starts a small Caddy reverse proxy. Out of the box you get:

| Link | Goes to |
| --- | --- |
| <http://streamguard.localhost> | Streamlit dashboard |
| <http://grafana.localhost> | Grafana |

Browsers resolve `*.localhost` to your own machine, so there is nothing to configure. (`curl` and
PowerShell's `Invoke-WebRequest` may not resolve them on Windows; use a browser, or the `localhost:8501` /
`localhost:3000` links, which keep working.)

To make the Grafana tab and links use the new name, copy `.env.example` to `.env` (compose reads it
automatically) and restart:

```powershell
Copy-Item .env.example .env
docker compose up -d
```

**Port 80 already in use?** (IIS, Skype, another web server.) Add `PROXY_HTTP_PORT=8080` to `.env`, open
`http://streamguard.localhost:8080`, and set `GRAFANA_URL=http://grafana.localhost:8080`.

**Your own domain.** On a server (a small VPS) with ports 80 and 443 open to the internet, point two DNS
records (`dashboard.example.com`, `grafana.example.com`) at its IP, then set in `.env`:

```
STREAMGUARD_ADDR=dashboard.example.com
GRAFANA_ADDR=grafana.example.com
GRAFANA_URL=https://grafana.example.com
```

Caddy then obtains and renews free HTTPS certificates automatically. Because Grafana allows anonymous
viewing, **change the admin password and keep the default `admin`/`admin` login away from the public**
(`GF_SECURITY_ADMIN_PASSWORD` in `docker-compose.yml`) before exposing it.

## C. Share your local stack temporarily with Cloudflare Tunnel

No domain needed for a quick link (requires the free `cloudflared` tool):

```powershell
winget install Cloudflare.cloudflared
cloudflared tunnel --url http://localhost:8501
```

It prints a random `https://<words>.trycloudflare.com` address that works while the command runs.
For a stable custom address use a named tunnel with a domain you own (see Cloudflare's docs).

## Linking from your portfolio site (Vercel)

Vercel hosts the static portfolio, not the dashboard. Add the link to the project card:

```html
<a class="btn" href="https://streamguard.streamlit.app" target="_blank" rel="noopener">Live demo</a>
<a class="btn" href="https://github.com/Ravindi373/StreamGuard" target="_blank" rel="noopener">GitHub</a>
```
