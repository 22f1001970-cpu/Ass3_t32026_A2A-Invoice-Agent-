# A2A Invoice Agent

## Deploy on Render

1. Create a new GitHub repo containing `main.py`, `requirements.txt`, `render.yaml`.
2. Create a **Postgres database** on a hosting provider with a persistent database URL (a managed external PostgreSQL database works). Create a Render Web Service from the repository.
3. Set `PUBLIC_BASE_URL=https://YOUR-SERVICE.onrender.com/a2a/` (exactly your submitted URL).
4. Set `DATABASE_URL` to the persistent PostgreSQL connection string (`postgresql://...`). If using a provider requiring TLS, use a connection URL with `sslmode=require`.
5. Set `AI_API_KEY` to your model provider key, `AI_BASE_URL` to its OpenAI-compatible base URL, and `AI_MODEL` to an available model. Example: Gemini OpenAI-compatible API endpoint `https://generativelanguage.googleapis.com/v1beta/openai` and model `gemini-2.5-flash`. Do not expose the API key in the repository.
6. Render build: `pip install -r requirements.txt`. Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`. Python 3.11+.
7. Submit `https://YOUR-SERVICE.onrender.com/a2a/` (not the agent card URL).

**Important**: Do not use Render's ephemeral `/tmp` SQLite data for final verification. Persistent PostgreSQL is required to preserve tasks across restarts or scaling. `AGENT_BEARER_TOKEN` is not used: the grader assigns Bearer principals dynamically. Every syntactically valid Bearer token is treated as a distinct user and stored as a SHA-256 digest.

## Smoke test

```bash
BASE=https://YOUR-SERVICE.onrender.com/a2a
curl -i "${BASE%/a2a}/.well-known/agent-card.json"
curl -i "$BASE/tasks" -H 'Authorization: Bearer test-principal' -H 'A2A-Version: 1.0'
```

The first response should be the public card; the second `{"tasks":[]}` for a fresh principal.

## Protocol safeguards

- All write operations use one serializable-by-advisory-lock transaction in PostgreSQL or SQLite's `BEGIN IMMEDIATE`.
- Idempotency is scoped to `(principal, messageId)`, hashes the entire `message` and ignores configuration.
- Stored tasks are authoritative; receipts are never fabricated and include only grader-accepted proposals.
- The same Task is returned on a repeated message; contradictory replays produce `409 IDEMPOTENCY_CONFLICT`.
- A continuation must match the principal, task, context, batch, package IDs, action IDs and actions.
- Cancellation and result processing cannot both win.

## Quality limitations / critical caveats

- AI invoice accuracy depends on your chosen model and the unseen schema/policies inside the packages. The model sees full package JSON and the policyRevision identifier. No explicit policy rules other than the prompt are provided in the assignment excerpt.
- The grader's audit requires new invoice decisions; validate on representative data before spending a Check.
- Each transaction holds a global database lock during the AI request. This guarantees single execution under concurrency but can cause long waits under heavy traffic. Render and proxy timeouts may require an asynchronous workflow if core batches are slow.
- The receipt continuation validates receipt nonce presence but not its cryptographic provenance: the grader is the authority returning ACCEPTED. Never expose bearer tokens.
