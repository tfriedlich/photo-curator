# Photo Curator

Automatically curates the best photos from a Google Takeout export and creates a new Google Photos album.

## How it works

1. Everyone on the trip adds photos to a shared Google Photos album
2. You export the album via Google Takeout (one ZIP file)
3. Upload the ZIP to this app
4. The pipeline runs automatically:
   - Extracts and inventories all photos
   - Filters screenshots, blurry, and dark photos
   - Clusters near-duplicates and burst shots using perceptual hashing
   - Scores surviving candidates with Claude Vision AI
   - Selects the top ~20% of photos
   - Creates a new "Best Of" album in Google Photos

---

## Setup

### 1. Clone and install

```bash
git clone <your-repo>
cd photo-curator/backend
pip install -r requirements.txt
```

### 2. Get an Anthropic API key

Go to https://console.anthropic.com → API Keys → Create Key

### 3. Set up Google Photos API

1. Go to https://console.cloud.google.com
2. Create a new project (e.g. "Photo Curator")
3. Enable the **Photos Library API**
4. Create OAuth 2.0 credentials (Desktop app type)
5. Download the credentials JSON

Then run the auth helper to get your token:
```bash
cd backend
python google_auth.py
```
This opens a browser, you approve access, and it prints your access token.

### 4. Deploy to Railway

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Deploy from the backend directory
cd backend
railway up
```

Set these environment variables in Railway dashboard:
- `ANTHROPIC_API_KEY` — your Anthropic key
- `GOOGLE_PHOTOS_TOKEN` — from step 3 above (see note on token refresh below)

---

## Per-trip workflow

1. **Create a shared Google Photos album** for the trip
2. Everyone joins and their photos auto-sync
3. Go to https://takeout.google.com
   - Select only "Google Photos"
   - Select the specific shared album
   - Export as ZIP
4. Download the ZIP (may take 10-30 min to generate)
5. Open your Photo Curator app URL
6. Enter an album name (e.g. "Costa Rica · April 2026")
7. Upload the ZIP
8. Wait ~10-15 min for 1,000 photos
9. Open the resulting album in Google Photos

---

## Notes on Google Photos token

Google OAuth tokens expire after 1 hour. For a production setup you should:
- Store the refresh token in Railway env vars
- Use `google-auth-oauthlib` to auto-refresh

For now, re-run `python google_auth.py` before each trip and update the Railway env var.
A future version will handle this with a proper OAuth flow in the UI.

---

## Cost estimate

| Photos | Stage 1 (free) | Claude Vision | Total |
|--------|---------------|---------------|-------|
| 500    | free          | ~$0.50        | ~$0.50 |
| 1,000  | free          | ~$1.00        | ~$1.00 |
| 2,000  | free          | ~$2.00        | ~$2.00 |

Claude Vision is only called on candidates that survive Stage 1 filtering (~30-40% of input).
