# Pipeline API Deployment Guide

This guide covers local development and deployment options for the Pipeline API.

## Prerequisites

- Python 3.11+ (for local development)
- Docker (for containerized deployment)
- Google Cloud SDK (for Cloud Run deployment)
- Or Render account (for Render deployment)

## Local Development

### 1. Install Dependencies

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. Set Environment Variables (if needed)

```bash
# Windows PowerShell
$env:SERPER_API_KEY="your-api-key-here"

# Linux/Mac
export SERPER_API_KEY="your-api-key-here"
```

### 3. Run the API Server

```bash
# Default port 8000
python api.py

# Or with custom port
PORT=8080 python api.py
```

The API will be available at `http://localhost:8000`

### 4. Test the API

```bash
# Health check
curl http://localhost:8000/health

# Run pipeline
curl -X POST http://localhost:8000/run \
  -H "Content-Type: application/json" \
  -d '{"source_url": "https://signexpo.org/exhibitors"}' \
  --output enriched_output.csv
```

Or using PowerShell:

```powershell
# Health check
Invoke-WebRequest -Uri http://localhost:8000/health

# Run pipeline
$body = @{source_url = "https://signexpo.org/exhibitors"} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri http://localhost:8000/run -Body $body -ContentType "application/json" -OutFile enriched_output.csv
```

## Docker Local Run

### 1. Build the Docker Image

```bash
docker build -t pipeline-api .
```

### 2. Run the Container

```bash
docker run -p 8000:8000 \
  -e SERPER_API_KEY="your-api-key-here" \
  pipeline-api
```

Or with custom port:

```bash
docker run -p 8080:8080 \
  -e PORT=8080 \
  -e SERPER_API_KEY="your-api-key-here" \
  pipeline-api
```

## Deployment Options

### Option 1: Google Cloud Run

#### Prerequisites

1. Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
2. Authenticate: `gcloud auth login`
3. Set your project: `gcloud config set project YOUR_PROJECT_ID`

#### Deploy Steps

1. **Build and push the Docker image:**

```bash
# Set your GCP project and region
PROJECT_ID="your-project-id"
REGION="us-central1"
IMAGE_NAME="pipeline-api"

# Build and push
gcloud builds submit --tag gcr.io/${PROJECT_ID}/${IMAGE_NAME}
```

2. **Deploy to Cloud Run:**

```bash
gcloud run deploy ${IMAGE_NAME} \
  --image gcr.io/${PROJECT_ID}/${IMAGE_NAME} \
  --platform managed \
  --region ${REGION} \
  --allow-unauthenticated \
  --memory 2Gi \
  --timeout 15m \
  --cpu 2 \
  --set-env-vars "SERPER_API_KEY=your-api-key-here"
```

3. **Get the service URL:**

```bash
gcloud run services describe ${IMAGE_NAME} --region ${REGION} --format 'value(status.url)'
```

#### Update Environment Variables

```bash
gcloud run services update ${IMAGE_NAME} \
  --region ${REGION} \
  --update-env-vars "SERPER_API_KEY=new-key-here"
```

#### Monitor Logs

```bash
gcloud run services logs read ${IMAGE_NAME} --region ${REGION}
```

### Option 2: Render

#### Prerequisites

1. Create a [Render account](https://render.com)
2. Connect your GitHub repository (or use Render CLI)

#### Deploy Steps

1. **Create a new Web Service:**

   - Go to Render Dashboard → New → Web Service
   - Connect your repository
   - Or use Render CLI:

```bash
# Install Render CLI
npm install -g render-cli

# Login
render login

# Create service
render blueprint
```

2. **Configuration:**

   - **Name:** `pipeline-api`
   - **Environment:** `Docker`
   - **Dockerfile Path:** `Dockerfile`
   - **Docker Context:** `.`
   - **Port:** `8000` (or use `$PORT` env var)
   - **Plan:** `Starter` or higher (recommend `Standard` for better performance)

3. **Environment Variables:**

   - Add `SERPER_API_KEY` in the Environment section
   - Render automatically sets `PORT` environment variable

4. **Deploy:**

   - Click "Create Web Service"
   - Render will build and deploy automatically
   - Your service will be available at: `https://pipeline-api.onrender.com`

#### Update Environment Variables

- Go to Dashboard → Your Service → Environment
- Add or update environment variables
- Service will automatically redeploy

## API Usage

### Endpoint: POST /run

**Request:**

```json
{
  "source_url": "https://signexpo.org/exhibitors"
}
```

**Response:**

- **Content-Type:** `text/csv`
- **Body:** CSV file content
- **Headers:** `Content-Disposition: attachment; filename=enriched_yes_companies.csv`

**Example with curl:**

```bash
curl -X POST https://your-api-url/run \
  -H "Content-Type: application/json" \
  -d '{"source_url": "https://signexpo.org/exhibitors"}' \
  --output enriched_output.csv
```

**Example with Python:**

```python
import requests

response = requests.post(
    "https://your-api-url/run",
    json={"source_url": "https://signexpo.org/exhibitors"},
    timeout=600  # 10 minutes
)

if response.status_code == 200:
    with open("enriched_output.csv", "wb") as f:
        f.write(response.content)
    print("CSV saved successfully")
else:
    print(f"Error: {response.status_code}")
    print(response.json())
```

**Example with n8n:**

1. Add HTTP Request node
2. Method: `POST`
3. URL: `https://your-api-url/run`
4. Body: JSON
   ```json
   {
     "source_url": "{{ $json.url }}"
   }
   ```
5. Set timeout to 600 seconds (10 minutes)
6. Response will contain CSV content

## Timeout and Resource Limits

- **API Timeout:** 10 minutes (600 seconds)
- **Recommended Memory:** 2GB minimum
- **Recommended CPU:** 2 vCPUs minimum
- **Recommended Cloud Run Timeout:** 15 minutes
- **Recommended Render Plan:** Standard or higher

## Troubleshooting

### Pipeline Timeout

- Increase timeout in `api.py` (TIMEOUT_SECONDS)
- Increase Cloud Run/Render timeout settings
- Check if the source URL is accessible

### Memory Issues

- Increase container memory allocation
- Cloud Run: Use `--memory 4Gi`
- Render: Upgrade to a higher plan

### Missing Output File

- Check logs for pipeline errors
- Verify `outputs/` directory is writable
- Ensure pipeline completes successfully

### Playwright Issues

- Ensure Chromium is installed: `playwright install chromium`
- Check Dockerfile includes Playwright dependencies
- For Cloud Run, may need to use `--no-sandbox` flag (add to Playwright config if needed)

## Security Notes

1. **API Keys:** Never commit API keys to version control
2. **Authentication:** For production, consider adding authentication (API keys, OAuth, etc.)
3. **Rate Limiting:** Consider adding rate limiting for production use
4. **Input Validation:** URLs are validated via Pydantic HttpUrl

## Monitoring

- **Cloud Run:** Use Cloud Logging and Cloud Monitoring
- **Render:** Use Render Dashboard logs
- **Local:** Check console output and `outputs/run_manifest.json`
