# Merchant Onboarding API Service

A standalone FastAPI microservice for merchant onboarding in a multi-merchant chatbot system. This service handles file uploads via signed URLs, processes knowledge base documents and product files, and sets up Vertex AI Search datastores.

## Features

- **Signed URL Uploads**: Direct file uploads to Google Cloud Storage (no file size limits)
- **Background Processing**: Asynchronous processing with progress tracking
- **Document Conversion**: Converts PDF, DOCX, TXT, HTML to NDJSON format for Vertex AI Search
- **Product Processing**: Processes CSV/XLSX product files into curated and full schemas
- **Website Crawling**: Uses Vertex AI Search's built-in crawler to automatically crawl merchant websites
- **Vertex AI Search Integration**: Automated datastore creation and document import
- **Status Tracking**: Real-time progress monitoring for onboarding jobs

## Architecture

```
onboarding-service/
├── onboarding_api.py          # Main FastAPI application
├── handlers/
│   ├── gcs_handler.py         # GCS operations + signed URL generation
│   ├── product_processor.py   # Product CSV/XLSX processing
│   ├── document_converter.py  # Document conversion to NDJSON
│   ├── vertex_setup.py        # Vertex AI Search setup (includes website crawling config)
│   └── config_generator.py    # Config JSON generation
├── utils/
│   └── status_tracker.py      # Status tracking
├── requirements.txt
├── Dockerfile
└── README.md
```

## API Endpoints

### File Upload

#### `POST /files/upload-url`
Generate signed URL for direct file upload to GCS.

**Request (Form Data):**
- `filename`: Original filename
- `content_type`: MIME type (e.g., `application/pdf`)
- `folder`: Folder name (`knowledge_base`, `prompt-docs`, `training_files`, `brand-images`)
- `user_id`: User identifier
- `expiration_minutes`: URL expiration time (default: 60)

**Response:**
```json
{
  "upload_url": "https://storage.googleapis.com/...",
  "object_path": "users/{user_id}/knowledge_base/document.pdf",
  "expires_in": 3600,
  "method": "PUT",
  "headers": {
    "Content-Type": "application/pdf"
  }
}
```

#### `POST /files/confirm`
Confirm file upload was successful.

**Request (Form Data):**
- `object_path`: GCS object path

**Response:**
```json
{
  "status": "confirmed",
  "object_path": "users/{user_id}/knowledge_base/document.pdf",
  "size": 12345,
  "content_type": "application/pdf",
  "created": "2024-01-01T00:00:00"
}
```

### Onboarding

#### `POST /onboard`
Start merchant onboarding process.

**Request (JSON):**
```json
{
  "merchant_id": "merchant-slug",
  "user_id": "firebase-uid",
  "shop_name": "Shop Name",
  "shop_url": "https://shop.com",
  "bot_name": "AI Assistant",
  "target_customer": "Tech-savvy millennials",
  "top_questions": "What are your return policies?",
  "top_products": "Product A, Product B",
  "primary_color": "#667eea",
  "secondary_color": "#764ba2",
  "logo_url": "gs://bucket/path/to/logo.png",
  "file_paths": {
    "knowledge": [
      "merchants/{merchant_id}/knowledge_base/doc1.pdf",
      "merchants/{merchant_id}/knowledge_base/doc2.docx"
    ]
  }
}
```

**Note:** 
- Upload `products.csv` and `categories.csv` to `knowledge_base/` folder initially
- The system will **auto-detect** them from `knowledge_base/` - no need to specify in `file_paths`
- You can omit `file_paths` entirely if all files are in `knowledge_base/`

**Response:**
```json
{
  "job_id": "merchant-slug_1234567890",
  "merchant_id": "merchant-slug",
  "status": "started",
  "status_url": "/onboard-status/merchant-slug"
}
```

#### `GET /onboard-status/{merchant_id}`
Get onboarding progress status.

**Response:**
```json
{
  "job_id": "merchant-slug_1234567890",
  "merchant_id": "merchant-slug",
  "user_id": "firebase-uid",
  "status": "in_progress",
  "progress": 50,
  "total_steps": 6,
  "current_step": "convert_documents",
  "steps": {
    "create_folders": {
      "status": "completed",
      "message": "Creating folder structure",
      "started_at": "2024-01-01T00:00:00",
      "completed_at": "2024-01-01T00:00:05"
    },
    "process_products": {
      "status": "completed",
      "message": "Processed 150 products"
    },
    "convert_documents": {
      "status": "in_progress",
      "message": "Converting documents to NDJSON"
    },
    "setup_vertex": {
      "status": "pending",
      "message": "Setting up Vertex AI Search"
    },
    "generate_config": {
      "status": "pending",
      "message": "Generating merchant configuration"
    },
    "finalize": {
      "status": "pending",
      "message": "Finalizing onboarding"
    }
  },
  "created_at": "2024-01-01T00:00:00",
  "updated_at": "2024-01-01T00:01:00"
}
```

### Merchant Management

#### Custom Chatbot Fields

The merchant config includes a `custom_chatbot` section for chatbot customization:

```json
{
  "custom_chatbot": {
    "title": "AI Assistant",              // Chatbot title/name
    "logo_signed_url": "",                // Signed URL for chatbot logo
    "color": "#667eea",                   // Primary color for chatbot UI
    "font_family": "Inter, sans-serif",   // Font family for chatbot text
    "tag_line": "",                       // Tag line displayed in chatbot
    "position": "bottom-right"            // Position: bottom-right, bottom-left, top-right, top-left
  }
}
```

These fields can be:
- **Set during onboarding** (via `bot_name`, `primary_color`, `logo_url`)
- **Updated via** `PATCH /merchants/{merchant_id}/config` endpoint
- **Retrieved** from the config file or via `GET /merchants/{merchant_id}`

#### `GET /merchants/{merchant_id}?user_id={user_id}`
Get merchant information.

**Query Parameters:**
- `user_id`: User identifier (required for security)

**Response:**
```json
{
  "merchant_id": "merchant-slug",
  "user_id": "firebase-uid",
  "shop_name": "My Store",
  "shop_url": "https://shop.com",
  "bot_name": "AI Assistant",
  "status": "active",
  "created_at": "2024-01-01T00:00:00",
  "updated_at": "2024-01-01T00:00:00"
}
```

#### `GET /merchants?user_id={user_id}`
List all merchants for a user.

**Query Parameters:**
- `user_id`: User identifier (required)

**Response:**
```json
{
  "user_id": "firebase-uid",
  "count": 2,
  "merchants": [
    {
      "merchant_id": "merchant-1",
      "shop_name": "Store 1",
      ...
    },
    {
      "merchant_id": "merchant-2",
      "shop_name": "Store 2",
      ...
    }
  ]
}
```

#### `PATCH /merchants/{merchant_id}?user_id={user_id}`
Update merchant information. Only provided fields will be updated.

**⚠️ IMPORTANT:** This endpoint ONLY updates:
- Database record
- `config.json` file (if config-relevant fields changed)
- Vertex AI Search datastore (if shop_name/shop_url changed)

**It does NOT re-run the full onboarding process:**
- Does NOT re-process products
- Does NOT re-convert documents
- Does NOT re-import to Vertex AI Search
- Does NOT re-create folders

To re-run full onboarding, use `POST /onboard` endpoint.

**⚠️ Auto-Updates:**
- **Config Regeneration:** If any config-relevant fields are updated, `config.json` will be automatically regenerated with the new values.
- **Vertex AI Search:** If `shop_name` or `shop_url` are updated, the Vertex AI Search datastore will be automatically updated:
  - `shop_name` changes → Updates datastore display name
  - `shop_url` changes → Re-registers site for website crawling

**Config-relevant fields** (trigger auto-regeneration):
- `shop_name`, `shop_url`, `bot_name`
- `primary_color`, `secondary_color`, `logo_url`
- `target_customer`, `customer_persona`, `bot_tone`, `prompt_text`
- `top_questions`, `top_products`

**Vertex-relevant fields** (trigger datastore update):
- `shop_name` - Updates datastore display name
- `shop_url` - Re-registers site for crawling

**Query Parameters:**
- `user_id`: User identifier (required for security)

**Request (JSON):**
```json
{
  "shop_name": "Updated Store Name",
  "bot_name": "New Bot Name",
  "primary_color": "#ff0000",
  "platform": "woocommerce",
  "custom_url_pattern": "/product/{handle}"
}
```

**Response:**
```json
{
  "merchant_id": "merchant-slug",
  "status": "updated",
  "updated_fields": ["shop_name", "bot_name", "primary_color", "platform", "custom_url_pattern"],
  "config_regenerated": true,
  "vertex_datastore_updated": true,
  "vertex_updated_fields": ["display_name", "site_registration"]
}
```

**Notes:**
- If config regeneration fails, the merchant update will still succeed. Check logs for any config regeneration errors.
- If Vertex AI Search datastore update fails, the merchant update will still succeed. Check logs for any Vertex update errors.
- Vertex datastore updates only occur if the datastore exists. If it doesn't exist, the update is skipped (no error).

#### `GET /merchants/{merchant_id}/config?user_id={user_id}`
Get merchant_config.json content including custom_chatbot fields.

**Query Parameters:**
- `user_id`: User identifier (required for security)

**Response:**
```json
{
  "merchant_id": "merchant-slug",
  "config_path": "merchants/merchant-slug/merchant_config.json",
  "config": {
    "user_id": "firebase-uid",
    "merchant_id": "merchant-slug",
    "shop_name": "My Store",
    "custom_chatbot": {
      "title": "AI Assistant",
      "logo_signed_url": "",
      "color": "#667eea",
      "font_family": "Inter, sans-serif",
      "tag_line": "",
      "position": "bottom-right"
    },
    ...
  }
}
```

#### `PATCH /merchants/{merchant_id}/config?user_id={user_id}`
Update merchant_config.json by merging provided fields with existing config.

**⚠️ CRITICAL:** This endpoint ONLY updates the `merchant_config.json` file in GCS.

**It does NOT:**
- ❌ Trigger onboarding process
- ❌ Re-process products
- ❌ Re-convert documents
- ❌ Re-import to Vertex AI Search
- ❌ Update database records
- ❌ Re-create folders
- ❌ Re-generate any other files

**It ONLY:**
- ✅ Updates merchant_config.json file
- ✅ Merges provided fields with existing config
- ✅ Preserves all other existing fields

**To re-run full onboarding, use `POST /onboard` endpoint.**

Perfect for updating `custom_chatbot` fields (title, logo, color, font, tag_line, position) without triggering onboarding.

**Behavior:**
- **Existing fields**: Updated with new values (field names preserved)
- **New fields**: Added to the config
- **Other fields**: All existing fields are automatically preserved
- **Nested objects**: Deep merge - updates/adds fields within nested structures

**Frontend can send any fields** - existing or new. The endpoint automatically handles merging.

**Query Parameters:**
- `user_id`: User identifier (required for security)

**Request (JSON):**
```json
{
  "shop_name": "Updated Shop Name",
  "custom_field": "new value",
  "branding": {
    "primary_color": "#ff0000",
    "tertiary_color": "#00ff00"
  },
  "new_section": {
    "field1": "value1",
    "field2": "value2"
  }
}
```

**Response:**
```json
{
  "merchant_id": "merchant-slug",
  "status": "updated",
  "config_path": "merchants/merchant-slug/merchant_config.json",
  "updated_fields": ["shop_name", "custom_field", "branding", "new_section"]
}
```

**Example - Update existing nested field:**
```json
// Request
{
  "branding": {
    "primary_color": "#ff0000"
  }
}

// Result: Only primary_color updated, secondary_color and other branding fields preserved
```

**Example - Add new field:**
```json
// Request
{
  "custom_settings": {
    "feature_enabled": true,
    "max_items": 10
  }
}

// Result: New custom_settings section added, all existing fields preserved
```

**Example - Update custom chatbot fields:**
```json
// Request
{
  "custom_chatbot": {
    "title": "Help Assistant",
    "logo_signed_url": "https://storage.googleapis.com/...",
    "color": "#667eea",
    "font_family": "Roboto, sans-serif",
    "tag_line": "How can I help you today?",
    "position": "bottom-left"
  }
}

// Result: Custom chatbot fields updated, all other config fields preserved
```

**Available chatbot position values:**
- `bottom-right` (default)
- `bottom-left`
- `top-right`
- `top-left`

#### `DELETE /merchants/{merchant_id}?user_id={user_id}`
Delete merchant and all associated data.

**⚠️ WARNING:** This will permanently delete:
- Merchant record from database
- All files in GCS (products, documents, configs)
- Vertex AI Search datastore (if exists)
- All onboarding job history

**Query Parameters:**
- `user_id`: User identifier (required for security)

**Response:**
```json
{
  "merchant_id": "merchant-slug",
  "status": "deleted",
  "message": "Merchant and associated data deleted successfully"
}
```

### Health Check

#### `GET /health`
Health check endpoint.

#### `GET /`
API information and available endpoints.

## File Upload Workflow

1. **Frontend calls** `POST /files/upload-url` with filename, content_type, folder, and user_id
2. **Backend returns** signed URL for direct GCS upload
3. **Frontend uploads** file directly to GCS using signed URL (PUT request)
4. **Frontend optionally calls** `POST /files/confirm` to verify upload
5. **Frontend calls** `POST /onboard` with merchant info and file paths

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GCS_BUCKET_NAME` | `chekout-ai` | Google Cloud Storage bucket name |
| `GCP_PROJECT_ID` | `shopify-473015` | Google Cloud Project ID |
| `GCP_LOCATION` | `global` | GCP location for Vertex AI Search |
| `VERTEX_COLLECTION` | `default_collection` | Vertex AI Search collection ID |
| `PORT` | `8080` | Server port |
| `ALLOWED_ORIGINS` | `*` | CORS allowed origins (comma-separated) |
| `SIGNED_URL_EXPIRATION` | `3600` | Signed URL expiration in seconds |

## Setup

### Local Development

1. **Install dependencies:**
```bash
pip install -r requirements.txt
```

2. **Set up Google Cloud credentials:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

3. **Set environment variables:**
```bash
export GCS_BUCKET_NAME="chekout-ai"
export GCP_PROJECT_ID="shopify-473015"
export GCP_LOCATION="global"
```

4. **Run the application:**
```bash
uvicorn onboarding_api:app --host 0.0.0.0 --port 8080 --reload
```

### Docker

1. **Build the image:**
```bash
docker build -t onboarding-service .
```

2. **Run the container:**
```bash
docker run -p 8080:8080 \
  -e GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json \
  -e GCS_BUCKET_NAME=chekout-ai \
  -e GCP_PROJECT_ID=shopify-473015 \
  -v /path/to/key.json:/path/to/key.json \
  onboarding-service
```

### Cloud Run Deployment

1. **Build and push to Google Container Registry:**
```bash
gcloud builds submit --tag gcr.io/shopify-473015/onboarding-service
```

2. **Deploy to Cloud Run:**
```bash
gcloud run deploy onboarding-service \
  --image gcr.io/shopify-473015/onboarding-service \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GCS_BUCKET_NAME=chekout-ai,GCP_PROJECT_ID=shopify-473015
```

## Processing Steps

The onboarding process includes the following steps:

1. **Create Folders**: Creates GCS folder structure for merchant
2. **Process Products**: Converts product CSV/XLSX to JSON and NDJSON formats
3. **Convert Documents**: Converts knowledge base documents to NDJSON format
4. **Setup Vertex**: Creates Vertex AI Search datastore with website crawling configuration (if shop_url provided) and imports documents (knowledge base, products)
5. **Generate Config**: Creates merchant configuration JSON
6. **Finalize**: Completes onboarding process

## Product Processing

### Curated Products (products.json)
**Location**: `merchants/{merchant_id}/prompt-docs/products.json`

Extracts essential fields for **frontend display** and Langflow:
- `name` - Product name (REQUIRED)
- `image_url` - Product image URL (REQUIRED for frontend display)
- `link` - Product URL/link (REQUIRED for frontend)
- `price` - Price (from variant_price) (REQUIRED)
- `compare_at_price` - Compare at price (optional, only if exists)

**Note**: Description is NOT included in products.json - it can be fetched from Vertex AI Search when needed.

**Usage**: 
- Frontend: Display product cards with images, names, prices, and links
- Langflow: Reference file for product lookups
- Contains ALL products but is NOT passed directly to Gemini prompts

### Full Products (products.ndjson)
**Location**: `merchants/{merchant_id}/training_files/products.ndjson`

Complete product schema for Vertex AI Search ingestion with ALL original fields.

**Usage**: Indexed in Vertex AI Search. When user asks about products, only the top 3-5 most relevant products are retrieved and passed to Gemini (not all 200).

### Important: RAG Architecture

**DO NOT** pass all 200 products to Gemini in every prompt. Instead:

1. **Index all products** in Vertex AI Search (via products.ndjson)
2. **Retrieve only relevant products** (3-5) when user asks a question
3. **Pass only retrieved products** to Gemini in the prompt

This approach:
- ✅ Reduces token usage and costs
- ✅ Provides better context (only relevant products)
- ✅ Faster responses
- ✅ Scales to thousands of products

See `PRODUCT_ARCHITECTURE.md` for detailed implementation guide.

## Document Conversion

Supports conversion from:
- **PDF**: Extracts text from all pages
- **DOCX**: Extracts text from paragraphs
- **TXT**: Direct text extraction
- **HTML**: Extracts text content (removes scripts/styles)

All documents are converted to NDJSON format with chunking for large files.

## Website Crawling

The service uses **Vertex AI Search's built-in website crawler** when a `shop_url` is provided:

- **Native Integration**: Leverages Vertex AI Search's native website crawling capabilities
- **Automatic Configuration**: Datastore is automatically configured for website crawling when `shop_url` is provided
- **No Custom Code**: No need for custom crawler implementation - Vertex AI Search handles everything
- **Efficient**: Vertex AI Search's crawler is optimized for search indexing
- **Managed Service**: Crawling, indexing, and updates are handled by Google's infrastructure

When a merchant provides a `shop_url` during onboarding, the datastore is created with website crawling enabled. Vertex AI Search will automatically crawl the website and index the content. The crawling configuration may need to be completed via the Google Cloud Console or SiteSearchEngine API depending on your setup.

## Configuration Schema

Generated `config.json` structure:
```json
{
  "user_id": "firebase-uid",
  "merchant_id": "merchant-slug",
  "shop_name": "Shop Name",
  "shop_url": "https://shop.com",
  "bot_name": "AI Assistant",
  "products": {
    "bucket_name": "chekout-ai",
    "file_path": "users/{user_id}/prompt-docs/products.json"
  },
  "vertex_search": {
    "project_id": "shopify-473015",
    "location": "global",
    "datastore_id": "{merchant_id}-engine"
  }
}
```

## Error Handling

- All endpoints include comprehensive error handling
- Background tasks track errors at each step
- Status endpoint shows detailed error messages
- Failed steps are marked with error details

## Logging

The service uses Python's logging module with console output. In production (Cloud Run), logs are automatically sent to Google Cloud Logging.

### Log Configuration

**Environment Variables:**
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default: `INFO`

### Log Output

- **Local Development**: Logs to console (stdout/stderr)
- **Production (Cloud Run)**: Logs automatically sent to Google Cloud Logging
- **Docker**: Logs to stdout/stderr (can be captured by Docker logging)

### Log Format

```
YYYY-MM-DD HH:MM:SS - logger_name - LEVEL - function_name:line_number - message
```

Example:
```
2024-01-01 12:00:00 - onboarding_api - INFO - start_onboarding:654 - Started onboarding job merchant-slug_1234567890 for merchant merchant-slug
```

### Viewing Logs

**Local Development:**
```bash
# View logs in terminal (stdout)
# Logs appear directly in console when running uvicorn
```

**Docker:**
```bash
# View container logs
docker logs -f container-name
```

**Cloud Run / Production:**
- Logs are automatically sent to Google Cloud Logging
- View in Cloud Console: Logging > Logs Explorer
- Filter by service name: `onboarding-service`

## License

Proprietary - Internal Use Only

