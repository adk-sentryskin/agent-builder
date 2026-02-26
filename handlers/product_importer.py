"""
Product importer: CSV/JSON/XLSX → platform-specific database tables with embeddings.

Handles two product onboarding paths:
1. API-connected merchants (Shopify Connect, WooCommerce keys) → products synced via webhook service
2. CSV-uploaded merchants → this module imports into the correct platform table

Supports: Shopify, WooCommerce, Squarespace
"""

import csv
import io
import json
import logging
import os
import re
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# Fake product IDs for CSV-imported products (avoid collision with API-synced)
BASE_PRODUCT_ID = 9900000000001
BASE_VARIANT_ID = 9900000100001

# Vertex AI embedding model (lazy-loaded, shared with document_converter)
_embedding_model = None
_aiplatform_initialized = False


class ProductImporter:
    """Import products from CSV/JSON/XLSX into platform-specific database tables with embeddings."""

    def __init__(self, gcs_handler):
        self.gcs_handler = gcs_handler

    def import_products(
        self,
        merchant_id: str,
        platform: str,
        products_file_path: str,
        shop_url: Optional[str] = None,
        shop_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Main entry point. Downloads file from GCS, parses, builds platform records,
        generates embeddings, inserts into correct platform table.

        Args:
            merchant_id: Merchant identifier
            platform: Platform name (shopify, woocommerce, squarespace)
            products_file_path: GCS path to products file
            shop_url: Shop URL (used for store FK entry)
            shop_name: Shop name

        Returns:
            dict with product_count, store_id, platform, etc.
        """
        platform = platform.strip().lower()
        if platform not in ("shopify", "woocommerce", "squarespace"):
            logger.info(f"Platform '{platform}' not supported for product DB import, skipping")
            return {"product_count": 0, "skipped": True, "reason": f"Unsupported platform: {platform}"}

        logger.info(f"Importing products for {merchant_id} (platform={platform}) from {products_file_path}")

        # Download file from GCS
        file_content = self.gcs_handler.download_file(products_file_path)
        filename = os.path.basename(products_file_path).lower()

        # Parse into rows
        if filename.endswith(".csv"):
            rows = self._parse_csv(file_content)
        elif filename.endswith(".json"):
            rows = self._parse_json(file_content)
        elif filename.endswith((".xlsx", ".xls")):
            rows = self._parse_excel(file_content, filename)
        else:
            logger.warning(f"Unsupported file format: {filename}")
            return {"product_count": 0, "error": f"Unsupported format: {filename}"}

        if not rows:
            logger.warning(f"No product rows parsed from {products_file_path}")
            return {"product_count": 0}

        logger.info(f"Parsed {len(rows)} rows from {filename}")

        # Import based on platform
        from utils.db_helpers import get_connection, return_connection
        conn = get_connection()

        try:
            store_id = self._ensure_store_entry(conn, merchant_id, platform, shop_url, shop_name)

            if platform == "shopify":
                count = self._import_shopify(conn, merchant_id, store_id, rows)
            elif platform == "woocommerce":
                count = self._import_woocommerce(conn, merchant_id, store_id, rows)
            elif platform == "squarespace":
                count = self._import_squarespace(conn, merchant_id, store_id, rows)
            else:
                count = 0

            conn.commit()
            logger.info(f"Imported {count} products for {merchant_id} into {platform} table")
            return {"product_count": count, "store_id": store_id, "platform": platform}

        except Exception as e:
            conn.rollback()
            logger.error(f"Product import failed for {merchant_id}: {e}", exc_info=True)
            raise
        finally:
            return_connection(conn)

    # ──────────────────────────────────────────────
    # File Parsing
    # ──────────────────────────────────────────────

    def _parse_csv(self, file_content: bytes) -> List[Dict[str, str]]:
        text = file_content.decode("utf-8-sig", errors="ignore")
        reader = csv.DictReader(io.StringIO(text))
        return [row for row in reader]

    def _parse_json(self, file_content: bytes) -> List[Dict[str, Any]]:
        data = json.loads(file_content.decode("utf-8", errors="ignore"))
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "products" in data:
            return data["products"]
        return [data]

    def _parse_excel(self, file_content: bytes, filename: str) -> List[Dict[str, str]]:
        try:
            import pandas as pd
            df = pd.read_excel(io.BytesIO(file_content))
            return df.fillna("").to_dict("records")
        except ImportError:
            logger.error("pandas/openpyxl not installed, cannot parse Excel")
            return []

    # ──────────────────────────────────────────────
    # Store FK Entry
    # ──────────────────────────────────────────────

    def _ensure_store_entry(
        self, conn, merchant_id: str, platform: str, shop_url: Optional[str], shop_name: Optional[str]
    ) -> int:
        """Create or get the store FK entry required by product tables."""
        cursor = conn.cursor()
        shop_url = shop_url or f"https://{merchant_id}.com"
        shop_name = shop_name or merchant_id

        if platform == "shopify":
            domain = shop_url.replace("https://", "").replace("http://", "").rstrip("/")
            # Handle both unique constraints: shop_domain AND merchant_id.
            # ON CONFLICT (shop_domain): when the same domain is reused (e.g. test merchants),
            # update merchant_id to the current merchant so the store row is claimed.
            # A separate ON CONFLICT (merchant_id) clause handles the case where merchant_id
            # already has a store row with a different domain — update domain to the new one.
            # We use a two-step approach: upsert on shop_domain, then fetch by shop_domain.
            cursor.execute(
                """INSERT INTO shopify_sync.shopify_stores (merchant_id, shop_domain, is_active)
                   VALUES (%s, %s, 1)
                   ON CONFLICT (shop_domain) DO UPDATE
                     SET merchant_id = EXCLUDED.merchant_id, is_active = 1""",
                (merchant_id, domain),
            )
            cursor.execute(
                "SELECT id FROM shopify_sync.shopify_stores WHERE shop_domain = %s", (domain,)
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Failed to create or fetch shopify store for domain {domain}")
            return row["id"] if isinstance(row, dict) else row[0]

        elif platform == "woocommerce":
            cursor.execute(
                """INSERT INTO woocommerce_sync.woocommerce_stores
                   (merchant_id, store_url, store_name, consumer_key, consumer_secret, is_active)
                   VALUES (%s, %s, %s, 'csv-import', 'csv-import', 1)
                   ON CONFLICT (merchant_id) DO NOTHING""",
                (merchant_id, shop_url, shop_name),
            )
            cursor.execute(
                "SELECT id FROM woocommerce_sync.woocommerce_stores WHERE merchant_id = %s",
                (merchant_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Failed to create or fetch woocommerce store for merchant {merchant_id}")
            return row["id"] if isinstance(row, dict) else row[0]

        elif platform == "squarespace":
            cursor.execute(
                """INSERT INTO squarespace_sync.squarespace_stores
                   (merchant_id, site_url, site_name, is_active)
                   VALUES (%s, %s, %s, 1)
                   ON CONFLICT (merchant_id) DO NOTHING""",
                (merchant_id, shop_url, shop_name),
            )
            cursor.execute(
                "SELECT id FROM squarespace_sync.squarespace_stores WHERE merchant_id = %s",
                (merchant_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"Failed to create or fetch squarespace store for merchant {merchant_id}")
            return row["id"] if isinstance(row, dict) else row[0]

        raise ValueError(f"Unknown platform: {platform}")

    def _fetch_id(self, cursor, merchant_id: str, table: str) -> int:
        """Fallback to fetch store ID after ON CONFLICT DO NOTHING."""
        cursor.execute(f"SELECT id FROM {table} WHERE merchant_id = %s", (merchant_id,))
        row = cursor.fetchone()
        if isinstance(row, dict):
            return row["id"]
        return row[0]

    # ──────────────────────────────────────────────
    # Embeddings
    # ──────────────────────────────────────────────

    def _get_embedding_model(self):
        global _embedding_model, _aiplatform_initialized
        if not _aiplatform_initialized:
            from google.cloud import aiplatform
            project_id = os.getenv("GCP_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT", "shopify-473015")
            region = os.getenv("GCP_REGION", "us-central1")
            aiplatform.init(project=project_id, location=region)
            _aiplatform_initialized = True
        if _embedding_model is None:
            from vertexai.language_models import TextEmbeddingModel
            _embedding_model = TextEmbeddingModel.from_pretrained("text-embedding-004")
            logger.info("Loaded text-embedding-004 model for product embeddings")
        return _embedding_model

    def _generate_embeddings_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        try:
            from vertexai.language_models import TextEmbeddingInput
            model = self._get_embedding_model()
            inputs = [TextEmbeddingInput(text=t[:20000], task_type="RETRIEVAL_DOCUMENT") for t in texts]
            result = model.get_embeddings(inputs)
            return [e.values for e in result]
        except Exception as e:
            logger.error(f"Embedding batch failed: {e}")
            return [None] * len(texts)

    def _generate_all_embeddings(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Generate embeddings in batches of 25."""
        all_embeddings = []
        batch_size = 25
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            logger.info(f"Generating embeddings batch {i // batch_size + 1} ({len(batch)} texts)")
            embeddings = self._generate_embeddings_batch(batch)
            all_embeddings.extend(embeddings)
        return all_embeddings

    def _embedding_str(self, embedding: Optional[List[float]]) -> Optional[str]:
        if embedding is None:
            return None
        return "[" + ",".join(map(str, embedding)) + "]"

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    def _strip_html(self, html: str) -> str:
        if not html:
            return ""
        try:
            from bs4 import BeautifulSoup
            return BeautifulSoup(html, "html.parser").get_text(separator=" ").strip()
        except ImportError:
            return re.sub(r"<[^>]+>", " ", html).strip()

    def _get_col(self, row: Dict, *keys: str) -> str:
        """Get first matching column value from a row (case-insensitive)."""
        row_lower = {k.lower().strip(): v for k, v in row.items()}
        for key in keys:
            val = row_lower.get(key.lower().strip(), "")
            if isinstance(val, str):
                val = val.strip()
            if val:
                return str(val)
        return ""

    # ──────────────────────────────────────────────
    # Shopify Import
    # ──────────────────────────────────────────────

    def _import_shopify(self, conn, merchant_id: str, store_id: int, rows: List[Dict]) -> int:
        """Import Shopify CSV format: group by Handle, build raw_data JSONB."""
        cursor = conn.cursor()

        # Delete existing products for clean re-import
        cursor.execute("DELETE FROM shopify_sync.products WHERE merchant_id = %s", (merchant_id,))
        deleted = cursor.rowcount
        if deleted:
            logger.info(f"Deleted {deleted} existing Shopify products for {merchant_id}")

        # Check if this is Shopify-format CSV (has Handle column)
        has_handle = any("Handle" in row or "handle" in row for row in rows[:3])

        if has_handle:
            products = self._build_shopify_products_from_csv(rows)
        else:
            products = self._build_shopify_products_generic(rows)

        if not products:
            return 0

        # Generate embeddings
        embed_texts = []
        for p in products:
            text = f"{p['title']} {p.get('body_text', '')} {p.get('tags', '')} {p.get('product_type', '')}".strip()
            embed_texts.append(text)

        embeddings = self._generate_all_embeddings(embed_texts)

        # Insert
        inserted = 0
        for product, embedding in zip(products, embeddings):
            raw_json = json.dumps(product["raw_data"])
            emb_str = self._embedding_str(embedding)

            if emb_str:
                cursor.execute(
                    """INSERT INTO shopify_sync.products
                       (shopify_product_id, store_id, title, vendor, product_type, handle,
                        status, raw_data, is_deleted, merchant_id, embedding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, 0, %s, %s::vector)""",
                    (product["shopify_product_id"], store_id, product["title"],
                     product.get("vendor", ""), product.get("product_type", ""),
                     product["handle"], product.get("status", "active"), raw_json,
                     merchant_id, emb_str),
                )
            else:
                cursor.execute(
                    """INSERT INTO shopify_sync.products
                       (shopify_product_id, store_id, title, vendor, product_type, handle,
                        status, raw_data, is_deleted, merchant_id)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, 0, %s)""",
                    (product["shopify_product_id"], store_id, product["title"],
                     product.get("vendor", ""), product.get("product_type", ""),
                     product["handle"], product.get("status", "active"), raw_json,
                     merchant_id),
                )
            inserted += 1

        return inserted

    def _build_shopify_products_from_csv(self, rows: List[Dict]) -> List[Dict]:
        """Parse Shopify CSV export format (grouped by Handle)."""
        # Group by Handle
        groups: Dict[str, List[Dict]] = {}
        for row in rows:
            handle = self._get_col(row, "Handle")
            if not handle:
                continue
            groups.setdefault(handle, []).append(row)

        products = []
        for idx, (handle, group_rows) in enumerate(groups.items()):
            first = group_rows[0]
            product_id = BASE_PRODUCT_ID + idx

            title = self._get_col(first, "Title")
            body_html = self._get_col(first, "Body (HTML)", "Body")
            vendor = self._get_col(first, "Vendor")
            product_type = self._get_col(first, "Type", "Product Type")
            tags = self._get_col(first, "Tags")
            status = self._get_col(first, "Status") or "active"

            # Options
            option_names = {}
            for row in group_rows:
                for i in range(1, 4):
                    name = self._get_col(row, f"Option{i} Name")
                    if name and i not in option_names:
                        option_names[i] = name

            option_values: Dict[int, set] = {i: set() for i in range(1, 4)}
            for row in group_rows:
                for i in range(1, 4):
                    val = self._get_col(row, f"Option{i} Value")
                    if val and i in option_names:
                        option_values[i].add(val)

            options = []
            for pos, name in sorted(option_names.items()):
                options.append({
                    "id": product_id * 10 + pos,
                    "name": name,
                    "position": pos,
                    "product_id": product_id,
                    "values": sorted(option_values.get(pos, set())),
                })

            # Variants
            variants = []
            variant_idx = 0
            for row in group_rows:
                price = self._get_col(row, "Variant Price")
                if not price:
                    continue
                variant_id = BASE_VARIANT_ID + idx * 100 + variant_idx

                opt1 = self._get_col(row, "Option1 Value") or None
                opt2 = self._get_col(row, "Option2 Value") or None
                opt3 = self._get_col(row, "Option3 Value") or None
                parts = [v for v in [opt1, opt2, opt3] if v]
                variant_title = " / ".join(parts) if parts else "Default"

                sku = self._get_col(row, "Variant SKU")
                compare_price = self._get_col(row, "Variant Compare At Price") or None
                inv_qty = 0
                try:
                    inv_qty = int(self._get_col(row, "Variant Inventory Qty") or "0")
                except ValueError:
                    pass

                variants.append({
                    "id": variant_id,
                    "product_id": product_id,
                    "title": variant_title,
                    "price": price,
                    "compare_at_price": compare_price,
                    "sku": sku or None,
                    "position": variant_idx + 1,
                    "option1": opt1,
                    "option2": opt2,
                    "option3": opt3,
                    "inventory_quantity": inv_qty,
                    "inventory_policy": self._get_col(row, "Variant Inventory Policy") or "deny",
                    "inventory_management": "shopify",
                    "fulfillment_service": "manual",
                    "requires_shipping": True,
                    "taxable": True,
                })
                variant_idx += 1

            # Images
            images = []
            for row in group_rows:
                img_src = self._get_col(row, "Image Src")
                if not img_src:
                    continue
                try:
                    position = int(self._get_col(row, "Image Position") or "1")
                except ValueError:
                    position = len(images) + 1
                alt = self._get_col(row, "Image Alt Text")
                images.append({
                    "id": product_id * 100 + position,
                    "product_id": product_id,
                    "position": position,
                    "src": img_src,
                    "alt": alt,
                    "width": 1000,
                    "height": 1000,
                    "variant_ids": [],
                })

            raw_data = {
                "id": product_id,
                "title": title,
                "handle": handle,
                "body_html": body_html,
                "vendor": vendor,
                "product_type": product_type,
                "tags": tags,
                "status": status,
                "published_at": "2026-01-01T00:00:00-05:00",
                "created_at": "2026-01-01T00:00:00-05:00",
                "updated_at": "2026-01-01T00:00:00-05:00",
                "published_scope": "web",
                "options": options,
                "variants": variants,
                "images": images,
                "image": images[0] if images else None,
            }

            products.append({
                "shopify_product_id": product_id,
                "title": title,
                "vendor": vendor,
                "product_type": product_type,
                "handle": handle,
                "status": status,
                "raw_data": raw_data,
                "body_text": self._strip_html(body_html),
                "tags": tags,
            })

        return products

    def _build_shopify_products_generic(self, rows: List[Dict]) -> List[Dict]:
        """Build Shopify-format products from generic CSV (one row per product)."""
        products = []
        for idx, row in enumerate(rows):
            product_id = BASE_PRODUCT_ID + idx
            title = self._get_col(row, "title", "name", "product_name", "product_title")
            if not title:
                continue

            handle = self._get_col(row, "handle", "slug", "url") or re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
            price = self._get_col(row, "price", "variant_price", "amount") or "0"
            description = self._get_col(row, "description", "body", "body_html", "body (html)")
            vendor = self._get_col(row, "vendor", "brand")
            product_type = self._get_col(row, "type", "product_type", "category")
            tags = self._get_col(row, "tags")
            image = self._get_col(row, "image", "image_url", "featured_image", "image_src")
            compare_price = self._get_col(row, "compare_at_price", "original_price", "compare_price") or None

            variant = {
                "id": BASE_VARIANT_ID + idx * 100,
                "product_id": product_id,
                "title": "Default",
                "price": price,
                "compare_at_price": compare_price,
                "position": 1,
                "option1": "Default",
                "inventory_quantity": 100,
                "inventory_policy": "deny",
            }

            images = []
            if image:
                images.append({
                    "id": product_id * 100 + 1,
                    "product_id": product_id,
                    "position": 1,
                    "src": image,
                    "alt": title,
                    "width": 1000,
                    "height": 1000,
                    "variant_ids": [],
                })

            raw_data = {
                "id": product_id,
                "title": title,
                "handle": handle,
                "body_html": description,
                "vendor": vendor,
                "product_type": product_type,
                "tags": tags,
                "status": "active",
                "options": [{"id": product_id * 10 + 1, "name": "Title", "position": 1, "values": ["Default"]}],
                "variants": [variant],
                "images": images,
                "image": images[0] if images else None,
            }

            products.append({
                "shopify_product_id": product_id,
                "title": title,
                "vendor": vendor,
                "product_type": product_type,
                "handle": handle,
                "status": "active",
                "raw_data": raw_data,
                "body_text": self._strip_html(description),
                "tags": tags,
            })

        return products

    # ──────────────────────────────────────────────
    # WooCommerce Import
    # ──────────────────────────────────────────────

    def _import_woocommerce(self, conn, merchant_id: str, store_id: int, rows: List[Dict]) -> int:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM woocommerce_sync.products WHERE merchant_id = %s", (merchant_id,))

        products = []
        for idx, row in enumerate(rows):
            product_id = BASE_PRODUCT_ID + idx
            name = self._get_col(row, "name", "title", "product_name", "product_title")
            if not name:
                continue

            slug = self._get_col(row, "slug", "handle", "url") or re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
            price = self._get_col(row, "price", "regular_price", "variant_price", "amount") or "0"
            sale_price = self._get_col(row, "sale_price") or ""
            description = self._get_col(row, "description", "body", "body_html")
            sku = self._get_col(row, "sku")
            product_type = self._get_col(row, "type", "product_type") or "simple"
            categories_str = self._get_col(row, "categories", "category")
            tags_str = self._get_col(row, "tags")
            image = self._get_col(row, "image", "image_url", "featured_image", "image_src")
            status = self._get_col(row, "status") or "publish"

            categories = [{"id": i, "name": c.strip()} for i, c in enumerate(categories_str.split(","))] if categories_str else []
            tags = [{"id": i, "name": t.strip()} for i, t in enumerate(tags_str.split(","))] if tags_str else []
            images = [{"id": product_id * 100 + 1, "src": image, "alt": name}] if image else []

            raw_data = {
                "id": product_id,
                "name": name,
                "slug": slug,
                "description": description,
                "short_description": "",
                "sku": sku,
                "price": price,
                "regular_price": price,
                "sale_price": sale_price,
                "type": product_type,
                "status": status,
                "categories": categories,
                "tags": tags,
                "images": images,
                "variations": [],
            }

            products.append({
                "wc_product_id": product_id,
                "name": name,
                "slug": slug,
                "sku": sku,
                "type": product_type,
                "status": status,
                "price": price,
                "regular_price": price,
                "sale_price": sale_price,
                "categories": json.dumps(categories),
                "tags": json.dumps(tags),
                "raw_data": raw_data,
                "body_text": self._strip_html(description),
                "tags_str": tags_str,
            })

        if not products:
            return 0

        embed_texts = [f"{p['name']} {p.get('body_text', '')} {p.get('tags_str', '')} {p.get('type', '')}".strip() for p in products]
        embeddings = self._generate_all_embeddings(embed_texts)

        inserted = 0
        for product, embedding in zip(products, embeddings):
            emb_str = self._embedding_str(embedding)
            raw_json = json.dumps(product["raw_data"])

            if emb_str:
                cursor.execute(
                    """INSERT INTO woocommerce_sync.products
                       (wc_product_id, store_id, merchant_id, name, slug, sku, type, status,
                        price, regular_price, sale_price, categories, tags, raw_data, is_deleted, embedding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, 0, %s::vector)""",
                    (product["wc_product_id"], store_id, merchant_id, product["name"],
                     product["slug"], product["sku"], product["type"], product["status"],
                     product["price"], product["regular_price"], product["sale_price"],
                     product["categories"], product["tags"], raw_json, emb_str),
                )
            else:
                cursor.execute(
                    """INSERT INTO woocommerce_sync.products
                       (wc_product_id, store_id, merchant_id, name, slug, sku, type, status,
                        price, regular_price, sale_price, categories, tags, raw_data, is_deleted)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, 0)""",
                    (product["wc_product_id"], store_id, merchant_id, product["name"],
                     product["slug"], product["sku"], product["type"], product["status"],
                     product["price"], product["regular_price"], product["sale_price"],
                     product["categories"], product["tags"], raw_json),
                )
            inserted += 1

        return inserted

    # ──────────────────────────────────────────────
    # Squarespace Import
    # ──────────────────────────────────────────────

    def _import_squarespace(self, conn, merchant_id: str, store_id: int, rows: List[Dict]) -> int:
        cursor = conn.cursor()

        # Delete existing (cascade deletes variants)
        cursor.execute("DELETE FROM squarespace_sync.squarespace_products WHERE merchant_id = %s", (merchant_id,))

        products = []
        for idx, row in enumerate(rows):
            product_id_str = str(BASE_PRODUCT_ID + idx)
            title = self._get_col(row, "title", "name", "product_name")
            if not title:
                continue

            handle = self._get_col(row, "handle", "slug", "url") or re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
            description = self._get_col(row, "description", "body", "body_html")
            price_str = self._get_col(row, "price", "regular_price", "amount") or "0"
            sale_price_str = self._get_col(row, "sale_price") or None
            sku = self._get_col(row, "sku")
            product_type = self._get_col(row, "type", "product_type") or "PHYSICAL"
            categories_str = self._get_col(row, "categories", "category")
            tags_str = self._get_col(row, "tags")
            image = self._get_col(row, "image", "image_url", "featured_image", "image_src")
            stock = self._get_col(row, "stock", "inventory", "quantity") or "Unlimited"

            try:
                price = float(re.sub(r"[^\d.]", "", price_str)) if price_str else 0
            except ValueError:
                price = 0
            try:
                sale_price = float(re.sub(r"[^\d.]", "", sale_price_str)) if sale_price_str else None
            except ValueError:
                sale_price = None

            categories = [c.strip() for c in categories_str.split(",") if c.strip()] if categories_str else []
            tags = [t.strip() for t in tags_str.split(",") if t.strip()] if tags_str else []
            image_urls = [image] if image else []

            products.append({
                "squarespace_product_id": product_id_str,
                "title": title,
                "description": description,
                "handle": handle,
                "product_type": product_type,
                "sku": sku,
                "price": price,
                "sale_price": sale_price,
                "stock": stock,
                "categories": categories,
                "tags": tags,
                "image_urls": image_urls,
                "body_text": self._strip_html(description),
                "tags_str": tags_str,
            })

        if not products:
            return 0

        embed_texts = [f"{p['title']} {p.get('body_text', '')} {p.get('tags_str', '')} {p.get('product_type', '')}".strip() for p in products]
        embeddings = self._generate_all_embeddings(embed_texts)

        inserted = 0
        for product, embedding in zip(products, embeddings):
            emb_str = self._embedding_str(embedding)

            raw_data = json.dumps({
                "title": product["title"],
                "description": product["description"],
                "handle": product["handle"],
                "price": product["price"],
                "sale_price": product["sale_price"],
                "categories": product["categories"],
                "tags": product["tags"],
                "image_urls": product["image_urls"],
            })

            if emb_str:
                cursor.execute(
                    """INSERT INTO squarespace_sync.squarespace_products
                       (squarespace_product_id, store_id, merchant_id, title, description, handle,
                        product_type, sku, price, sale_price, stock, categories, tags, image_urls,
                        raw_data, is_deleted, embedding)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 0, %s::vector)""",
                    (product["squarespace_product_id"], store_id, merchant_id, product["title"],
                     product["description"], product["handle"], product["product_type"],
                     product["sku"], product["price"], product["sale_price"], product["stock"],
                     product["categories"], product["tags"], product["image_urls"],
                     raw_data, emb_str),
                )
            else:
                cursor.execute(
                    """INSERT INTO squarespace_sync.squarespace_products
                       (squarespace_product_id, store_id, merchant_id, title, description, handle,
                        product_type, sku, price, sale_price, stock, categories, tags, image_urls,
                        raw_data, is_deleted)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, 0)""",
                    (product["squarespace_product_id"], store_id, merchant_id, product["title"],
                     product["description"], product["handle"], product["product_type"],
                     product["sku"], product["price"], product["sale_price"], product["stock"],
                     product["categories"], product["tags"], product["image_urls"], raw_data),
                )

            db_product_id = cursor.fetchone()[0] if cursor.description else None

            # For Squarespace, insert a default variant
            if db_product_id is None:
                cursor.execute(
                    "SELECT id FROM squarespace_sync.squarespace_products WHERE squarespace_product_id = %s AND merchant_id = %s",
                    (product["squarespace_product_id"], merchant_id),
                )
                row = cursor.fetchone()
                db_product_id = row[0] if row else None

            if db_product_id:
                cursor.execute(
                    """INSERT INTO squarespace_sync.squarespace_variants
                       (product_id, squarespace_variant_id, sku, price, stock,
                        option1_name, option1_value)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (db_product_id, f"var-{product['squarespace_product_id']}",
                     product["sku"], product["price"], product["stock"],
                     "Default", "Default"),
                )

            inserted += 1

        return inserted
