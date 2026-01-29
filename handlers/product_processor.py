"""Product file processor for CSV/XLSX files"""

import os
import json
import re
import base64
import logging
from typing import List, Dict, Any, Optional
import pandas as pd
from io import BytesIO

logger = logging.getLogger(__name__)


class ProductProcessor:
    """Process product CSV/XLSX files"""

    def __init__(self, gcs_handler):
        """
        Initialize product processor

        Args:
            gcs_handler: GCSHandler instance
        """
        self.gcs_handler = gcs_handler

    def process_products_file(
        self,
        merchant_id: str,
        products_file_path: str,
        shop_url: Optional[str] = None,
        platform: Optional[str] = None,
        custom_url_pattern: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Process product file and create two outputs:
        1. products.json (curated for Langflow)
        2. products.ndjson (full schema for Vertex AI Search)

        Args:
            merchant_id: Merchant identifier
            products_file_path: GCS path to products file
            shop_url: Shop URL to construct full product URLs from handles
            platform: E-commerce platform ('shopify', 'woocommerce', 'wordpress', 'custom')
            custom_url_pattern: Custom URL pattern for 'custom' platform (e.g., '/item/{handle}')

        Returns:
            dict with paths to generated files
        """
        try:
            # Download product file from GCS
            logger.info(f"Downloading products file: {products_file_path}")
            file_content = self.gcs_handler.download_file(products_file_path)

            # Determine file type and read
            if products_file_path.endswith('.json'):
                # JSON file - already in curated format
                products_data = json.loads(file_content.decode('utf-8'))
                if not isinstance(products_data, list):
                    raise ValueError("JSON products file must contain an array of products")
                
                logger.info(f"Loaded {len(products_data)} products from JSON file")
                
                # Process JSON products with URL construction
                curated_products = self._process_json_products(
                    products_data, 
                    shop_url=shop_url,
                    platform=platform,
                    custom_url_pattern=custom_url_pattern
                )
                full_products = self._create_full_products_from_json(
                    curated_products, 
                    shop_url=shop_url,
                    platform=platform,
                    custom_url_pattern=custom_url_pattern
                )
                
            elif products_file_path.endswith('.xlsx') or products_file_path.endswith('.xls'):
                df = pd.read_excel(BytesIO(file_content))
                logger.info(f"Loaded {len(df)} products from Excel file")
                
                # Process products from dataframe
                curated_products = self._create_curated_products(
                    df, 
                    shop_url=shop_url,
                    platform=platform,
                    custom_url_pattern=custom_url_pattern
                )
                full_products = self._create_full_products(
                    df, 
                    shop_url=shop_url,
                    platform=platform,
                    custom_url_pattern=custom_url_pattern
                )
                
            elif products_file_path.endswith('.csv'):
                df = pd.read_csv(BytesIO(file_content))
                logger.info(f"Loaded {len(df)} products from CSV file")
                
                # Process products from dataframe
                curated_products = self._create_curated_products(
                    df, 
                    shop_url=shop_url,
                    platform=platform,
                    custom_url_pattern=custom_url_pattern
                )
                full_products = self._create_full_products(
                    df, 
                    shop_url=shop_url,
                    platform=platform,
                    custom_url_pattern=custom_url_pattern
                )
            else:
                raise ValueError(f"Unsupported file type: {products_file_path}. Supported: .json, .csv, .xlsx, .xls")

            # Upload curated products.json
            products_json_path = f"merchants/{merchant_id}/prompt-docs/products.json"
            products_json_content = json.dumps(curated_products, indent=2)
            self.gcs_handler.upload_file(
                products_json_path,
                products_json_content.encode('utf-8'),
                content_type="application/json"
            )
            logger.info(f"Uploaded curated products.json: {products_json_path}")

            # Upload full products.ndjson
            products_ndjson_path = f"merchants/{merchant_id}/training_files/products.ndjson"
            products_ndjson_content = self._create_ndjson(full_products)
            self.gcs_handler.upload_file(
                products_ndjson_path,
                products_ndjson_content.encode('utf-8'),
                content_type="application/x-ndjson"
            )
            logger.info(f"Uploaded products.ndjson: {products_ndjson_path}")

            product_count = len(curated_products)
            
            return {
                "curated_products": products_json_path,
                "full_products": products_ndjson_path,
                "product_count": product_count
            }

        except Exception as e:
            logger.error(f"Error processing products file: {e}")
            raise

    def _construct_product_url(
        self, 
        link_value: str, 
        shop_url: Optional[str] = None,
        platform: Optional[str] = None,
        custom_url_pattern: Optional[str] = None
    ) -> str:
        """
        Construct full product URL from handle/slug and shop URL based on platform
        
        Args:
            link_value: Product handle, slug, or full URL
            shop_url: Shop base URL (e.g., https://shop.com)
            platform: E-commerce platform type ('shopify', 'woocommerce', 'wordpress', 'custom', or None for auto-detect)
            custom_url_pattern: Custom URL pattern for 'custom' platform (e.g., '/item/{handle}' or '/p/{handle}')
            
        Returns:
            Full product URL
        """
        if not link_value:
            return None
        
        link_value = str(link_value).strip()
        
        # If it's already a full URL, return as-is
        if link_value.startswith('http://') or link_value.startswith('https://'):
            return link_value
        
        # If shop_url is not provided, return handle as-is
        if not shop_url:
            return link_value
        
        # Remove trailing slash from shop_url if present
        shop_url = shop_url.rstrip('/')
        
        # Determine URL pattern based on platform
        # Priority: custom_url_pattern > explicit platform > auto-detect
        url_pattern = None
        
        # If custom_url_pattern is provided, use it (highest priority)
        if custom_url_pattern:
            url_pattern = custom_url_pattern
            logger.info(f"Using custom URL pattern: {custom_url_pattern}")
        
        # If no custom pattern, use platform-specific patterns
        if not url_pattern:
            if platform:
                platform_lower = platform.lower()
                if platform_lower == 'shopify':
                    url_pattern = '/products/{handle}'
                elif platform_lower == 'woocommerce':
                    url_pattern = '/product/{handle}'  # WooCommerce uses singular "product"
                elif platform_lower == 'wordpress':
                    url_pattern = '/product/{handle}'  # WordPress/WooCommerce typically uses singular
                elif platform_lower == 'squarespace':
                    url_pattern = '/p/{handle}'  # Squarespace commerce typically uses /p/{handle}
                elif platform_lower == 'custom':
                    # Custom platform but no pattern provided - default to Shopify
                    logger.warning(f"Platform is 'custom' but no custom_url_pattern provided, defaulting to Shopify pattern")
                    url_pattern = '/products/{handle}'
                else:
                    # Unknown platform, default to Shopify pattern
                    logger.warning(f"Unknown platform '{platform}', defaulting to Shopify pattern")
                    url_pattern = '/products/{handle}'
            else:
                # Auto-detect platform from shop_url (basic heuristics)
                shop_url_lower = shop_url.lower()
                if 'woocommerce' in shop_url_lower or 'wordpress' in shop_url_lower:
                    url_pattern = '/product/{handle}'
                elif 'squarespace' in shop_url_lower:
                    url_pattern = '/p/{handle}'
                elif 'shopify' in shop_url_lower or '.myshopify.com' in shop_url_lower:
                    url_pattern = '/products/{handle}'
                else:
                    # Default to Shopify pattern (most common)
                    url_pattern = '/products/{handle}'
                    logger.info(f"Platform not specified, defaulting to Shopify pattern for {shop_url}")
        
        # Construct URL using pattern
        # Replace {handle} placeholder with actual handle
        if '{handle}' in url_pattern:
            constructed_url = url_pattern.replace('{handle}', link_value)
            return f"{shop_url}{constructed_url}"
        else:
            # If pattern doesn't have {handle}, append handle directly
            return f"{shop_url}{url_pattern}/{link_value}"

    def _format_handle_as_name(self, handle: str) -> str:
        """
        Format a handle (e.g., 'anvil-try') into a readable product name (e.g., 'Anvil Try')
        
        Args:
            handle: Product handle (may contain hyphens, underscores, etc.)
        
        Returns:
            Formatted product name with capitalized words
        """
        if not handle:
            return handle
        
        # Replace hyphens and underscores with spaces
        formatted = handle.replace('-', ' ').replace('_', ' ')
        
        # Capitalize each word
        formatted = ' '.join(word.capitalize() for word in formatted.split())
        
        return formatted

    def _create_curated_products(
        self, 
        df: pd.DataFrame, 
        shop_url: Optional[str] = None,
        platform: Optional[str] = None,
        custom_url_pattern: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Create curated products for Langflow prompt-docs and frontend display
        Includes essential fields: name, image_url, link, price (required for frontend)
        Description can be fetched from Vertex AI Search when needed

        Args:
            df: Product dataframe
            shop_url: Shop URL to construct full product URLs from handles
            platform: E-commerce platform ('shopify', 'woocommerce', 'wordpress', 'custom')
            custom_url_pattern: Custom URL pattern for 'custom' platform

        Returns:
            List of curated product dictionaries with essential fields for frontend
        """
        curated = []

        # Map column names for essential fields (required for frontend display)
        # NOTE: Only these fields are extracted for products.json
        # Description is NOT included - fetch from Vertex AI Search when needed
        column_mapping = {
            'name': ['title', 'name', 'product_name', 'product title', 'product_title'],
            'image_url': ['image', 'image_url', 'image_src', 'featured_image', 'featured_image_url', 'image_url_1'],
            'link': ['url', 'link', 'handle', 'product_url', 'product_link', 'product_handle'],
            'price': ['price', 'variant_price', 'amount', 'cost'],
            'compare_at_price': ['compare_at_price', 'variant_compare_at_price', 'original_price']
        }

        # Find actual column names (case-insensitive)
        actual_columns = {}
        df_columns_lower = {col.lower(): col for col in df.columns}
        
        for target, possible_names in column_mapping.items():
            for possible_name in possible_names:
                if possible_name.lower() in df_columns_lower:
                    actual_columns[target] = df_columns_lower[possible_name.lower()]
                    break

        # Also find handle column separately (might be separate from link)
        handle_col = None
        handle_columns = ['handle', 'product_handle', 'slug', 'product_slug']
        for col in handle_columns:
            if col.lower() in df_columns_lower:
                handle_col = df_columns_lower[col.lower()]
                break

        logger.info(f"Found columns mapping: {actual_columns}")

        for _, row in df.iterrows():
            product = {}

            # Extract name (REQUIRED)
            # Try title/name column first, then fallback to handle
            name_col = actual_columns.get('name')
            
            if name_col and pd.notna(row.get(name_col)) and str(row[name_col]).strip():
                product['name'] = str(row[name_col]).strip()
            elif handle_col and pd.notna(row.get(handle_col)) and str(row[handle_col]).strip():
                # Use handle as fallback if title/name is null - format it nicely
                handle_value = str(row[handle_col]).strip()
                product['name'] = self._format_handle_as_name(handle_value)
            elif actual_columns.get('link') and pd.notna(row.get(actual_columns['link'])) and str(row[actual_columns['link']]).strip():
                # Try link column as last resort (might contain handle)
                link_value = str(row[actual_columns['link']]).strip()
                # If link looks like a handle (no http/https), format it
                if not link_value.startswith(('http://', 'https://')):
                    product['name'] = self._format_handle_as_name(link_value)
                else:
                    product['name'] = link_value
            else:
                product['name'] = 'Untitled Product'

            # Extract image_url (REQUIRED for frontend)
            image_col = actual_columns.get('image_url')
            if image_col and pd.notna(row.get(image_col)):
                image_value = str(row[image_col]).strip()
                product['image_url'] = image_value if image_value else None
            else:
                # Try to find any image column
                for col in df.columns:
                    if 'image' in col.lower() and pd.notna(row.get(col)):
                        product['image_url'] = str(row[col]).strip()
                        break
                else:
                    product['image_url'] = None

            # Extract link (REQUIRED for frontend)
            link_col = actual_columns.get('link')
            link_value = None
            if link_col and pd.notna(row.get(link_col)):
                link_value = str(row[link_col]).strip()
            else:
                # Try to find any URL/link column
                for col in df.columns:
                    if any(term in col.lower() for term in ['url', 'link', 'handle']) and pd.notna(row.get(col)):
                        link_value = str(row[col]).strip()
                        break
            
            # Construct full URL from handle if shop_url is provided
            product['link'] = self._construct_product_url(
                link_value, 
                shop_url, 
                platform=platform,
                custom_url_pattern=custom_url_pattern
            ) if link_value else None

            # Extract price (REQUIRED for frontend)
            price_col = actual_columns.get('price')
            if price_col and pd.notna(row.get(price_col)):
                try:
                    price_value = row[price_col]
                    # Handle string prices like "$38.00" or "38.00"
                    if isinstance(price_value, str):
                        price_value = price_value.replace('$', '').replace(',', '').strip()
                    if price_value:
                        product['price'] = float(price_value)
                    else:
                        product['price'] = None
                except (ValueError, TypeError):
                    product['price'] = None
            else:
                # Try to find any price column
                for col in df.columns:
                    if 'price' in col.lower() and pd.notna(row.get(col)):
                        try:
                            price_value = row[col]
                            if isinstance(price_value, str):
                                price_value = price_value.replace('$', '').replace(',', '').strip()
                            if price_value:
                                product['price'] = float(price_value)
                                break
                        except (ValueError, TypeError):
                            continue
                else:
                    product['price'] = None

            # Extract compare_at_price (optional) - only include if exists
            compare_price_col = actual_columns.get('compare_at_price')
            if compare_price_col and pd.notna(row.get(compare_price_col)):
                try:
                    compare_price_value = row[compare_price_col]
                    # Handle string prices like "$38.00" or "38.00"
                    if isinstance(compare_price_value, str):
                        compare_price_value = compare_price_value.replace('$', '').replace(',', '').strip()
                    if compare_price_value:  # Only add if not empty
                        product['compare_at_price'] = float(compare_price_value)
                except (ValueError, TypeError):
                    pass  # Don't add compare_at_price if conversion fails

            # Only add product if it has required fields (name, image_url, link, price)
            # Description is not required - can be fetched from Vertex AI Search
            if (product.get('name') and 
                product.get('image_url') and 
                product.get('link') and 
                product.get('price') is not None):
                curated.append(product)
            else:
                logger.warning(f"Skipping product '{product.get('name')}' - missing required fields (name, image_url, link, or price)")

        logger.info(f"Created {len(curated)} curated products")
        return curated

    def _create_full_products(
        self, 
        df: pd.DataFrame, 
        shop_url: Optional[str] = None,
        platform: Optional[str] = None,
        custom_url_pattern: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Create full product schema for Vertex AI Search
        Extracts ALL fields from the dataframe to structData

        Args:
            df: Product dataframe
            shop_url: Shop URL to construct full product URLs from handles
            platform: E-commerce platform ('shopify', 'woocommerce', 'wordpress', 'custom')
            custom_url_pattern: Custom URL pattern for 'custom' platform

        Returns:
            List of full product dictionaries with all fields
        """
        full_products = []

        # Find ID column (for document ID)
        id_columns = ['id', 'sku', 'product_id', 'variant_id']
        id_col = None
        for col in id_columns:
            if col in df.columns:
                id_col = col
                break

        # Find title/name column
        title_columns = ['title', 'name', 'product_name', 'product_title']
        title_col = None
        for col in title_columns:
            if col in df.columns:
                title_col = col
                break

        # Find handle column (fallback for title)
        handle_columns = ['handle', 'product_handle', 'slug', 'product_slug']
        handle_col = None
        for col in handle_columns:
            if col in df.columns:
                handle_col = col
                break

        # Find description column
        desc_columns = ['description', 'body_html', 'body', 'product_description']
        desc_col = None
        for col in desc_columns:
            if col in df.columns:
                desc_col = col
                break

        for idx, row in df.iterrows():
            # Create product ID - sanitize to match pattern [a-zA-Z0-9-_]*
            if id_col and pd.notna(row.get(id_col)):
                original_id = str(row[id_col])
            else:
                original_id = f"product-{idx}"
            
            # Sanitize: replace any character not in [a-zA-Z0-9-_] with hyphen
            product_id = re.sub(r'[^a-zA-Z0-9-_]', '-', original_id)
            # Replace multiple consecutive hyphens with single hyphen
            product_id = re.sub(r'-+', '-', product_id)
            # Remove leading/trailing hyphens
            product_id = product_id.strip('-')
            # Ensure ID is not empty
            if not product_id:
                product_id = f"product-{idx}"

            # Create title - try title/name first, then fallback to handle
            if title_col and pd.notna(row.get(title_col)):
                title = str(row[title_col])
            elif handle_col and pd.notna(row.get(handle_col)):
                # Use handle as fallback if title/name is null - format it nicely
                handle_value = str(row[handle_col]).strip()
                title = self._format_handle_as_name(handle_value)
            else:
                title = 'Untitled Product'

            # Create content (description) - will be base64 encoded
            if desc_col and pd.notna(row.get(desc_col)):
                content_text = str(row[desc_col])
            else:
                content_text = title or ''

            # Build struct_data from all columns (this is where ALL metadata goes)
            # Title should be in struct_data, not at top level
            struct_data = {}
            for col in df.columns:
                value = row.get(col)
                if pd.notna(value):
                    # Convert to appropriate type
                    if isinstance(value, (int, float)):
                        struct_data[col] = value
                    elif isinstance(value, bool):
                        struct_data[col] = value
                    elif isinstance(value, str):
                        # Preserve string values as-is
                        struct_data[col] = value.strip()
                    else:
                        # Convert other types to string
                        struct_data[col] = str(value)
            
            # Construct full product URL for link/handle/url columns if shop_url is provided
            link_columns = ['link', 'url', 'handle', 'product_url', 'product_link', 'product_handle']
            for link_col in link_columns:
                if link_col in struct_data and shop_url:
                    link_value = struct_data[link_col]
                    full_url = self._construct_product_url(
                        link_value, 
                        shop_url,
                        platform=platform,
                        custom_url_pattern=custom_url_pattern
                    )
                    if full_url:
                        struct_data[link_col] = full_url
                        # Also add a 'product_url' field with the full URL for consistency
                        if link_col != 'product_url':
                            struct_data['product_url'] = full_url
            
            # Add title to struct_data (Vertex AI Search format)
            struct_data["title"] = title or f"Product {idx}"

            # Encode content as base64 (matching working script format)
            content_bytes = content_text.encode('utf-8')
            content_base64 = base64.b64encode(content_bytes).decode('utf-8')

            # Create Vertex AI Search document format (matching working script)
            product = {
                "id": product_id,
                "content": {
                    "mime_type": "text/plain",
                    "raw_bytes": content_base64
                },
                "struct_data": struct_data
            }

            full_products.append(product)

        logger.info(f"Created {len(full_products)} full products with all fields")
        return full_products

    def process_categories_file(
        self,
        merchant_id: str,
        categories_file_path: str
    ) -> Dict[str, str]:
        """
        Process categories file and create NDJSON for Vertex AI Search

        Args:
            merchant_id: Merchant identifier
            categories_file_path: GCS path to categories file

        Returns:
            dict with path to generated NDJSON file
        """
        try:
            # Download categories file from GCS
            logger.info(f"Downloading categories file: {categories_file_path}")
            file_content = self.gcs_handler.download_file(categories_file_path)

            # Determine file type and read
            if categories_file_path.endswith('.xlsx') or categories_file_path.endswith('.xls'):
                df = pd.read_excel(BytesIO(file_content))
            elif categories_file_path.endswith('.csv'):
                df = pd.read_csv(BytesIO(file_content))
            else:
                raise ValueError(f"Unsupported file type: {categories_file_path}")

            logger.info(f"Loaded {len(df)} categories from file")

            # Convert categories to NDJSON format for Vertex AI Search
            categories_ndjson = self._create_categories_ndjson(df, merchant_id)

            # Upload categories.ndjson
            categories_ndjson_path = f"merchants/{merchant_id}/training_files/categories.ndjson"
            self.gcs_handler.upload_file(
                categories_ndjson_path,
                categories_ndjson.encode('utf-8'),
                content_type="application/x-ndjson"
            )
            logger.info(f"Uploaded categories.ndjson: {categories_ndjson_path}")

            return {
                "categories_ndjson": categories_ndjson_path,
                "category_count": len(df)
            }

        except Exception as e:
            logger.error(f"Error processing categories file: {e}")
            raise

    def _create_categories_ndjson(self, df: pd.DataFrame, merchant_id: str) -> str:
        """
        Convert categories dataframe to NDJSON format for Vertex AI Search

        Args:
            df: Categories dataframe
            merchant_id: Merchant identifier

        Returns:
            NDJSON string
        """
        categories = []

        # Find ID column
        id_columns = ['id', 'category_id', 'categoryId', 'slug', 'handle']
        id_col = None
        for col in id_columns:
            if col in df.columns:
                id_col = col
                break

        # Find name/title column
        name_columns = ['name', 'title', 'category_name', 'categoryName', 'label']
        name_col = None
        for col in name_columns:
            if col in df.columns:
                name_col = col
                break

        # Find description column
        desc_columns = ['description', 'desc', 'category_description', 'body']
        desc_col = None
        for col in desc_columns:
            if col in df.columns:
                desc_col = col
                break

        for idx, row in df.iterrows():
            # Create category ID - sanitize to match pattern [a-zA-Z0-9-_]*
            if id_col and pd.notna(row.get(id_col)):
                original_id = f"category-{merchant_id}-{str(row[id_col])}"
            else:
                original_id = f"category-{merchant_id}-{idx}"
            
            # Sanitize: replace any character not in [a-zA-Z0-9-_] with hyphen
            category_id = re.sub(r'[^a-zA-Z0-9-_]', '-', original_id)
            # Replace multiple consecutive hyphens with single hyphen
            category_id = re.sub(r'-+', '-', category_id)
            # Remove leading/trailing hyphens
            category_id = category_id.strip('-')
            # Ensure ID is not empty
            if not category_id:
                category_id = f"category-{merchant_id}-{idx}"

            # Create title
            if name_col and pd.notna(row.get(name_col)):
                title = str(row[name_col])
            else:
                title = 'Untitled Category'

            # Create content (description) - will be base64 encoded
            if desc_col and pd.notna(row.get(desc_col)):
                content_text = str(row[desc_col])
            else:
                content_text = title or ''

            # Build struct_data from all columns (this is where ALL metadata goes)
            # Title should be in struct_data, not at top level
            struct_data = {
                "type": "category",
                "merchant_id": merchant_id
            }
            
            for col in df.columns:
                value = row.get(col)
                if pd.notna(value):
                    # Convert to appropriate type
                    if isinstance(value, (int, float)):
                        struct_data[col] = value
                    elif isinstance(value, bool):
                        struct_data[col] = value
                    elif isinstance(value, str):
                        # Preserve string values as-is
                        struct_data[col] = value.strip()
                    else:
                        # Convert other types to string
                        struct_data[col] = str(value)
            
            # Add title to struct_data (Vertex AI Search format)
            struct_data["title"] = title or f"Category {idx}"

            # Encode content as base64 (matching working script format)
            content_bytes = content_text.encode('utf-8')
            content_base64 = base64.b64encode(content_bytes).decode('utf-8')

            # Create Vertex AI Search document format (matching working script)
            category = {
                "id": category_id,
                "content": {
                    "mime_type": "text/plain",
                    "raw_bytes": content_base64
                },
                "struct_data": struct_data
            }

            categories.append(category)

        logger.info(f"Created {len(categories)} categories for Vertex AI Search")
        
        # Convert to NDJSON
        lines = []
        for category in categories:
            lines.append(json.dumps(category, ensure_ascii=False))
        return '\n'.join(lines)

    def _process_json_products(
        self, 
        products: List[Dict[str, Any]], 
        shop_url: Optional[str] = None,
        platform: Optional[str] = None,
        custom_url_pattern: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Process and validate products from JSON file, constructing full URLs
        
        Args:
            products: List of product dictionaries from JSON
            shop_url: Shop URL to construct full product URLs from handles
            platform: E-commerce platform ('shopify', 'woocommerce', 'wordpress', 'custom')
            custom_url_pattern: Custom URL pattern for 'custom' platform
            
        Returns:
            List of validated and processed product dictionaries with full URLs
        """
        validated = []
        
        for idx, product in enumerate(products):
            if not isinstance(product, dict):
                logger.warning(f"Skipping product at index {idx} - not a dictionary")
                continue
            
            # Validate required fields
            cleaned_product = {}
            
            # Name (required)
            if 'name' in product and product['name']:
                cleaned_product['name'] = str(product['name']).strip()
            else:
                logger.warning(f"Skipping product at index {idx} - missing 'name' field")
                continue
            
            # Image URL (required)
            if 'image_url' in product and product['image_url']:
                cleaned_product['image_url'] = str(product['image_url']).strip()
            else:
                logger.warning(f"Skipping product '{cleaned_product['name']}' - missing 'image_url' field")
                continue
            
            # Link (required) - construct full URL
            if 'link' in product and product['link']:
                link_value = str(product['link']).strip()
                cleaned_product['link'] = self._construct_product_url(
                    link_value, 
                    shop_url,
                    platform=platform,
                    custom_url_pattern=custom_url_pattern
                )
            else:
                logger.warning(f"Skipping product '{cleaned_product['name']}' - missing 'link' field")
                continue
            
            # Price (required)
            if 'price' in product and product['price'] is not None:
                try:
                    price_value = product['price']
                    if isinstance(price_value, str):
                        price_value = price_value.replace('$', '').replace(',', '').strip()
                    cleaned_product['price'] = float(price_value)
                except (ValueError, TypeError):
                    logger.warning(f"Skipping product '{cleaned_product['name']}' - invalid 'price' value")
                    continue
            else:
                logger.warning(f"Skipping product '{cleaned_product['name']}' - missing 'price' field")
                continue
            
            # Compare at price (optional)
            if 'compare_at_price' in product and product['compare_at_price'] is not None:
                try:
                    compare_price_value = product['compare_at_price']
                    if isinstance(compare_price_value, str):
                        compare_price_value = compare_price_value.replace('$', '').replace(',', '').strip()
                    if compare_price_value:  # Only add if not empty
                        cleaned_product['compare_at_price'] = float(compare_price_value)
                except (ValueError, TypeError):
                    pass  # Don't add compare_at_price if conversion fails
            
            validated.append(cleaned_product)
        
        logger.info(f"Processed {len(validated)} products from JSON (skipped {len(products) - len(validated)})")
        return validated
    
    def _create_full_products_from_json(
        self, 
        curated_products: List[Dict[str, Any]], 
        shop_url: Optional[str] = None,
        platform: Optional[str] = None,
        custom_url_pattern: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Create full product schema for Vertex AI Search from curated JSON products
        
        Args:
            curated_products: List of curated product dictionaries (already with full URLs)
            shop_url: Shop URL (for reference, URLs already constructed)
            platform: E-commerce platform (for reference, URLs already constructed)
            custom_url_pattern: Custom URL pattern (for reference, URLs already constructed)
            
        Returns:
            List of full product dictionaries for Vertex AI Search
        """
        full_products = []
        
        for idx, product in enumerate(curated_products):
            # Create product ID from link (extract handle from URL if needed)
            link_value = product.get('link', product.get('name', f"product-{idx}"))
            # Extract handle from URL if it's a full URL
            if '/' in link_value:
                # Extract handle from URL like https://shop.com/products/handle
                parts = link_value.split('/products/')
                if len(parts) > 1:
                    handle = parts[-1].split('?')[0].split('#')[0]  # Remove query params and fragments
                else:
                    handle = link_value.split('/')[-1]
            else:
                handle = link_value
            
            # Sanitize handle to create product ID
            product_id = re.sub(r'[^a-zA-Z0-9-_]', '-', str(handle))
            product_id = re.sub(r'-+', '-', product_id)
            product_id = product_id.strip('-')
            if not product_id:
                product_id = f"product-{idx}"
            
            # Create title
            title = product.get('name', 'Untitled Product')
            
            # Create content (use name as description if no description available)
            content_text = title
            
            # Build struct_data from all product fields
            struct_data = {
                "title": title,
                "name": product.get('name'),
                "image_url": product.get('image_url'),
                "link": product.get('link'),  # Already full URL
                "product_url": product.get('link'),  # Also add as product_url
                "price": product.get('price')
            }
            
            # Add compare_at_price if exists
            if 'compare_at_price' in product:
                struct_data["compare_at_price"] = product['compare_at_price']
            
            # Encode content as base64
            content_bytes = content_text.encode('utf-8')
            content_base64 = base64.b64encode(content_bytes).decode('utf-8')
            
            # Create Vertex AI Search document format
            full_product = {
                "id": product_id,
                "content": {
                    "mime_type": "text/plain",
                    "raw_bytes": content_base64
                },
                "struct_data": struct_data
            }
            
            full_products.append(full_product)
        
        logger.info(f"Created {len(full_products)} full products from JSON for Vertex AI Search")
        return full_products

    def _create_ndjson(self, products: List[Dict[str, Any]]) -> str:
        """
        Convert products list to NDJSON format

        Args:
            products: List of product dictionaries

        Returns:
            NDJSON string
        """
        lines = []
        for product in products:
            lines.append(json.dumps(product, ensure_ascii=False))
        return '\n'.join(lines)

