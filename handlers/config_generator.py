"""Configuration generator for merchant setup"""

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class ConfigGenerator:
    """Generate merchant configuration JSON"""

    def __init__(self, gcs_handler):
        """
        Initialize config generator

        Args:
            gcs_handler: GCSHandler instance
        """
        self.gcs_handler = gcs_handler
        self.project_id = os.getenv("GCP_PROJECT_ID", "shopify-473015")
        self.location = os.getenv("GCP_LOCATION", "global")

    def generate_config(
        self,
        user_id: str,
        merchant_id: str,
        shop_name: str,
        shop_url: str,
        bot_name: Optional[str] = "AI Assistant",
        target_customer: Optional[str] = None,
        customer_persona: Optional[str] = None,
        bot_tone: Optional[str] = None,
        prompt_text: Optional[str] = None,
        top_questions: Optional[str] = None,
        top_products: Optional[str] = None,
        primary_color: Optional[str] = "#667eea",
        secondary_color: Optional[str] = "#764ba2",
        logo_url: Optional[str] = None,
        platform: Optional[str] = None,
        custom_url_pattern: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate merchant configuration JSON

        Args:
            user_id: User identifier
            merchant_id: Merchant identifier
            shop_name: Shop name
            shop_url: Shop URL
            bot_name: Bot name (default: AI Assistant)
            target_customer: Target customer description
            customer_persona: Detailed customer persona description
            bot_tone: Bot tone and personality
            prompt_text: Custom prompt text/guidelines
            top_questions: Top questions
            top_products: Top products
            primary_color: Primary color
            secondary_color: Secondary color
            logo_url: Logo URL
            platform: E-commerce platform (shopify, woocommerce, wordpress, squarespace, custom)
            custom_url_pattern: Custom product URL path for 'custom' platform (e.g. /boutique/p/)

        Returns:
            dict with config path and content
        """
        try:
            # Get current timestamp in ISO format
            now = datetime.now(timezone.utc).isoformat()
            
            # Try to read existing config to preserve custom_chatbot settings
            existing_config = {}
            config_path = f"merchants/{merchant_id}/merchant_config.json"
            try:
                if self.gcs_handler.file_exists(config_path):
                    file_content = self.gcs_handler.download_file(config_path)
                    existing_config = json.loads(file_content.decode('utf-8'))
                    logger.info(f"Loaded existing config to preserve custom_chatbot settings")
            except Exception as e:
                logger.debug(f"Could not read existing config (will create new): {e}")
            
            # Preserve existing custom_chatbot settings if they exist
            existing_custom_chatbot = existing_config.get("custom_chatbot", {})
            
            # Construct logo URL if provided (convert GCS path to full URL if needed)
            full_logo_url = logo_url
            if logo_url and not logo_url.startswith(('http://', 'https://')):
                # If it's a GCS path, convert to storage URL
                if logo_url.startswith('gs://'):
                    # Extract bucket and path from gs:// URL
                    parts = logo_url.replace('gs://', '').split('/', 1)
                    if len(parts) == 2:
                        bucket, path = parts
                        full_logo_url = f"https://storage.cloud.google.com/{bucket}/{path}"
                else:
                    # Assume it's a GCS path relative to bucket
                    full_logo_url = f"https://storage.cloud.google.com/{self.gcs_handler.bucket_name}/{logo_url}"
            
            # Use existing logo from custom_chatbot if no new logo provided
            if not full_logo_url and existing_custom_chatbot.get("logo_signed_url"):
                full_logo_url = existing_custom_chatbot.get("logo_signed_url")
            
            # Preserve existing platform/product_url_path if not provided (e.g. from update_config)
            existing_platform = existing_config.get("platform")
            existing_custom_url = existing_config.get("custom_url_pattern") or existing_config.get("product_url_path")
            platform_val = (platform or existing_platform or "").strip().lower() or None
            custom_url_val = (custom_url_pattern or existing_custom_url or "").strip() or None

            # Build the complete config structure
            config = {
                "user_id": user_id,
                "merchant_id": merchant_id,
                "shop_name": shop_name,
                "shop_url": shop_url,
                "bot_name": bot_name,
                "products": {
                    "bucket_name": self.gcs_handler.bucket_name,
                    "file_path": f"merchants/{merchant_id}/prompt-docs/products.json"
                },
                "bigquery": {
                    "project_id": self.project_id,
                    "dataset_id": "chatbot_logs",
                    "table_id": "conversations"
                },
                "vertex_search": {
                    "project_id": self.project_id,
                    "location": self.location,
                    "website_id": f"{merchant_id}-website-engine"
                },
                "branding": {
                    "primary_color": primary_color or "#667eea",
                    "secondary_color": secondary_color or "#764ba2",
                    "logo_url": full_logo_url or ""
                },
                "custom_chatbot": {
                    # Preserve existing custom_chatbot settings if they exist, otherwise use defaults
                    "title": existing_custom_chatbot.get("title", bot_name or "AI Assistant"),
                    "logo_signed_url": existing_custom_chatbot.get("logo_signed_url", full_logo_url or ""),
                    "color": existing_custom_chatbot.get("color", primary_color or "#667eea"),
                    "font_family": existing_custom_chatbot.get("font_family", "Inter, sans-serif"),
                    "tag_line": existing_custom_chatbot.get("tag_line", ""),
                    "position": existing_custom_chatbot.get("position", "bottom-right")
                },
                "metadata": {
                    # Preserve created_at from existing config if it exists
                    "created_at": existing_config.get("metadata", {}).get("created_at", now),
                    "updated_at": now,
                    "version": existing_config.get("metadata", {}).get("version", "1.0")
                }
            }

            # Add optional fields (only if provided). platform/custom_url_pattern are synced from DB
            # whenever config is generated (onboarding, save_ai_persona, PATCH merchant).
            if target_customer:
                config["target_customer"] = target_customer
            if customer_persona:
                config["customer_persona"] = customer_persona
            if bot_tone:
                config["bot_tone"] = bot_tone
            if prompt_text:
                config["prompt_text"] = prompt_text
            if top_questions:
                config["top_questions"] = top_questions
            if top_products:
                config["top_products"] = top_products
            if platform_val:
                config["platform"] = platform_val
            if custom_url_val:
                config["custom_url_pattern"] = custom_url_val
                # Langflow expects product_url_path as prefix (e.g. /boutique/p/); derive from pattern like /boutique/p/{handle}
                path_prefix = custom_url_val.replace("{handle}", "").replace("{}", "").rstrip("/") + "/"
                config["product_url_path"] = path_prefix

            # Upload config to GCS - Langflow expects merchant_config.json
            config_path = f"merchants/{merchant_id}/merchant_config.json"
            config_content = json.dumps(config, indent=4, ensure_ascii=False)
            self.gcs_handler.upload_file(
                config_path,
                config_content.encode('utf-8'),
                content_type="application/json"
            )

            logger.info(f"Generated and uploaded config: {config_path}")

            return {
                "config_path": config_path,
                "config": config
            }

        except Exception as e:
            logger.error(f"Error generating config: {e}")
            raise

    def update_config(
        self,
        merchant_id: str,
        new_fields: Dict[str, Any],
        preserve_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Update merchant_config.json by adding/updating fields while preserving existing ones
        
        ⚠️ IMPORTANT: This method ONLY updates the config file.
        It does NOT trigger onboarding, product processing, or any other operations.
        
        Args:
            merchant_id: Merchant identifier
            new_fields: Dictionary of new fields to add/update (can be nested)
            preserve_existing: If True, preserves all existing fields. If False, only updates specified fields.
        
        Returns:
            dict with config path and updated content
        
        Example:
            update_config(
                merchant_id="merchant-1",
                new_fields={
                    "custom_field": "value",
                    "custom_chatbot": {
                        "title": "Help Assistant",
                        "color": "#ff0000"
                    }
                }
            )
        """
        try:
            config_path = f"merchants/{merchant_id}/merchant_config.json"
            
            # Try to read existing config
            existing_config = {}
            try:
                if self.gcs_handler.file_exists(config_path):
                    file_content = self.gcs_handler.download_file(config_path)
                    existing_config = json.loads(file_content.decode('utf-8'))
                    logger.info(f"Loaded existing config from {config_path}")
                else:
                    logger.warning(f"Config file not found at {config_path}, creating new config")
            except Exception as e:
                logger.warning(f"Could not read existing config: {e}, creating new config")
            
            # Merge new fields with existing config
            if preserve_existing:
                # Deep merge: preserve existing fields, add/update new ones
                updated_config = self._deep_merge(existing_config.copy(), new_fields)
            else:
                # Shallow merge: only update specified fields, remove others
                updated_config = existing_config.copy()
                updated_config.update(new_fields)
            
            # Update metadata
            now = datetime.now(timezone.utc).isoformat()
            if "metadata" not in updated_config:
                updated_config["metadata"] = {}
            
            # Preserve created_at if it exists, update updated_at
            if "metadata" in existing_config and "created_at" in existing_config["metadata"]:
                updated_config["metadata"]["created_at"] = existing_config["metadata"]["created_at"]
            else:
                updated_config["metadata"]["created_at"] = now
            
            updated_config["metadata"]["updated_at"] = now
            updated_config["metadata"]["version"] = existing_config.get("metadata", {}).get("version", "1.0")
            
            # Upload updated config
            config_content = json.dumps(updated_config, indent=4, ensure_ascii=False)
            self.gcs_handler.upload_file(
                config_path,
                config_content.encode('utf-8'),
                content_type="application/json"
            )
            
            logger.info(f"Updated config at {config_path} with new fields: {list(new_fields.keys())}")
            
            return {
                "config_path": config_path,
                "config": updated_config,
                "added_fields": list(new_fields.keys()),
                "preserved_existing": preserve_existing
            }
            
        except Exception as e:
            logger.error(f"Error updating config: {e}")
            raise

    def _deep_merge(self, base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries, preserving nested structures
        
        Args:
            base: Base dictionary (existing config)
            update: Dictionary with updates (new fields)
        
        Returns:
            Merged dictionary
        """
        result = base.copy()
        
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Recursively merge nested dictionaries
                result[key] = self._deep_merge(result[key], value)
            else:
                # Update or add the field
                result[key] = value
        
        return result

