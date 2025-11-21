"""Vertex AI Search setup handler"""

import os
import re
import logging
from typing import Dict, Any, Optional
from google.cloud import discoveryengine_v1 as vertex
from google.api_core import exceptions as gcp_exceptions
from google.oauth2 import service_account
from google.protobuf import field_mask_pb2

logger = logging.getLogger(__name__)


class VertexSetup:
    """Handle Vertex AI Search datastore setup and document import"""

    def __init__(
        self,
        project_id: str = None,
        location: str = None,
        collection_id: str = None,
        gcs_bucket: str = None
    ):
        """
        Initialize Vertex AI Search handler

        Args:
            project_id: GCP project ID
            location: GCP location (default: global)
            collection_id: Vertex AI Search collection ID
            gcs_bucket: GCS bucket name
        """
        # Use Vertex-specific environment variables, fallback to GCP variables
        # NOTE: Vertex AI Search requires "global" location for the API endpoint
        self.project_id = project_id or os.getenv("VERTEX_PROJECT_ID") or os.getenv("GCP_PROJECT_ID", "shopify-473015")
        # Force global location - Vertex AI Search API endpoint only serves "global" region
        self.location = location or os.getenv("VERTEX_LOCATION") or os.getenv("GCP_LOCATION", "global")
        if self.location != "global":
            logger.warning(f"Vertex AI Search requires 'global' location. Changing from '{self.location}' to 'global'")
            self.location = "global"
        self.collection_id = collection_id or os.getenv("VERTEX_COLLECTION", "default_collection")
        self.gcs_bucket = gcs_bucket or os.getenv("GCS_BUCKET_NAME", "chekout-ai")

        # Get credentials (same method as GCSHandler)
        credentials = self._get_credentials()

        try:
            if credentials:
                # Log which service account is being used
                # Try multiple ways to get the service account email
                service_account_email = (
                    getattr(credentials, 'service_account_email', None) or
                    getattr(credentials, '_service_account_email', None) or
                    (credentials._key.get('client_email') if hasattr(credentials, '_key') and isinstance(credentials._key, dict) else None) or
                    'Unknown'
                )
                logger.info(f"Using service account for Vertex AI: {service_account_email}")
                
                # Store credentials for later access
                self._credentials = credentials
                self._service_account_email = service_account_email
                
                # Use vertex module for all clients to ensure version consistency
                self.client = vertex.DocumentServiceClient(credentials=credentials)
                self.datastore_client = vertex.DataStoreServiceClient(credentials=credentials)
                self.site_search_client = vertex.SiteSearchEngineServiceClient(credentials=credentials)
                logger.info(f"Initialized Vertex AI Search client with service account credentials for project: {self.project_id}")
                
                # Verify we're using the Vertex service account if VERTEX_CLIENT_EMAIL is set
                expected_vertex_email = os.getenv("VERTEX_CLIENT_EMAIL")
                if expected_vertex_email:
                    expected_vertex_email = expected_vertex_email.strip().strip('"').strip("'")
                    if service_account_email != expected_vertex_email and service_account_email != 'Unknown':
                        logger.warning(f"WARNING: Expected Vertex service account '{expected_vertex_email}' but using '{service_account_email}'")
                    elif service_account_email == expected_vertex_email:
                        logger.info(f"✓ Confirmed using correct Vertex service account: {service_account_email}")
            else:
                # Fallback to default credentials (for local dev with gcloud auth)
                # Use vertex module for all clients to ensure version consistency
                self.client = vertex.DocumentServiceClient()
                self.datastore_client = vertex.DataStoreServiceClient()
                self.site_search_client = vertex.SiteSearchEngineServiceClient()
                logger.warning("Using default credentials (application-default). For production, use service account credentials.")
                logger.info(f"Initialized Vertex AI Search client for project: {self.project_id}")
        except Exception as e:
            logger.error(f"Failed to initialize Vertex AI Search client: {e}")
            raise

    def _get_credentials(self):
        """Get credentials from Vertex-specific or GCS environment variables or service account file"""
        # First, check for Vertex-specific credentials path
        vertex_creds_path = os.getenv("VERTEX_CREDENTIALS_PATH")
        if vertex_creds_path and os.path.exists(vertex_creds_path):
            logger.info(f"Using Vertex AI credentials from file: {vertex_creds_path}")
            return service_account.Credentials.from_service_account_file(vertex_creds_path)
        
        # Second, check if GOOGLE_APPLICATION_CREDENTIALS is set (service account JSON file)
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path and os.path.exists(creds_path):
            logger.info(f"Using credentials from GOOGLE_APPLICATION_CREDENTIALS: {creds_path}")
            return service_account.Credentials.from_service_account_file(creds_path)
        
        # Third, try Vertex-specific environment variables
        vertex_client_email = os.getenv("VERTEX_CLIENT_EMAIL")
        vertex_private_key = os.getenv("VERTEX_PRIVATE_KEY")
        vertex_private_key_id = os.getenv("VERTEX_PRIVATE_KEY_ID")
        vertex_project_id = os.getenv("VERTEX_PROJECT_ID") or self.project_id
        
        # Debug logging
        if vertex_client_email:
            logger.info(f"Found VERTEX_CLIENT_EMAIL: {vertex_client_email}")
        else:
            logger.warning("VERTEX_CLIENT_EMAIL not found in environment")
        
        if vertex_private_key:
            logger.info(f"Found VERTEX_PRIVATE_KEY (length: {len(vertex_private_key)})")
        else:
            logger.warning("VERTEX_PRIVATE_KEY not found in environment")
        
        if vertex_client_email and vertex_private_key:
            # Clean up the private key (remove quotes and newline escapes)
            private_key = vertex_private_key.strip('"').replace('\\n', '\n')
            
            service_account_info = {
                "type": "service_account",
                "project_id": vertex_project_id,
                "private_key_id": vertex_private_key_id or "",
                "private_key": private_key,
                "client_email": vertex_client_email,
                "client_id": os.getenv("VERTEX_CLIENT_ID", ""),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            }
            
            try:
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info
                )
                logger.info(f"Using Vertex AI credentials from environment variables for: {vertex_client_email}")
                return credentials
            except Exception as e:
                logger.warning(f"Failed to create Vertex credentials from env vars: {e}")
        
        # Fourth, fallback to GCS environment variables (for backward compatibility)
        gcs_client_email = os.getenv("GCS_CLIENT_EMAIL")
        gcs_private_key = os.getenv("GCS_PRIVATE_KEY")
        gcs_private_key_id = os.getenv("GCS_PRIVATE_KEY_ID")
        gcs_project_id = os.getenv("GCS_PROJECT_ID") or self.project_id
        
        if gcs_client_email and gcs_private_key:
            # Clean up the private key (remove quotes and newline escapes)
            private_key = gcs_private_key.strip('"').replace('\\n', '\n')
            
            service_account_info = {
                "type": "service_account",
                "project_id": gcs_project_id,
                "private_key_id": gcs_private_key_id or "",
                "private_key": private_key,
                "client_email": gcs_client_email,
                "client_id": os.getenv("GCS_CLIENT_ID", ""),
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            }
            
            try:
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info
                )
                logger.info(f"Using GCS credentials for Vertex AI (fallback) for: {gcs_client_email}")
                return credentials
            except Exception as e:
                logger.warning(f"Failed to create credentials from GCS env vars: {e}")
                return None
        
        return None

    def create_datastore(
        self,
        merchant_id: str,
        shop_url: Optional[str] = None,
        shop_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create or get Vertex AI Search datastore
        If shop_url is provided, configures the datastore for website crawling

        Args:
            merchant_id: Merchant identifier
            shop_url: Optional website URL for website data store
            shop_name: Optional shop name for display

        Returns:
            dict with datastore information
        """
        try:
            datastore_id = f"{merchant_id}-engine"
            parent = f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}"

            # Check if datastore already exists
            datastore_path = f"{parent}/dataStores/{datastore_id}"

            try:
                # Try to get existing datastore
                datastore = self.datastore_client.get_data_store(name=datastore_path)
                logger.info(f"Datastore already exists: {datastore_id}")
                return {
                    "datastore_id": datastore_id,
                    "datastore_path": datastore_path,
                    "status": "exists",
                    "name": datastore.display_name,
                    "content_config": datastore.content_config.name if datastore.content_config else None
                }
            except Exception:
                # Datastore doesn't exist, create it
                logger.info(f"Creating new datastore: {datastore_id}")

                # Create datastore with website configuration if URL provided
                display_name = shop_name or f"{merchant_id} Store"
                
                # Create datastore object - use PUBLIC_WEBSITE for website crawling
                # This matches the working script approach
                if shop_url:
                    # Website data store - enables both public web crawling + doc ingestion
                    content_config = vertex.DataStore.ContentConfig.PUBLIC_WEBSITE
                    logger.info(f"Creating website data store for: {shop_url}")
                else:
                    # Generic data store for document imports only
                    content_config = vertex.DataStore.ContentConfig.NO_CONTENT

                datastore = vertex.DataStore(
                    display_name=display_name,
                    content_config=content_config,
                    solution_types=[vertex.SolutionType.SOLUTION_TYPE_SEARCH],
                    industry_vertical=vertex.IndustryVertical.GENERIC,
                )

                try:
                    # Create the datastore - explicitly create request object to avoid version issues
                    logger.info(f"Creating DataStore: {datastore_id}")
                    
                    # Create request object explicitly (matching working script approach)
                    request = vertex.CreateDataStoreRequest(
                        parent=parent,
                        data_store=datastore,
                        data_store_id=datastore_id
                    )
                    
                    operation = self.datastore_client.create_data_store(request=request)
                    
                    # Wait for operation to complete
                    result = operation.result(timeout=600)  # 10 minute timeout (matching working script)
                    logger.info(f"✅ Created DataStore: {result.name}")

                    # If website URL provided, register site for crawling
                    if shop_url:
                        self._register_site_for_crawl(datastore_path, shop_url)

                    return {
                        "datastore_id": datastore_id,
                        "datastore_path": datastore_path,
                        "status": "created",
                        "content_config": content_config.name,
                        "shop_url": shop_url if shop_url else None
                    }
                except (gcp_exceptions.AlreadyExists, gcp_exceptions.Conflict) as e:
                    logger.info(f"ℹ️ DataStore already exists: {datastore_path}")
                    # If it already exists, still try to register the site
                    if shop_url:
                        self._register_site_for_crawl(datastore_path, shop_url)
                    return {
                        "datastore_id": datastore_id,
                        "datastore_path": datastore_path,
                        "status": "exists",
                        "content_config": content_config.name,
                        "shop_url": shop_url if shop_url else None
                    }
                except gcp_exceptions.BadRequest as e:
                    error_str = str(e).lower()
                    if 'being deleted' in error_str:
                        logger.warning(f"ℹ️ DataStore '{datastore_id}' is currently being deleted. Please wait or use a different ID.")
                    logger.warning(f"Could not create datastore via API: {e}")
                    return {
                        "datastore_id": datastore_id,
                        "datastore_path": datastore_path,
                        "status": "pending_creation",
                        "note": "Datastore creation failed. May need manual setup or Terraform.",
                        "shop_url": shop_url if shop_url else None
                    }
                except Exception as create_error:
                    logger.warning(f"Could not create datastore via API: {create_error}")
                    logger.info("Datastore may need to be created manually or via Terraform")
                    return {
                        "datastore_id": datastore_id,
                        "datastore_path": datastore_path,
                        "status": "pending_creation",
                        "note": "Datastore creation may require manual setup or Terraform",
                        "shop_url": shop_url if shop_url else None
                    }

        except Exception as e:
            logger.error(f"Error creating datastore: {e}")
            raise

    def update_datastore(
        self,
        merchant_id: str,
        shop_name: Optional[str] = None,
        shop_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update existing Vertex AI Search datastore
        
        Args:
            merchant_id: Merchant identifier
            shop_name: New shop name (updates display_name)
            shop_url: New shop URL (re-registers site for crawling)
        
        Returns:
            dict with update status
        """
        try:
            datastore_id = f"{merchant_id}-engine"
            parent = f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}"
            datastore_path = f"{parent}/dataStores/{datastore_id}"
            
            # Check if datastore exists
            try:
                datastore = self.datastore_client.get_data_store(name=datastore_path)
            except Exception as e:
                logger.warning(f"Datastore {datastore_id} not found, cannot update: {e}")
                return {
                    "datastore_id": datastore_id,
                    "status": "not_found",
                    "message": "Datastore does not exist"
                }
            
            updated = False
            updates = []
            
            # Update display_name if shop_name provided
            if shop_name and datastore.display_name != shop_name:
                datastore.display_name = shop_name
                updated = True
                updates.append("display_name")
                logger.info(f"Updating datastore display_name to: {shop_name}")
            
            # Update datastore if display_name changed
            if updated:
                try:
                    update_mask = field_mask_pb2.FieldMask(paths=["display_name"])
                    request = vertex.UpdateDataStoreRequest(
                        data_store=datastore,
                        update_mask=update_mask
                    )
                    updated_datastore = self.datastore_client.update_data_store(request=request)
                    logger.info(f"✅ Updated datastore display_name: {updated_datastore.display_name}")
                except Exception as e:
                    logger.error(f"Failed to update datastore display_name: {e}")
                    return {
                        "datastore_id": datastore_id,
                        "status": "update_failed",
                        "error": str(e)
                    }
            
            # Re-register site if shop_url changed
            if shop_url:
                try:
                    self._register_site_for_crawl(datastore_path, shop_url)
                    updates.append("site_registration")
                except Exception as e:
                    logger.warning(f"Failed to re-register site for crawl: {e}")
                    # Don't fail the whole update if site registration fails
            
            return {
                "datastore_id": datastore_id,
                "status": "updated" if updated or shop_url else "no_changes",
                "updated_fields": updates
            }
            
        except Exception as e:
            logger.error(f"Error updating datastore: {e}")
            return {
                "datastore_id": datastore_id,
                "status": "error",
                "error": str(e)
            }

    def _register_site_for_crawl(self, datastore_path: str, shop_url: str):
        """
        Register the website for Vertex AI Search crawling using SiteSearchEngineServiceClient.
        This matches the working script approach.

        Args:
            datastore_path: Path to the datastore
            shop_url: Website URL to crawl
        """
        try:
            # Parent must include /siteSearchEngine at the end
            parent = f"{datastore_path}/siteSearchEngine"
            
            # Remove protocol from URL (API doesn't accept http:// or https://)
            uri_pattern = shop_url.replace('https://', '').replace('http://', '').rstrip('/')
            
            # Create TargetSite request
            target_site = vertex.TargetSite(
                provided_uri_pattern=uri_pattern,
                type_=vertex.TargetSite.Type.INCLUDE
            )
            
            request = vertex.CreateTargetSiteRequest(
                parent=parent,
                target_site=target_site
            )
            
            logger.info(f"Registering site for crawl: {shop_url}")
            operation = self.site_search_client.create_target_site(request=request)
            result = operation.result(timeout=300)  # 5 minute timeout
            logger.info(f"🌐 Registered site for crawl: {shop_url}")
            
        except (gcp_exceptions.AlreadyExists, gcp_exceptions.Conflict) as e:
            logger.info(f"ℹ️ Site already registered: {shop_url}")
        except gcp_exceptions.NotFound:
            logger.debug(f"DataStore not found - site registration skipped (DataStore must exist first)")
        except Exception as e:
            error_str = str(e).lower()
            if 'not found' in error_str or '404' in error_str:
                logger.debug(f"DataStore not found - site registration skipped (DataStore must exist first)")
            else:
                logger.warning(f"Could not register site for crawl: {e}")
                # Don't raise - this is not critical for the flow

    def import_documents(
        self,
        merchant_id: str,
        gcs_uri: str,
        import_type: str = "FULL"
    ) -> Dict[str, Any]:
        """
        Import documents from GCS to Vertex AI Search

        Args:
            merchant_id: Merchant identifier
            gcs_uri: GCS URI of NDJSON file (gs://bucket/path)
            import_type: Import type (FULL or INCREMENTAL)

        Returns:
            dict with import operation information
        """
        try:
            datastore_id = f"{merchant_id}-engine"
            datastore_path = f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}/dataStores/{datastore_id}"
            
            # Parent for import must include /branches/default_branch
            parent = f"{datastore_path}/branches/default_branch"
            
            # Verify datastore exists before trying to import
            try:
                datastore = self.datastore_client.get_data_store(name=datastore_path)
                logger.info(f"Verified datastore exists: {datastore_id}")
            except Exception as check_error:
                error_msg = str(check_error)
                # Check if it's a permission error or not found
                if "IAM_PERMISSION_DENIED" in error_msg or "Permission" in error_msg:
                    # This is a permission issue - log which service account is being used
                    sa_email = getattr(self, '_service_account_email', 'Unknown')
                    raise Exception(f"Permission denied accessing datastore '{datastore_id}'. Service account: {sa_email}. Ensure the service account has 'discoveryengine.dataStores.get' permission. Error: {error_msg}")
                elif "404" in error_msg or "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                    raise Exception(f"Datastore '{datastore_id}' does not exist. Please create it first or ensure datastore creation succeeded. Error: {error_msg}")
                else:
                    # Re-raise if it's a different error
                    raise

            # Create GCS source - use vertex module for consistency
            gcs_source = vertex.GcsSource(
                input_uris=[gcs_uri] if isinstance(gcs_uri, str) else gcs_uri,
                data_schema="document"
            )

            # Create import request - use vertex module
            request = vertex.ImportDocumentsRequest(
                parent=parent,
                gcs_source=gcs_source,
                reconciliation_mode=vertex.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
                if import_type == "INCREMENTAL"
                else vertex.ImportDocumentsRequest.ReconciliationMode.FULL
            )

            # Start import operation
            logger.info(f"Starting document import from: {gcs_uri}")
            operation = self.client.import_documents(request=request)
            
            # Get operation name safely
            # Operation object has 'operation' attribute, not 'name' directly
            operation_name = None
            try:
                # Try to get name from operation.operation if it exists
                if hasattr(operation, 'operation'):
                    op_obj = operation.operation
                    # Check if it's a dict-like object
                    if isinstance(op_obj, dict):
                        operation_name = op_obj.get('name') if 'name' in op_obj else None
                    # Check if it's an object with name attribute
                    elif hasattr(op_obj, 'name'):
                        operation_name = op_obj.name
                # Also try metadata
                if not operation_name and hasattr(operation, 'metadata'):
                    metadata = operation.metadata
                    if isinstance(metadata, dict) and 'name' in metadata:
                        operation_name = metadata['name']
                    elif hasattr(metadata, 'name'):
                        operation_name = metadata.name
            except Exception as name_error:
                # Silently fail - operation name is optional
                logger.debug(f"Could not extract operation name: {name_error}")
            
            if operation_name:
                logger.info(f"Started document import operation: {operation_name}")
            else:
                logger.info("Started document import operation (name not available)")

            # Wait for operation to complete (matching working script timeout)
            try:
                result = operation.result(timeout=1800)  # 30 minute timeout (matching working script)
                logger.info(f"✅ Document import operation completed")
                
                # Check for errors in result
                if hasattr(result, 'error_samples') and result.error_samples:
                    error_count = len(result.error_samples)
                    logger.warning(f"⚠️ Import completed with {error_count} error(s)")
                    for i, error in enumerate(result.error_samples[:5], 1):
                        logger.warning(f"  Error {i}: {error}")
                
                return {
                    "operation_name": operation_name or "unknown",
                    "status": "completed",
                    "gcs_uri": gcs_uri,
                    "import_type": import_type,
                    "datastore_id": datastore_id
                }
            except gcp_exceptions.GoogleAPIError as api_error:
                # Handle API errors (like conflicting imports)
                error_str = str(api_error)
                if "Conflicting document import" in error_str or "already in progress" in error_str.lower():
                    logger.warning(f"⚠️ Import already in progress: {error_str}")
                    # Extract operation name from error if available
                    op_match = re.search(r'operations/([^/\s]+)', error_str)
                    conflicting_op = op_match.group(1) if op_match else None
                    return {
                        "operation_name": conflicting_op or operation_name or "unknown",
                        "status": "in_progress",
                        "gcs_uri": gcs_uri,
                        "import_type": import_type,
                        "datastore_id": datastore_id,
                        "note": "Import already in progress - waiting for previous operation to complete"
                    }
                else:
                    # Re-raise other API errors
                    raise
            except Exception as result_error:
                # Operation started but result check failed
                logger.warning(f"Import operation started but result check failed: {result_error}")
                return {
                    "operation_name": operation_name or "unknown",
                    "status": "started",
                    "gcs_uri": gcs_uri,
                    "import_type": import_type,
                    "datastore_id": datastore_id,
                    "note": "Operation started but result check failed - check operation status manually"
                }

        except gcp_exceptions.GoogleAPIError as api_error:
            # Handle API errors at the top level
            error_str = str(api_error)
            logger.error(f"Error importing documents (API error): {error_str}")
            raise
        except Exception as e:
            # Log full error details
            error_type = type(e).__name__
            error_msg = str(e)
            logger.error(f"Error importing documents ({error_type}): {error_msg}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def get_datastore_info(self, merchant_id: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a datastore

        Args:
            merchant_id: Merchant identifier

        Returns:
            dict with datastore information or None if not found
        """
        try:
            datastore_id = f"{merchant_id}-engine"
            datastore_path = f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}/dataStores/{datastore_id}"

            datastore_service = vertex.DataStoreServiceClient()
            datastore = datastore_service.get_data_store(name=datastore_path)

            return {
                "datastore_id": datastore_id,
                "name": datastore.display_name,
                "solution_types": list(datastore.solution_types),
                "create_time": datastore.create_time.isoformat() if datastore.create_time else None
            }

        except Exception as e:
            logger.warning(f"Could not get datastore info: {e}")
            return None


