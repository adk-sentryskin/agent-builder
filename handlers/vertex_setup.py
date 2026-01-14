"""Vertex AI Search setup handler - Production Ready

IMPORTANT IAM PERMISSIONS REQUIRED:
The service account used by this class must have the following roles:
- roles/discoveryengine.admin (or roles/discoveryengine.editor)
- roles/discoveryengine.dataAdmin
- roles/storage.objectViewer
- roles/storage.objectAdmin
- roles/iam.serviceAccountUser

Without these permissions, operations will fail with IAM_PERMISSION_DENIED errors.
"""

import os
import re
import json
import logging
from typing import Dict, Any, Optional, List
from google.cloud import discoveryengine_v1 as vertex
from google.api_core import exceptions as gcp_exceptions
from google.api_core import retry as retries
from google.oauth2 import service_account
from google.protobuf import field_mask_pb2

logger = logging.getLogger(__name__)


class VertexSetup:
    """Handle Vertex AI Search datastore setup and document import
    
    Production-ready implementation with proper credential handling,
    retry logic, and error handling for Cloud Run environments.
    """

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
        # CRITICAL: Using non-global location causes IAM_PERMISSION_DENIED errors
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
                service_account_email = (
                    getattr(credentials, 'service_account_email', None) or
                    getattr(credentials, '_service_account_email', None) or
                    (credentials._key.get('client_email') if hasattr(credentials, '_key') and isinstance(credentials._key, dict) else None) or
                    'Unknown'
                )
                logger.info(f"Using service account for Vertex AI: {service_account_email}")
                
                # Store credentials for later access (CRITICAL for Cloud Run)
                self._credentials = credentials
                self._service_account_email = service_account_email
                
                # Use vertex module for all clients with explicit credentials
                # CRITICAL: Must pass credentials to all clients for Cloud Run to work
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
                # WARNING: This will fail in Cloud Run - must use service account credentials
                self.client = vertex.DocumentServiceClient()
                self.datastore_client = vertex.DataStoreServiceClient()
                self.site_search_client = vertex.SiteSearchEngineServiceClient()
                logger.warning("Using default credentials (application-default). For production, use service account credentials.")
                logger.info(f"Initialized Vertex AI Search client for project: {self.project_id}")
                # Set _credentials to None for fallback methods
                self._credentials = None
                self._service_account_email = 'Unknown'
        except Exception as e:
            logger.error(f"Failed to initialize Vertex AI Search client: {e}")
            raise

    def _get_credentials(self):
        """Get credentials from Vertex-specific or GCS environment variables or service account file
        
        CRITICAL FIX: Properly handles multiline private keys and base64 encoded keys
        """
        # First, check for Vertex-specific credentials path
        vertex_creds_path = os.getenv("VERTEX_CREDENTIALS_PATH")
        if vertex_creds_path and os.path.exists(vertex_creds_path):
            logger.info(f"Using Vertex AI credentials from file: {vertex_creds_path}")
            return service_account.Credentials.from_service_account_file(
                vertex_creds_path,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
        
        # Second, check if GOOGLE_APPLICATION_CREDENTIALS is set (service account JSON file)
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path and os.path.exists(creds_path):
            logger.info(f"Using credentials from GOOGLE_APPLICATION_CREDENTIALS: {creds_path}")
            return service_account.Credentials.from_service_account_file(
                creds_path,
                scopes=['https://www.googleapis.com/auth/cloud-platform']
            )
        
        # Third, try Vertex-specific environment variables
        vertex_client_email = os.getenv("VERTEX_CLIENT_EMAIL")
        vertex_private_key = os.getenv("VERTEX_PRIVATE_KEY")
        vertex_private_key_id = os.getenv("VERTEX_PRIVATE_KEY_ID")
        vertex_project_id = os.getenv("VERTEX_PROJECT_ID") or self.project_id
        
        if vertex_client_email and vertex_private_key:
            # CRITICAL FIX: Properly decode private key to handle multiline and base64
            # This preserves formatting and prevents "Invalid PEM format" errors
            try:
                # First try unicode_escape decode (handles \n escapes properly)
                private_key = vertex_private_key.encode('utf-8').decode('unicode_escape')
                # Remove any surrounding quotes if present
                private_key = private_key.strip('"').strip("'")
            except Exception as decode_error:
                logger.warning(f"Failed to decode private key with unicode_escape, trying fallback: {decode_error}")
                # Fallback to simple replace (for backward compatibility)
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
                    service_account_info,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
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
            # CRITICAL FIX: Same fix for GCS private key
            try:
                private_key = gcs_private_key.encode('utf-8').decode('unicode_escape')
                private_key = private_key.strip('"').strip("'")
            except Exception as decode_error:
                logger.warning(f"Failed to decode GCS private key with unicode_escape, trying fallback: {decode_error}")
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
                    service_account_info,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
                logger.info(f"Using GCS credentials for Vertex AI (fallback) for: {gcs_client_email}")
                return credentials
            except Exception as e:
                logger.warning(f"Failed to create credentials from GCS env vars: {e}")
        
        return None

    def create_datastore(
        self,
        merchant_id: str,
        shop_url: Optional[str] = None,
        shop_name: Optional[str] = None,
        create_documents_datastore: bool = True
    ) -> Dict[str, Any]:
        """
        Create or get Vertex AI Search datastores
        
        CRITICAL: Vertex AI Search does NOT allow both website crawling and NDJSON imports
        in the same datastore. Therefore, we create TWO datastores per merchant:
        
        1. Website datastore ({merchant_id}-website-engine): For public website crawling
           - ContentConfig: PUBLIC_WEBSITE
           - Used for: SiteSearchEngine crawling
           
        2. Documents datastore ({merchant_id}-docs-engine): For NDJSON document imports
           - ContentConfig: CONTENT_REQUIRED (documents have content that needs to be indexed)
           - Used for: Knowledge base documents, products, categories

        Args:
            merchant_id: Merchant identifier
            shop_url: Optional website URL for website data store
            shop_name: Optional shop name for display
            create_documents_datastore: Whether to create the documents datastore (default: True)

        Returns:
            dict with both datastore information
        """
        try:
            parent = f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}"
            display_name = shop_name or f"{merchant_id} Store"
            
            results = {
                "website_datastore": None,
                "documents_datastore": None
            }
            
            # CRITICAL: Create TWO separate datastores
            # 1. Website datastore (for crawling) - only if shop_url provided
            if shop_url:
                website_datastore_id = f"{merchant_id}-website-engine"
                website_datastore_path = f"{parent}/dataStores/{website_datastore_id}"
                
                website_result = self._create_or_get_single_datastore(
                    datastore_id=website_datastore_id,
                    datastore_path=website_datastore_path,
                    display_name=f"{display_name} - Website",
                    content_config=vertex.DataStore.ContentConfig.PUBLIC_WEBSITE,
                    shop_url=shop_url,
                    parent=parent
                )
                results["website_datastore"] = website_result
            
            # 2. Documents datastore (for NDJSON imports) - always create if requested
            if create_documents_datastore:
                docs_datastore_id = f"{merchant_id}-docs-engine"
                docs_datastore_path = f"{parent}/dataStores/{docs_datastore_id}"
                
                docs_result = self._create_or_get_single_datastore(
                    datastore_id=docs_datastore_id,
                    datastore_path=docs_datastore_path,
                    display_name=f"{display_name} - Documents",
                    content_config=vertex.DataStore.ContentConfig.CONTENT_REQUIRED,
                    shop_url=None,  # No website crawling for documents datastore
                    parent=parent
                )
                results["documents_datastore"] = docs_result
            
            # Return combined results
            return results

        except Exception as e:
            logger.error(f"Error creating datastores: {e}")
            raise
    
    def _create_or_get_single_datastore(
        self,
        datastore_id: str,
        datastore_path: str,
        display_name: str,
        content_config: vertex.DataStore.ContentConfig,
        shop_url: Optional[str],
        parent: str
    ) -> Dict[str, Any]:
        """
        Helper method to create or get a single datastore
        
        Args:
            datastore_id: Datastore ID
            datastore_path: Full datastore path
            display_name: Display name for the datastore
            content_config: Content configuration (PUBLIC_WEBSITE, CONTENT_REQUIRED, or NO_CONTENT)
            shop_url: Optional website URL (only for PUBLIC_WEBSITE)
            parent: Parent path for datastore creation

        Returns:
            dict with datastore information
        """
        try:
            # Check if datastore already exists
            try:
                datastore = self.datastore_client.get_data_store(
                    name=datastore_path,
                    retry=retries.Retry()
                )
                logger.info(f"Datastore already exists: {datastore_id}")
                
                # If datastore exists and shop_url provided, ensure site is registered
                site_registration_result = None
                if shop_url and content_config == vertex.DataStore.ContentConfig.PUBLIC_WEBSITE:
                    site_registration_result = self._register_site_for_crawl(datastore_path, shop_url)
                
                return {
                    "datastore_id": datastore_id,
                    "datastore_path": datastore_path,
                    "status": "exists",
                    "name": datastore.display_name,
                    "content_config": datastore.content_config.name if datastore.content_config else None,
                    "shop_url": shop_url if shop_url else None,
                    "site_registration": site_registration_result
                }
            except Exception:
                # Datastore doesn't exist, create it
                logger.info(f"Creating new datastore: {datastore_id} (config: {content_config.name})")

                datastore = vertex.DataStore(
                    display_name=display_name,
                    content_config=content_config,
                    solution_types=[vertex.SolutionType.SOLUTION_TYPE_SEARCH],
                    industry_vertical=vertex.IndustryVertical.GENERIC,
                )

                try:
                    # Create the datastore
                    request = vertex.CreateDataStoreRequest(
                        parent=parent,
                        data_store=datastore,
                        data_store_id=datastore_id
                    )
                    
                    operation = self.datastore_client.create_data_store(request=request)
                    result = operation.result(timeout=600)  # 10 minute timeout
                    logger.info(f"✅ Created DataStore: {result.name}")

                    # If website URL provided, register site for crawling
                    site_registration_result = None
                    if shop_url and content_config == vertex.DataStore.ContentConfig.PUBLIC_WEBSITE:
                        site_registration_result = self._register_site_for_crawl(datastore_path, shop_url)
                        if site_registration_result.get("status") == "error":
                            logger.warning(f"⚠️ Site registration had errors but datastore was created successfully")

                    return {
                        "datastore_id": datastore_id,
                        "datastore_path": datastore_path,
                        "status": "created",
                        "content_config": content_config.name,
                        "shop_url": shop_url if shop_url else None,
                        "site_registration": site_registration_result
                    }
                except (gcp_exceptions.AlreadyExists, gcp_exceptions.Conflict) as e:
                    logger.info(f"ℹ️ DataStore already exists: {datastore_path}")
                    # If it already exists, still try to register the site
                    site_registration_result = None
                    if shop_url and content_config == vertex.DataStore.ContentConfig.PUBLIC_WEBSITE:
                        site_registration_result = self._register_site_for_crawl(datastore_path, shop_url)
                        if site_registration_result and site_registration_result.get("status") == "error":
                            logger.warning(f"⚠️ Site registration had errors for existing datastore")
                    
                    return {
                        "datastore_id": datastore_id,
                        "datastore_path": datastore_path,
                        "status": "exists",
                        "content_config": content_config.name,
                        "shop_url": shop_url if shop_url else None,
                        "site_registration": site_registration_result
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
            logger.error(f"Error creating/getting datastore {datastore_id}: {e}")
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
            
            # Check if datastore exists with retry
            try:
                datastore = self.datastore_client.get_data_store(
                    name=datastore_path,
                    retry=retries.Retry()
                )
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
            site_registration_result = None
            if shop_url:
                site_registration_result = self._register_site_for_crawl(datastore_path, shop_url)
                if site_registration_result and site_registration_result.get("status") in ["registered", "already_registered", "already_exists"]:
                    updates.append("site_registration")
                elif site_registration_result and site_registration_result.get("status") == "error":
                    logger.warning(f"⚠️ Failed to re-register site for crawl: {site_registration_result.get('error')}")
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
        
        CRITICAL: The datastore must have ContentConfig.PUBLIC_WEBSITE for this to work.

        Args:
            datastore_path: Path to the datastore
            shop_url: Website URL to crawl
        
        Returns:
            dict with registration status
        """
        try:
            # Parent must include /siteSearchEngine at the end
            # Path: projects/{project}/locations/global/collections/{collection}/dataStores/{ds}/siteSearchEngine
            parent = f"{datastore_path}/siteSearchEngine"
            
            # Remove protocol from URL (API doesn't accept http:// or https://)
            uri_pattern = shop_url.replace('https://', '').replace('http://', '').rstrip('/')
            
            # First, check if site is already registered
            existing_sites = self._list_target_sites(datastore_path)
            if existing_sites:
                for site in existing_sites:
                    if site.get('uri_pattern') == uri_pattern:
                        logger.info(f"✅ Site already registered for crawl: {shop_url} (URI: {uri_pattern})")
                        return {
                            "status": "already_registered",
                            "shop_url": shop_url,
                            "uri_pattern": uri_pattern,
                            "site_name": site.get('name')
                        }
            
            # Create TargetSite request
            target_site = vertex.TargetSite(
                provided_uri_pattern=uri_pattern,
                type_=vertex.TargetSite.Type.INCLUDE
            )
            
            request = vertex.CreateTargetSiteRequest(
                parent=parent,
                target_site=target_site
            )
            
            logger.info(f"🌐 Registering site for crawl: {shop_url} (URI pattern: {uri_pattern})")
            logger.info(f"   Parent path: {parent}")
            
            operation = self.site_search_client.create_target_site(request=request)
            
            # CRITICAL FIX: Increase timeout to 1200 seconds (20 minutes)
            # createTargetSite operation often takes 6-15 minutes, 300 seconds times out
            result = operation.result(timeout=1200)  # 20 minute timeout
            
            # Extract site name from result
            site_name = result.name if hasattr(result, 'name') else None
            logger.info(f"✅ Successfully registered site for crawl: {shop_url}")
            logger.info(f"   Site name: {site_name}")
            logger.info(f"   Vertex AI Search will automatically start crawling the website")
            
            return {
                "status": "registered",
                "shop_url": shop_url,
                "uri_pattern": uri_pattern,
                "site_name": site_name
            }
            
        except (gcp_exceptions.AlreadyExists, gcp_exceptions.Conflict) as e:
            logger.info(f"ℹ️ Site already registered: {shop_url}")
            # Try to get the existing site info
            existing_sites = self._list_target_sites(datastore_path)
            for site in existing_sites:
                if site.get('uri_pattern') == uri_pattern:
                    return {
                        "status": "already_exists",
                        "shop_url": shop_url,
                        "uri_pattern": uri_pattern,
                        "site_name": site.get('name')
                    }
            return {
                "status": "already_exists",
                "shop_url": shop_url,
                "uri_pattern": uri_pattern
            }
        except gcp_exceptions.NotFound as e:
            error_msg = f"DataStore not found - site registration skipped. Datastore must exist first. Path: {datastore_path}"
            logger.error(f"❌ {error_msg}")
            logger.error(f"   Error details: {e}")
            return {
                "status": "error",
                "error": error_msg,
                "shop_url": shop_url
            }
        except Exception as e:
            error_str = str(e).lower()
            if 'not found' in error_str or '404' in error_str:
                error_msg = f"DataStore not found - site registration skipped. Datastore must exist first. Path: {datastore_path}"
                logger.error(f"❌ {error_msg}")
                logger.error(f"   Error details: {e}")
                return {
                    "status": "error",
                    "error": error_msg,
                    "shop_url": shop_url
                }
            else:
                error_msg = f"Could not register site for crawl: {str(e)}"
                logger.error(f"❌ {error_msg}")
                logger.error(f"   Shop URL: {shop_url}")
                logger.error(f"   Datastore path: {datastore_path}")
                import traceback
                logger.error(f"   Traceback: {traceback.format_exc()}")
                return {
                    "status": "error",
                    "error": error_msg,
                    "shop_url": shop_url
                }
    
    def _list_target_sites(self, datastore_path: str) -> List[Dict[str, Any]]:
        """
        List all target sites registered for a datastore
        
        Args:
            datastore_path: Path to the datastore
            
        Returns:
            List of target site information
        """
        try:
            parent = f"{datastore_path}/siteSearchEngine"
            request = vertex.ListTargetSitesRequest(parent=parent)
            
            response = self.site_search_client.list_target_sites(request=request)
            
            sites = []
            for site in response.target_sites:
                sites.append({
                    "name": site.name if hasattr(site, 'name') else None,
                    "uri_pattern": site.provided_uri_pattern if hasattr(site, 'provided_uri_pattern') else None,
                    "type": site.type_.name if hasattr(site, 'type_') else None,
                    "site_verification_info": str(site.site_verification_info) if hasattr(site, 'site_verification_info') else None
                })
            
            return sites
        except Exception as e:
            logger.debug(f"Could not list target sites: {e}")
            return []
    
    def get_site_registration_status(self, merchant_id: str, shop_url: str) -> Dict[str, Any]:
        """
        Check if a website is registered for crawling
        
        Args:
            merchant_id: Merchant identifier
            shop_url: Website URL to check
            
        Returns:
            dict with registration status
        """
        try:
            datastore_id = f"{merchant_id}-engine"
            datastore_path = f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}/dataStores/{datastore_id}"
            
            # Verify datastore exists with retry
            try:
                datastore = self.datastore_client.get_data_store(
                    name=datastore_path,
                    retry=retries.Retry()
                )
            except Exception as e:
                return {
                    "status": "datastore_not_found",
                    "error": f"Datastore {datastore_id} not found: {str(e)}",
                    "shop_url": shop_url
                }
            
            # Get URI pattern
            uri_pattern = shop_url.replace('https://', '').replace('http://', '').rstrip('/')
            
            # List registered sites
            sites = self._list_target_sites(datastore_path)
            
            # Check if our site is registered
            for site in sites:
                if site.get('uri_pattern') == uri_pattern:
                    return {
                        "status": "registered",
                        "shop_url": shop_url,
                        "uri_pattern": uri_pattern,
                        "site_name": site.get('name'),
                        "type": site.get('type'),
                        "site_verification_info": site.get('site_verification_info')
                    }
            
            return {
                "status": "not_registered",
                "shop_url": shop_url,
                "uri_pattern": uri_pattern,
                "registered_sites": sites
            }
        except Exception as e:
            return {
                "status": "error",
                "error": str(e),
                "shop_url": shop_url
            }

    def _detect_data_schema(self, gcs_uri: str) -> str:
        """
        Detect the data schema from the NDJSON file
        
        CRITICAL: Vertex AI Search requires data_schema to match the schemaId in NDJSON
        - If schemaId is "content", use data_schema="content"
        - If schemaId is "default_schema" or missing, use data_schema="document"
        
        Args:
            gcs_uri: GCS URI of the NDJSON file
            
        Returns:
            "content" or "document"
        """
        try:
            # Try to read first line of NDJSON to detect schema
            # This is a best-effort detection - defaults to "document" if detection fails
            from google.cloud import storage
            
            # Extract bucket and path from gs:// URI
            if gcs_uri.startswith("gs://"):
                bucket_and_path = gcs_uri[5:]  # Remove "gs://"
                parts = bucket_and_path.split("/", 1)
                if len(parts) == 2:
                    bucket_name, file_path = parts
                    
                    # Create storage client with same credentials
                    if self._credentials:
                        storage_client = storage.Client(credentials=self._credentials, project=self.project_id)
                    else:
                        storage_client = storage.Client(project=self.project_id)
                    
                    bucket = storage_client.bucket(bucket_name)
                    blob = bucket.blob(file_path)
                    
                    # Download first few bytes to check schema
                    content = blob.download_as_bytes(start=0, end=2048)  # First 2KB
                    first_line = content.split(b'\n')[0].decode('utf-8', errors='ignore')
                    
                    # Parse JSON to check schemaId
                    try:
                        doc = json.loads(first_line)
                        schema_id = doc.get('schemaId', 'default_schema')
                        if schema_id == 'content':
                            logger.info(f"Detected schemaId='content' in NDJSON, using data_schema='content'")
                            return "content"
                    except (json.JSONDecodeError, KeyError):
                        pass
        except Exception as e:
            logger.debug(f"Could not detect data schema from NDJSON, defaulting to 'document': {e}")
        
        # Default to "document" schema
        logger.debug("Using default data_schema='document'")
        return "document"

    def import_documents(
        self,
        merchant_id: str,
        gcs_uri: str,
        import_type: str = "FULL",
        data_schema: Optional[str] = None,
        use_documents_datastore: bool = True
    ) -> Dict[str, Any]:
        """
        Import documents from GCS to Vertex AI Search
        
        CRITICAL: Documents are imported to the documents datastore ({merchant_id}-docs-engine),
        NOT the website datastore. This is because Vertex AI Search does NOT allow both
        website crawling and NDJSON imports in the same datastore.

        Args:
            merchant_id: Merchant identifier
            gcs_uri: GCS URI of NDJSON file (gs://bucket/path)
            import_type: Import type (FULL or INCREMENTAL)
            data_schema: Optional data schema ("document" or "content"). If not provided, will auto-detect.
            use_documents_datastore: If True, use docs-engine datastore (default). If False, use legacy engine.

        Returns:
            dict with import operation information
        """
        try:
            # CRITICAL: Use documents datastore for NDJSON imports
            if use_documents_datastore:
                datastore_id = f"{merchant_id}-docs-engine"
            else:
                # Legacy support for old single datastore approach
                datastore_id = f"{merchant_id}-engine"
            
            datastore_path = f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}/dataStores/{datastore_id}"
            
            # Parent for import must include /branches/default_branch
            # CRITICAL: Path must be: dataStores/{datastore}/branches/default_branch
            parent = f"{datastore_path}/branches/default_branch"
            
            # CRITICAL FIX: Verify datastore exists with retry and check content config
            try:
                datastore = self.datastore_client.get_data_store(
                    name=datastore_path,
                    retry=retries.Retry()
                )
                logger.info(f"Verified datastore exists: {datastore_id}")
                
                # CRITICAL: Verify content_config is CONTENT_REQUIRED for documents with content
                if datastore.content_config != vertex.DataStore.ContentConfig.CONTENT_REQUIRED:
                    current_config = datastore.content_config.name if datastore.content_config else "None"
                    error_msg = (
                        f"❌ CONTENT_CONFIG_MISMATCH: Datastore '{datastore_id}' has content_config='{current_config}' "
                        f"but requires 'CONTENT_REQUIRED' to import documents with content.\n"
                        f"SOLUTION:\n"
                        f"1. Delete the existing datastore '{datastore_id}' (if empty) OR\n"
                        f"2. Delete the datastore and recreate it - the system will now create it with CONTENT_REQUIRED automatically.\n"
                        f"Note: Datastore content_config cannot be modified after creation.\n"
                        f"Error: '[google.cloud.discoveryengine.v1.DocumentService.ImportDocuments] To create document with content, "
                        f"the content config of data store must be CONTENT_REQUIRED.'"
                    )
                    logger.error(error_msg)
                    raise Exception(error_msg)
            except Exception as check_error:
                error_msg = str(check_error)
                # Check if it's a permission error or not found
                if "IAM_PERMISSION_DENIED" in error_msg or "Permission" in error_msg:
                    # This is a permission issue - log which service account is being used
                    sa_email = getattr(self, '_service_account_email', 'Unknown')
                    raise Exception(
                        f"Permission denied accessing datastore '{datastore_id}'. "
                        f"Service account: {sa_email}. "
                        f"Ensure the service account has required IAM roles: "
                        f"roles/discoveryengine.admin, roles/discoveryengine.dataAdmin, "
                        f"roles/storage.objectViewer. Error: {error_msg}"
                    )
                elif "404" in error_msg or "not found" in error_msg.lower() or "does not exist" in error_msg.lower():
                    raise Exception(f"Datastore '{datastore_id}' not found. Please create it first or ensure datastore creation succeeded. Error: {error_msg}")
                else:
                    # Re-raise if it's a different error
                    raise

            # CRITICAL FIX: Auto-detect data_schema if not provided
            if data_schema is None:
                data_schema = self._detect_data_schema(gcs_uri)

            # Create GCS source - use vertex module for consistency
            gcs_source = vertex.GcsSource(
                input_uris=[gcs_uri] if isinstance(gcs_uri, str) else gcs_uri,
                data_schema=data_schema
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
            logger.info(f"Starting document import from: {gcs_uri} (schema: {data_schema})")
            operation = self.client.import_documents(request=request)
            
            # Get operation name safely (required for status checking)
            operation_name = None
            try:
                if hasattr(operation, 'operation'):
                    op_obj = operation.operation
                    if isinstance(op_obj, dict):
                        operation_name = op_obj.get('name') if 'name' in op_obj else None
                    elif hasattr(op_obj, 'name'):
                        operation_name = op_obj.name
                if not operation_name and hasattr(operation, 'metadata'):
                    metadata = operation.metadata
                    if isinstance(metadata, dict) and 'name' in metadata:
                        operation_name = metadata['name']
                    elif hasattr(metadata, 'name'):
                        operation_name = metadata.name
                # Try to get from operation directly
                if not operation_name and hasattr(operation, 'name'):
                    operation_name = operation.name
            except Exception as name_error:
                logger.debug(f"Could not extract operation name: {name_error}")
            
            if operation_name:
                logger.info(f"Started document import operation: {operation_name}")
            else:
                logger.warning("Started document import operation but could not extract operation name")
                # Try to construct operation name from parent
                operation_name = f"{parent}/operations/import-documents-unknown"

            # Wait for operation to complete (only if not async mode)
            try:
                result = operation.result(timeout=1800)  # 30 minute timeout
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
                    "data_schema": data_schema,
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
                        "data_schema": data_schema,
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
                    "data_schema": data_schema,
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

    def start_import_documents_async(
        self,
        merchant_id: str,
        gcs_uri: str,
        import_type: str = "FULL",
        data_schema: Optional[str] = None,
        use_documents_datastore: bool = True
    ) -> Dict[str, Any]:
        """
        Start document import without waiting for completion (async)
        
        Returns immediately with operation name for status checking.
        Use check_import_status() to check if import completed.
        
        Args:
            merchant_id: Merchant identifier
            gcs_uri: GCS URI of NDJSON file (gs://bucket/path)
            import_type: Import type (FULL or INCREMENTAL)
            data_schema: Optional data schema ("document" or "content"). If not provided, will auto-detect.
            use_documents_datastore: If True, use docs-engine datastore (default). If False, use legacy engine.
        
        Returns:
            dict with operation_name and status="started"
        """
        try:
            # CRITICAL: Use documents datastore for NDJSON imports
            if use_documents_datastore:
                datastore_id = f"{merchant_id}-docs-engine"
            else:
                datastore_id = f"{merchant_id}-engine"
            
            datastore_path = f"projects/{self.project_id}/locations/{self.location}/collections/{self.collection_id}/dataStores/{datastore_id}"
            parent = f"{datastore_path}/branches/default_branch"
            
            # Verify datastore exists
            try:
                datastore = self.datastore_client.get_data_store(
                    name=datastore_path,
                    retry=retries.Retry()
                )
                logger.info(f"Verified datastore exists: {datastore_id}")
                
                if datastore.content_config != vertex.DataStore.ContentConfig.CONTENT_REQUIRED:
                    current_config = datastore.content_config.name if datastore.content_config else "None"
                    raise Exception(
                        f"Datastore '{datastore_id}' has content_config='{current_config}' "
                        f"but requires 'CONTENT_REQUIRED' to import documents with content."
                    )
            except Exception as check_error:
                error_msg = str(check_error)
                if "IAM_PERMISSION_DENIED" in error_msg or "Permission" in error_msg:
                    sa_email = getattr(self, '_service_account_email', 'Unknown')
                    raise Exception(
                        f"Permission denied accessing datastore '{datastore_id}'. "
                        f"Service account: {sa_email}. Error: {error_msg}"
                    )
                elif "404" in error_msg or "not found" in error_msg.lower():
                    raise Exception(f"Datastore '{datastore_id}' not found. Error: {error_msg}")
                else:
                    raise
            
            # Auto-detect data_schema if not provided
            if data_schema is None:
                data_schema = self._detect_data_schema(gcs_uri)
            
            # Create import request
            gcs_source = vertex.GcsSource(
                input_uris=[gcs_uri] if isinstance(gcs_uri, str) else gcs_uri,
                data_schema=data_schema
            )
            
            request = vertex.ImportDocumentsRequest(
                parent=parent,
                gcs_source=gcs_source,
                reconciliation_mode=vertex.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
                if import_type == "INCREMENTAL"
                else vertex.ImportDocumentsRequest.ReconciliationMode.FULL
            )
            
            # Start import operation (don't wait)
            logger.info(f"Starting async document import from: {gcs_uri} (schema: {data_schema})")
            operation = self.client.import_documents(request=request)
            
            # Extract operation name
            operation_name = None
            try:
                if hasattr(operation, 'operation'):
                    op_obj = operation.operation
                    if isinstance(op_obj, dict):
                        operation_name = op_obj.get('name')
                    elif hasattr(op_obj, 'name'):
                        operation_name = op_obj.name
                if not operation_name and hasattr(operation, 'metadata'):
                    metadata = operation.metadata
                    if isinstance(metadata, dict) and 'name' in metadata:
                        operation_name = metadata['name']
                    elif hasattr(metadata, 'name'):
                        operation_name = metadata.name
                if not operation_name and hasattr(operation, 'name'):
                    operation_name = operation.name
            except Exception as name_error:
                logger.warning(f"Could not extract operation name: {name_error}")
            
            if not operation_name:
                # Construct fallback operation name
                operation_name = f"{parent}/operations/import-documents-{int(__import__('time').time())}"
                logger.warning(f"Using fallback operation name: {operation_name}")
            
            logger.info(f"✅ Started async document import operation: {operation_name}")
            
            return {
                "operation_name": operation_name,
                "status": "started",
                "gcs_uri": gcs_uri,
                "import_type": import_type,
                "data_schema": data_schema,
                "datastore_id": datastore_id,
                "message": "Document import started. Use check_import_status() to check completion."
            }
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            logger.error(f"Error starting async document import ({error_type}): {error_msg}")
            raise
    
    def check_import_status(self, operation_name: str) -> Dict[str, Any]:
        """
        Check the status of a document import operation.
        Returns status: completed | in_progress | failed | unknown
        """
        try:
            from google.api_core import operations_v1
            
            # Prefer public transport attribute, fall back to private
            transport = getattr(self.client, "transport", None) or getattr(self.client, "_transport", None)
            if transport is None:
                raise RuntimeError("Client has no transport; cannot create operations client.")

            # Prefer embedded operations client if provided by transport
            operations_client = getattr(transport, "operations_client", None) or getattr(transport, "_operations_client", None)

            # Otherwise create OperationsClient from gRPC channel (gRPC transport only)
            if operations_client is None:
                channel = getattr(transport, "grpc_channel", None) or getattr(transport, "_grpc_channel", None)
                if channel is None and hasattr(transport, "get_channel"):
                    channel = transport.get_channel()
                if channel is None and hasattr(transport, "get_grpc_channel"):
                    channel = transport.get_grpc_channel()

                if channel is None:
                    raise RuntimeError(
                        f"Cannot create OperationsClient: no gRPC channel found on transport (type={type(transport)}). "
                        "If you're using REST transport, use the transport's operations_client (if available) "
                        "or switch to gRPC transport."
                    )

                # IMPORTANT: pass channel positionally
                operations_client = operations_v1.OperationsClient(channel)

            operation = operations_client.get_operation(name=operation_name)

            # Metadata is useful even when not done
            metadata = None
            if getattr(operation, "metadata", None):
                metadata = str(operation.metadata)

            if not operation.done:
                return {
                    "operation_name": operation_name,
                    "status": "in_progress",
                    "metadata": metadata,
                    "message": "Import operation is still running",
                }
                } 

            # done=True
            if operation.HasField("error"):
                return {
                    "operation_name": operation_name,
                    "status": "failed",
                    "metadata": metadata,
                    "error": {
                        "code": operation.error.code,
                        "message": operation.error.message,
                    },
                }

            # completed
            response = None
            if getattr(operation, "response", None):
                # Usually google.protobuf.any_pb2.Any, so string may be ugly, but safe.
                response = str(operation.response)

            return {
                "operation_name": operation_name,
                "status": "completed",
                "metadata": metadata,
                "response": response,
            }

        except Exception as e:
            logger.exception("Error checking import status")
            return {
                "operation_name": operation_name,
                "status": "unknown",
                "error": str(e),
            }
    
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

            # CRITICAL FIX: Use self._credentials instead of default credentials
            # This ensures Cloud Run and Cloud Build work correctly
            if self._credentials:
                datastore_service = vertex.DataStoreServiceClient(credentials=self._credentials)
            else:
                # Fallback for local dev
                datastore_service = vertex.DataStoreServiceClient()
            
            datastore = datastore_service.get_data_store(
                name=datastore_path,
                retry=retries.Retry()
            )

            return {
                "datastore_id": datastore_id,
                "name": datastore.display_name,
                "solution_types": list(datastore.solution_types),
                "create_time": datastore.create_time.isoformat() if datastore.create_time else None
            }

        except Exception as e:
            logger.warning(f"Could not get datastore info: {e}")
            return None
