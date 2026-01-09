"""Google Cloud Storage handler with signed URL generation"""

import os
import json
import logging
from typing import Optional, List
from datetime import timedelta

# Load environment variables from .env file if available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, use system environment variables

from google.cloud import storage
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


class GCSHandler:
    """Handler for Google Cloud Storage operations"""

    def __init__(
        self,
        bucket_name: str = None,
        project_id: str = None
    ):
        """
        Initialize GCS handler

        Args:
            bucket_name: GCS bucket name (default: from env or 'chekout-ai')
            project_id: GCP project ID (default: from env or 'shopify-473015')
        """
        self.bucket_name = bucket_name or os.getenv("GCS_BUCKET_NAME", "chekout-ai")
        self.project_id = project_id or os.getenv("GCP_PROJECT_ID", "shopify-473015")
        
        try:
            # Try to use credentials from environment variables if GOOGLE_APPLICATION_CREDENTIALS is not set
            credentials = self._get_credentials()
            if credentials:
                self.client = storage.Client(project=self.project_id, credentials=credentials)
                logger.info("Using service account credentials from environment variables")
            else:
                logger.warning("No service account credentials found. Attempting to use default credentials.")
                logger.warning("If this fails, make sure GCS_CLIENT_EMAIL and GCS_PRIVATE_KEY are set in .env file")
                self.client = storage.Client(project=self.project_id)
            
            self.bucket = self.client.bucket(self.bucket_name)
            
            # Try to verify bucket exists, but don't fail if we don't have bucket.get permission or credentials
            # The bucket will be created/verified when we actually use it
            try:
                self.bucket.reload()
                logger.info(f"Initialized GCS handler for bucket: {self.bucket_name} (verified)")
            except Exception as verify_error:
                error_str = str(verify_error)
                if "storage.buckets.get" in error_str or "403" in error_str:
                    logger.warning(f"Could not verify bucket access (missing storage.buckets.get permission). Bucket operations may still work.")
                    logger.info(f"Initialized GCS handler for bucket: {self.bucket_name} (unverified)")
                elif "RefreshError" in error_str or "Reauthentication" in error_str:
                    logger.warning(f"Could not verify bucket (credential issue): {error_str}")
                    logger.warning("Continuing without verification. Make sure GCS_CLIENT_EMAIL and GCS_PRIVATE_KEY are set in .env file")
                    logger.warning("Bucket operations will be attempted when actually used.")
                    logger.info(f"Initialized GCS handler for bucket: {self.bucket_name} (unverified - credential issue)")
                    # Don't raise - allow initialization to continue
                else:
                    logger.warning(f"Could not verify bucket: {error_str}")
                    logger.warning("Continuing without verification. Bucket will be verified when actually used.")
                    logger.info(f"Initialized GCS handler for bucket: {self.bucket_name} (unverified)")
                    # Don't raise - allow initialization to continue
        except Exception as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise

    def _get_credentials(self):
        """Get credentials from environment variables or service account file"""
        # First, check if GOOGLE_APPLICATION_CREDENTIALS is set (service account JSON file)
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if creds_path and os.path.exists(creds_path):
            return service_account.Credentials.from_service_account_file(creds_path)
        
        # Otherwise, try to construct credentials from environment variables
        gcs_client_email = os.getenv("GCS_CLIENT_EMAIL")
        gcs_private_key = os.getenv("GCS_PRIVATE_KEY")
        gcs_private_key_id = os.getenv("GCS_PRIVATE_KEY_ID")
        gcs_project_id = os.getenv("GCS_PROJECT_ID") or self.project_id
        
        # Debug logging
        if gcs_client_email:
            logger.info(f"Found GCS_CLIENT_EMAIL: {gcs_client_email}")
        else:
            logger.warning("GCS_CLIENT_EMAIL not found in environment")
        
        if gcs_private_key:
            key_length = len(gcs_private_key)
            has_begin = "BEGIN PRIVATE KEY" in gcs_private_key
            has_end = "END PRIVATE KEY" in gcs_private_key
            logger.info(f"Found GCS_PRIVATE_KEY (length: {key_length}, has BEGIN: {has_begin}, has END: {has_end})")
        else:
            logger.warning("GCS_PRIVATE_KEY not found in environment")
        
        if gcs_client_email and gcs_private_key:
            # Clean up the private key (remove quotes and newline escapes)
            # Handle both escaped newlines (\n) and actual newlines
            private_key = gcs_private_key.strip('"').strip("'")
            # Replace escaped newlines with actual newlines
            private_key = private_key.replace('\\n', '\n').replace('\\\\n', '\n')
            # If still no newlines, try to detect if it's all on one line
            if '\n' not in private_key and 'BEGIN PRIVATE KEY' in private_key:
                # Try to add newlines after BEGIN and before END
                private_key = private_key.replace('-----BEGIN PRIVATE KEY-----', '-----BEGIN PRIVATE KEY-----\n')
                private_key = private_key.replace('-----END PRIVATE KEY-----', '\n-----END PRIVATE KEY-----')
            
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
                # Create credentials with GCS scopes (required for GCS operations)
                credentials = service_account.Credentials.from_service_account_info(
                    service_account_info,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
                logger.info(f"Using GCS credentials from environment variables for: {gcs_client_email}")
                logger.info("✅ GCS credentials initialized with cloud-platform scope")
                
                return credentials
            except Exception as e:
                logger.error(f"Failed to create credentials from env vars: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                return None
        
        return None

    def generate_upload_url(
        self,
        merchant_id: str,
        folder: str,
        filename: str,
        content_type: str,
        expiration_minutes: int = 60
    ) -> dict:
        """
        Generate signed URL for direct file upload to GCS

        Args:
            merchant_id: Merchant identifier (for multi-tenant isolation)
            folder: Folder name (knowledge_base, prompt-docs, training_files, brand-images)
            filename: Original filename
            content_type: MIME type of file
            expiration_minutes: URL expiration time in minutes

        Returns:
            dict with upload_url, object_path, expires_in
        """
        # Validate folder
        valid_folders = ['knowledge_base', 'prompt-docs', 'training_files', 'brand-images']
        if folder not in valid_folders:
            raise ValueError(f"Invalid folder. Must be one of: {valid_folders}")

        # Construct object path - use merchant_id for proper multi-tenant isolation
        object_path = f"merchants/{merchant_id}/{folder}/{filename}"

        try:
            # Check if credentials are available and have private key
            if not hasattr(self.client, '_credentials'):
                logger.warning("Storage client does not have credentials attribute")
            else:
                creds = self.client._credentials
                if hasattr(creds, 'private_key'):
                    has_key = creds.private_key is not None
                    logger.debug(f"Credentials have private_key: {has_key}")
                else:
                    logger.warning("Credentials object does not have private_key attribute")
            
            # Generate signed URL
            blob = self.bucket.blob(object_path)

            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(minutes=expiration_minutes),
                method="PUT",
                content_type=content_type,
            )

            logger.info(f"Generated signed URL for: {object_path}")

            return {
                "upload_url": url,
                "object_path": object_path,
                "expires_in": expiration_minutes * 60,
                "method": "PUT",
                "headers": {
                    "Content-Type": content_type
                }
            }
        except Exception as e:
            error_msg = str(e) if e else "Unknown error"
            error_type = type(e).__name__
            
            # Get more details about the exception
            error_repr = repr(e) if e else "None"
            
            # Check for common credential issues
            if "private key" in error_msg.lower() or "credentials" in error_msg.lower() or "Reauthentication" in error_msg or "you need a private key" in error_msg.lower():
                detailed_error = (
                    f"GCS credentials error: {error_msg}. "
                    f"To generate signed URLs, you need a service account with a private key. "
                    f"Please check your .env file and ensure:"
                    f"\n  1. GCS_CLIENT_EMAIL is set to your service account email"
                    f"\n  2. GCS_PRIVATE_KEY is set with the full private key (including BEGIN/END lines)"
                    f"\n  3. The private key in .env should have actual newlines or \\n escapes"
                    f"\n\nAlternatively, set GOOGLE_APPLICATION_CREDENTIALS to a service account JSON file path."
                )
            else:
                detailed_error = f"Error generating signed URL ({error_type}): {error_msg}"
                if error_repr != error_msg:
                    detailed_error += f"\nException details: {error_repr}"
            
            logger.error(f"Error generating signed URL: {detailed_error}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            raise ValueError(detailed_error) from e

    def generate_download_url(
        self,
        object_path: str,
        expiration_minutes: int = 60
    ) -> dict:
        """
        Generate signed URL for downloading a file from GCS

        Args:
            object_path: GCS object path (e.g., merchants/my-store/knowledge_base/file.pdf)
            expiration_minutes: URL expiration time in minutes

        Returns:
            dict with download_url, object_path, expires_in, file_size, content_type
            If credentials fail, returns file info with error message (doesn't raise exception)
        """
        filename = object_path.split("/")[-1] if "/" in object_path else object_path
        base_response = {
            "object_path": object_path,
            "filename": filename,
            "download_url": None,
            "download_url_expires_in": None,
            "file_size": None,
            "content_type": "application/octet-stream",
            "uploaded_at": None
        }
        
        try:
            blob = self.bucket.blob(object_path)
            
            # Try to check if file exists and get metadata
            try:
                if not blob.exists():
                    base_response["error"] = "File not found in storage"
                    return base_response
            except Exception as exists_error:
                error_msg = str(exists_error)
                logger.warning(f"Could not check file existence for {object_path}: {error_msg}")
                
                # If it's a credential issue, return with error but don't fail
                if "Reauthentication" in error_msg or "RefreshError" in error_msg or "credentials" in error_msg.lower():
                    base_response["error"] = "GCS credentials expired or invalid. Please check GCS_CLIENT_EMAIL and GCS_PRIVATE_KEY in .env file"
                    return base_response
                # For other errors, continue to try getting metadata
            
            # Try to get file metadata
            try:
                blob.reload()
                base_response["file_size"] = blob.size
                base_response["content_type"] = blob.content_type or "application/octet-stream"
                base_response["uploaded_at"] = blob.time_created.isoformat() if blob.time_created else None
            except Exception as metadata_error:
                error_msg = str(metadata_error)
                logger.warning(f"Could not get file metadata for {object_path}: {error_msg}")
                
                # If it's a credential issue, return with error
                if "Reauthentication" in error_msg or "RefreshError" in error_msg or "credentials" in error_msg.lower():
                    base_response["error"] = "GCS credentials expired or invalid. Please check GCS_CLIENT_EMAIL and GCS_PRIVATE_KEY in .env file"
                    return base_response
                # For other errors, continue to try generating URL
            
            # Try to generate signed URL
            try:
                url = blob.generate_signed_url(
                    version="v4",
                    expiration=timedelta(minutes=expiration_minutes),
                    method="GET"
                )
                logger.info(f"Generated download URL for: {object_path}")
                
                base_response["download_url"] = url
                base_response["download_url_expires_in"] = expiration_minutes * 60
                return base_response
                
            except Exception as url_error:
                # If signed URL generation fails, return file info without download URL
                error_msg = str(url_error)
                logger.warning(f"Could not generate download URL for {object_path}: {error_msg}")
                
                # Check if it's a credential issue
                if "Reauthentication" in error_msg or "RefreshError" in error_msg or "credentials" in error_msg.lower():
                    base_response["error"] = "GCS credentials expired or invalid. Please check GCS_CLIENT_EMAIL and GCS_PRIVATE_KEY in .env file"
                else:
                    base_response["error"] = f"Error generating download URL: {error_msg}"
                
                return base_response
                
        except FileNotFoundError:
            base_response["error"] = "File not found in storage"
            return base_response
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error in generate_download_url for {object_path}: {error_msg}")
            
            # Check if it's a credential issue (check multiple patterns)
            credential_errors = [
                "Reauthentication",
                "RefreshError", 
                "credentials",
                "authentication",
                "invalid_grant",
                "unauthorized",
                "403",
                "permission denied"
            ]
            
            is_credential_error = any(err.lower() in error_msg.lower() for err in credential_errors)
            
            if is_credential_error:
                base_response["error"] = "GCS credentials expired or invalid. Please check GCS_CLIENT_EMAIL and GCS_PRIVATE_KEY in .env file. See GCS_CREDENTIALS_FIX.md for help."
            else:
                base_response["error"] = f"Error accessing file: {error_msg}"
            
            return base_response
    
    def list_files_in_folder(self, folder_path: str) -> List[dict]:
        """
        List all files in a GCS folder
        
        Args:
            folder_path: Folder path (e.g., merchants/my-store/knowledge_base)
            
        Returns:
            List of file information dicts
        """
        try:
            files = []
            # Ensure folder path ends with / for prefix matching
            prefix = folder_path.rstrip('/') + '/'
            
            blobs = self.bucket.list_blobs(prefix=prefix)
            
            for blob in blobs:
                # Skip if it's a folder marker (ends with /)
                if blob.name.endswith('/'):
                    continue
                    
                files.append({
                    "file_path": blob.name,
                    "filename": blob.name.split("/")[-1],
                    "file_size": blob.size,
                    "content_type": blob.content_type or "application/octet-stream",
                    "uploaded_at": blob.time_created.isoformat() if blob.time_created else None
                })
            
            return files
        except Exception as e:
            logger.error(f"Error listing files in folder {folder_path}: {e}")
            return []
    
    def file_exists(self, object_path: str) -> bool:
        """
        Check if a file exists in GCS
        
        Args:
            object_path: GCS object path
            
        Returns:
            True if file exists, False otherwise
        """
        try:
            blob = self.bucket.blob(object_path)
            return blob.exists()
        except Exception as e:
            logger.error(f"Error checking file existence: {e}")
            return False
    
    def delete_file(self, object_path: str) -> dict:
        """
        Delete a file from GCS
        
        Args:
            object_path: GCS object path
            
        Returns:
            dict with deletion status
        """
        try:
            blob = self.bucket.blob(object_path)
            
            if not blob.exists():
                raise FileNotFoundError(f"File not found: {object_path}")
            
            blob.delete()
            logger.info(f"Deleted file: {object_path}")
            
            return {
                "status": "deleted",
                "object_path": object_path,
                "message": "File deleted successfully"
            }
        except FileNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error deleting file: {e}")
            raise

    def confirm_upload(self, object_path: str) -> dict:
        """
        Confirm that a file was uploaded successfully

        Args:
            object_path: GCS object path

        Returns:
            dict with confirmation status
        """
        try:
            blob = self.bucket.blob(object_path)

            if not blob.exists():
                raise FileNotFoundError(f"File not found: {object_path}")

            logger.info(f"Confirmed upload: {object_path} (size: {blob.size} bytes)")

            return {
                "status": "confirmed",
                "object_path": object_path,
                "size": blob.size,
                "content_type": blob.content_type,
                "created": blob.time_created.isoformat() if blob.time_created else None
            }
        except FileNotFoundError:
            raise
        except Exception as e:
            logger.error(f"Error confirming upload: {e}")
            raise

    def create_folder_structure(self, merchant_id: str, user_id: str = None) -> dict:
        """
        Create folder structure for merchant
        Uses merchant_id for proper multi-tenant isolation

        Args:
            merchant_id: Merchant identifier (primary identifier)
            user_id: User identifier (optional, for reference)

        Returns:
            dict with created folder paths
        """
        folders = [
            f"merchants/{merchant_id}/knowledge_base",
            f"merchants/{merchant_id}/prompt-docs",
            f"merchants/{merchant_id}/training_files",
            f"merchants/{merchant_id}/brand-images",
        ]

        created_folders = []
        for folder_path in folders:
            # In GCS, folders are created implicitly when files are uploaded
            # We create a placeholder file to ensure the folder exists
            placeholder_path = f"{folder_path}/.keep"
            blob = self.bucket.blob(placeholder_path)
            if not blob.exists():
                blob.upload_from_string("", content_type="text/plain")
                created_folders.append(folder_path)
                logger.info(f"Created folder: {folder_path}")

        return {
            "status": "created",
            "folders": created_folders,
            "user_id": user_id,
            "merchant_id": merchant_id
        }

    def file_exists(self, object_path: str) -> bool:
        """Check if a file exists in GCS"""
        try:
            blob = self.bucket.blob(object_path)
            return blob.exists()
        except Exception as e:
            logger.error(f"Error checking file existence: {e}")
            return False

    def download_file(self, object_path: str) -> bytes:
        """Download file from GCS"""
        try:
            blob = self.bucket.blob(object_path)
            return blob.download_as_bytes()
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            raise

    def upload_file(self, object_path: str, content: bytes, content_type: str = None) -> dict:
        """
        Upload file to GCS (replaces existing file if it exists)
        
        Args:
            object_path: GCS object path
            content: File content as bytes
            content_type: MIME type (optional)
        
        Returns:
            dict with upload status
        """
        try:
            blob = self.bucket.blob(object_path)
            # upload_from_string automatically replaces existing files in GCS
            blob.upload_from_string(content, content_type=content_type)
            
            # Note: upload_from_string automatically replaces existing files in GCS
            # We log the action for clarity
            logger.info(f"Uploaded file (replaces if exists): {object_path}")
            return {
                "status": "uploaded",
                "object_path": object_path,
                "size": len(content)
            }
        except Exception as e:
            logger.error(f"Error uploading file: {e}")
            raise

    def list_files(self, prefix: str) -> List[str]:
        """List files with given prefix"""
        try:
            blobs = self.bucket.list_blobs(prefix=prefix)
            return [blob.name for blob in blobs if not blob.name.endswith('/')]
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            raise

