"""Database helper functions for merchant onboarding"""

import os
import logging
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool

logger = logging.getLogger(__name__)

# Database connection pool
_db_pool = None


def get_db_pool():
    """Get or create database connection pool"""
    global _db_pool
    if _db_pool is None:
        db_dsn = os.getenv("DB_DSN")
        if not db_dsn:
            raise ValueError("DB_DSN environment variable not set")
        
        try:
            _db_pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                dsn=db_dsn
            )
            logger.info("Database connection pool created")
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise
    
    return _db_pool


def get_connection():
    """Get a database connection from the pool"""
    pool = get_db_pool()
    return pool.getconn()


def return_connection(conn):
    """Return a connection to the pool"""
    pool = get_db_pool()
    pool.putconn(conn)


# ============================================================================
# MERCHANT FUNCTIONS
# ============================================================================

def get_merchant(merchant_id: str, user_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Get merchant (optionally verify it belongs to user)
    
    Args:
        merchant_id: Merchant identifier
        user_id: User identifier (Firebase UID) - optional, if provided verifies ownership
    
    Returns:
        Merchant dict or None if not found/not owned by user
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Merchants table in public schema
        if user_id:
            query = """
                SELECT * FROM merchants
                WHERE merchant_id = %s AND user_id = %s
            """
            cursor.execute(query, (merchant_id, user_id))
        else:
            query = """
                SELECT * FROM merchants
                WHERE merchant_id = %s
            """
            cursor.execute(query, (merchant_id,))
        
        result = cursor.fetchone()
        
        cursor.close()
        return dict(result) if result else None
        
    except psycopg2.Error as e:
        logger.error(f"Database error getting merchant: {e}")
        return None
    except Exception as e:
        logger.error(f"Error getting merchant: {e}")
        return None
    finally:
        if conn:
            return_connection(conn)


def create_merchant(
    merchant_id: str,
    user_id: str,
    shop_name: str,
    shop_url: Optional[str] = None,
    bot_name: Optional[str] = "AI Assistant",
    platform: Optional[str] = None,
    custom_url_pattern: Optional[str] = None,
    **kwargs
) -> bool:
    """
    Create a new merchant record with comprehensive tracking
    
    Args:
        merchant_id: Merchant identifier
        user_id: User identifier
        shop_name: Shop name
        shop_url: Shop URL (optional)
        bot_name: Bot name (optional)
        platform: E-commerce platform (optional)
        custom_url_pattern: Custom URL pattern (optional)
        **kwargs: Additional merchant fields
    
    Returns:
        True if created successfully
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Build dynamic query to handle optional fields
        base_fields = ['merchant_id', 'user_id', 'shop_name', 'shop_url', 'bot_name', 'status', 'onboarding_status']
        base_values = [merchant_id, user_id, shop_name, shop_url, bot_name, 'active', 'pending']
        
        # Add optional fields if provided
        optional_fields = ['target_customer', 'customer_persona', 'bot_tone', 'prompt_text',
                          'top_questions', 'top_products', 
                          'primary_color', 'secondary_color', 'logo_url',
                          'platform', 'custom_url_pattern',
                          'knowledge_base_title', 'knowledge_base_usage_description']
        fields = base_fields.copy()
        values = base_values.copy()
        placeholders = ['%s'] * len(base_fields)
        
        for field in optional_fields:
            if field in kwargs and kwargs[field] is not None:
                fields.append(field)
                values.append(kwargs[field])
                placeholders.append('%s')
            elif field == 'platform' and platform:
                fields.append(field)
                values.append(platform)
                placeholders.append('%s')
            elif field == 'custom_url_pattern' and custom_url_pattern:
                fields.append(field)
                values.append(custom_url_pattern)
                placeholders.append('%s')
        
        # Build INSERT ... ON CONFLICT query
        fields_str = ', '.join(fields)
        placeholders_str = ', '.join(placeholders)
        update_fields = [f"{f} = EXCLUDED.{f}" for f in fields if f not in ['merchant_id', 'status', 'created_at']]
        update_str = ', '.join(update_fields)
        
        query = f"""
            INSERT INTO merchants (
                {fields_str}, created_at, updated_at
            )
            VALUES ({placeholders_str}, NOW(), NOW())
            ON CONFLICT (merchant_id) DO UPDATE
            SET {update_str},
                updated_at = NOW()
        """
        
        cursor.execute(query, tuple(values))
        conn.commit()
        cursor.close()
        
        logger.info(f"Created/updated merchant: {merchant_id}")
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Database error creating merchant: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Error creating merchant: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)


def update_merchant_onboarding_step(
    merchant_id: str,
    step_name: str,
    completed: bool = True,
    file_paths: Optional[Dict[str, str]] = None,
    counts: Optional[Dict[str, int]] = None,
    error: Optional[str] = None
) -> bool:
    """
    Update merchant onboarding step completion and track file paths
    
    Args:
        merchant_id: Merchant identifier
        step_name: Step name (e.g., 'products_processed', 'config_generated')
        completed: Whether step is completed
        file_paths: Dict of file paths (e.g., {'config_path': '...', 'products_json_path': '...'})
        counts: Dict of counts (e.g., {'product_count': 150, 'document_count': 5})
        error: Error message if step failed
    
    Returns:
        True if updated successfully
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # Map step names to database column names
        step_columns = {
            'merchant_record': 'step_merchant_record_completed',
            'folders': 'step_folders_created',
            'products': 'step_products_processed',
            'categories': 'step_categories_processed',
            'documents': 'step_documents_converted',
            'vertex': 'step_vertex_setup',
            'config': 'step_config_generated',
            'onboarding': 'step_onboarding_completed'
        }
        
        step_col = step_columns.get(step_name)
        if not step_col:
            logger.warning(f"Unknown step name: {step_name}")
            return False
        
        # Build update query
        updates = [f"{step_col} = %s", f"{step_col}_at = NOW()"]
        values = [completed]
        
        # Add config_path if provided (only file path we track)
        if file_paths and 'config_path' in file_paths:
            updates.append("config_path = %s")
            values.append(file_paths['config_path'])
        
        # Add counts if provided
        if counts:
            if 'product_count' in counts:
                updates.append("product_count = %s")
                values.append(counts['product_count'])
            if 'category_count' in counts:
                updates.append("category_count = %s")
                values.append(counts['category_count'])
            if 'document_count' in counts:
                updates.append("document_count = %s")
                values.append(counts['document_count'])
        
        # Update onboarding status
        if step_name == 'onboarding' and completed:
            updates.append("onboarding_status = 'completed'")
            updates.append("last_onboarding_at = NOW()")
        elif step_name == 'onboarding' and not completed:
            updates.append("onboarding_status = 'failed'")
        
        # Add error if provided
        if error:
            updates.append("last_error = %s")
            values.append(error)
        
        # Always update updated_at
        updates.append("updated_at = NOW()")
        values.append(merchant_id)
        
        query = f"""
            UPDATE merchants
            SET {', '.join(updates)}
            WHERE merchant_id = %s
        """
        
        cursor.execute(query, tuple(values))
        conn.commit()
        cursor.close()
        
        logger.info(f"Updated merchant {merchant_id} step {step_name}: completed={completed}")
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Database error updating merchant step: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Error updating merchant step: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)


# ============================================================================
# CRM INTEGRATION FUNCTIONS
# ============================================================================

def get_crm_integrations(user_id: str) -> list:
    """
    Get all CRM integrations for a user

    Args:
        user_id: User identifier

    Returns:
        List of CRM types (e.g., ['hubspot', 'salesforce'])
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)

        # Query crm_integration table in crm schema
        query = """
            SELECT crm_type
            FROM crm.crm_integration
            WHERE user_id = %s
        """

        cursor.execute(query, (user_id,))
        results = cursor.fetchall()
        cursor.close()

        # Extract crm_type values into an array
        crm_types = [row['crm_type'] for row in results] if results else []
        return crm_types

    except Exception as e:
        logger.error(f"Error getting CRM integrations: {e}")
        return []
    finally:
        if conn:
            return_connection(conn)


# ============================================================================
# SUBSCRIPTION FUNCTIONS
# ============================================================================

def check_subscription(user_id: str) -> bool:
    """
    Check if user has active subscription or is a production user
    
    Args:
        user_id: User identifier
    
    Returns:
        True if user has active subscription or is a production user
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # First check if user is a production user (bypasses subscription)
        # This check is safe even if user_type column doesn't exist yet (will be caught by exception handler)
        try:
            user_query = """
                SELECT user_type 
                FROM users
                WHERE user_id = %s
                LIMIT 1
            """
            cursor.execute(user_query, (user_id,))
            user_result = cursor.fetchone()
            
            if user_result and user_result.get('user_type') == 'production':
                cursor.close()
                logger.info(f"User {user_id} is a production user, bypassing subscription check")
                return True
        except Exception as user_check_error:
            # If user_type column doesn't exist or other error, log and continue to subscription check
            logger.debug(f"Could not check user_type (column may not exist yet): {user_check_error}")
            # Continue to subscription check below
        
        # Check user_subscriptions in billing schema
        query = """
            SELECT subscription_id 
            FROM billing.user_subscriptions
            WHERE user_id = %s 
                AND status = 'active'
                AND current_period_end > NOW()
            LIMIT 1
        """
        
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()
        cursor.close()
        
        return result is not None
        
    except Exception as e:
        logger.error(f"Error checking subscription: {e}")
        return False
    finally:
        if conn:
            return_connection(conn)


def get_subscription(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Get user's active subscription details
    
    Args:
        user_id: User identifier
    
    Returns:
        Subscription dict or None
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Get from billing.user_subscriptions
        query = """
            SELECT * 
            FROM billing.user_subscriptions
            WHERE user_id = %s 
                AND status = 'active'
                AND current_period_end > NOW()
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        cursor.execute(query, (user_id,))
        result = cursor.fetchone()
        cursor.close()
        
        return dict(result) if result else None
        
    except Exception as e:
        logger.error(f"Error getting subscription: {e}")
        return None
    finally:
        if conn:
            return_connection(conn)


# ============================================================================
# ONBOARDING JOB FUNCTIONS
# ============================================================================

def create_onboarding_job(
    job_id: str,
    merchant_id: str,
    user_id: str
) -> bool:
    """
    Create onboarding job record in database
    
    Args:
        job_id: Job identifier
        merchant_id: Merchant identifier
        user_id: User identifier
    
    Returns:
        True if created successfully
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        query = """
            INSERT INTO onboarding_jobs (
                job_id, merchant_id, user_id, status, progress, created_at, updated_at
            )
            VALUES (%s, %s, %s, 'pending', 0, NOW(), NOW())
        """
        
        cursor.execute(query, (job_id, merchant_id, user_id))
        conn.commit()
        cursor.close()
        
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Database error creating job: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Error creating job: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)


def update_onboarding_job(
    job_id: str,
    status: str,
    progress: Optional[int] = None,
    current_step: Optional[str] = None,
    error_message: Optional[str] = None
) -> bool:
    """
    Update onboarding job status
    
    Args:
        job_id: Job identifier
        status: Job status
        progress: Progress percentage (0-100)
        current_step: Current step name
        error_message: Error message if failed
    
    Returns:
        True if updated successfully
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        # TODO: Update based on your actual schema
        query = """
            UPDATE onboarding_jobs
            SET status = %s,
                progress = COALESCE(%s, progress),
                current_step = COALESCE(%s, current_step),
                error_message = %s,
                updated_at = NOW()
            WHERE job_id = %s
        """
        
        cursor.execute(query, (status, progress, current_step, error_message, job_id))
        conn.commit()
        cursor.close()
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating job: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def verify_merchant_access(merchant_id: str, user_id: str) -> bool:
    """
    Verify that merchant belongs to user
    
    Args:
        merchant_id: Merchant identifier
        user_id: User identifier
    
    Returns:
        True if merchant belongs to user
    """
    merchant = get_merchant(merchant_id, user_id)
    return merchant is not None


def get_user_merchants(user_id: str) -> list:
    """
    Get all merchants for a user
    
    Args:
        user_id: User identifier
    
    Returns:
        List of merchant dicts
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        query = """
            SELECT * FROM merchants
            WHERE user_id = %s
            ORDER BY created_at DESC
        """
        
        cursor.execute(query, (user_id,))
        results = cursor.fetchall()
        cursor.close()
        
        return [dict(row) for row in results]
        
    except Exception as e:
        logger.error(f"Error getting user merchants: {e}")
        return []
    finally:
        if conn:
            return_connection(conn)


def update_merchant(
    merchant_id: str,
    user_id: str,
    **updates
) -> bool:
    """
    Update merchant information
    
    Args:
        merchant_id: Merchant identifier
        user_id: User identifier (for verification)
        **updates: Fields to update (shop_name, shop_url, bot_name, etc.)
    
    Returns:
        True if updated successfully
    """
    conn = None
    try:
        # Verify merchant belongs to user
        if not verify_merchant_access(merchant_id, user_id):
            logger.warning(f"User {user_id} does not have access to merchant {merchant_id}")
            return False
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Build dynamic update query
        allowed_fields = [
            'shop_name', 'shop_url', 'bot_name', 'target_customer',
            'customer_persona', 'bot_tone', 'prompt_text',
            'top_questions', 'top_products', 'primary_color', 
            'secondary_color', 'logo_url', 'status'
        ]
        
        update_fields = []
        update_values = []
        
        for field, value in updates.items():
            if field in allowed_fields:
                update_fields.append(f"{field} = %s")
                update_values.append(value)
        
        if not update_fields:
            logger.warning(f"No valid fields to update for merchant {merchant_id}")
            return False
        
        # Add updated_at
        update_fields.append("updated_at = NOW()")
        update_values.append(merchant_id)
        update_values.append(user_id)
        
        query = f"""
            UPDATE merchants
            SET {', '.join(update_fields)}
            WHERE merchant_id = %s AND user_id = %s
        """
        
        cursor.execute(query, tuple(update_values))
        conn.commit()
        cursor.close()
        
        logger.info(f"Updated merchant {merchant_id}: {', '.join(updates.keys())}")
        return True
        
    except psycopg2.Error as e:
        logger.error(f"Database error updating merchant: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Error updating merchant: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)


def delete_merchant(merchant_id: str, user_id: str) -> bool:
    """
    Delete merchant and all associated data
    
    Args:
        merchant_id: Merchant identifier
        user_id: User identifier (for verification)
    
    Returns:
        True if deleted successfully
    """
    conn = None
    try:
        # Verify merchant belongs to user
        if not verify_merchant_access(merchant_id, user_id):
            logger.warning(f"User {user_id} does not have access to merchant {merchant_id}")
            return False
        
        conn = get_connection()
        cursor = conn.cursor()
        
        # Delete merchant (cascade will handle related records)
        query = """
            DELETE FROM merchants
            WHERE merchant_id = %s AND user_id = %s
        """
        
        cursor.execute(query, (merchant_id, user_id))
        rows_deleted = cursor.rowcount
        conn.commit()
        cursor.close()
        
        if rows_deleted > 0:
            logger.info(f"Deleted merchant {merchant_id} for user {user_id}")
            return True
        else:
            logger.warning(f"Merchant {merchant_id} not found or not owned by user {user_id}")
            return False
        
    except psycopg2.Error as e:
        logger.error(f"Database error deleting merchant: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Error deleting merchant: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            return_connection(conn)

