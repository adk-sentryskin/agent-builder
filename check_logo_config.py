#!/usr/bin/env python3
"""
Check logo configuration for a merchant
"""

import sys
import os
import json
import requests

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def check_logo_config(api_base_url: str, merchant_id: str, user_id: str):
    """Check logo configuration"""
    url = f"{api_base_url}/merchants/{merchant_id}/config"
    params = {"user_id": user_id}
    
    print(f"Checking logo configuration for merchant: {merchant_id}")
    print(f"API URL: {url}")
    print()
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            config = data.get("config", {})
            custom_chatbot = config.get("custom_chatbot", {})
            branding = config.get("branding", {})
            
            print("=" * 60)
            print("Logo Configuration Status")
            print("=" * 60)
            print()
            
            # Check custom_chatbot logo
            print("Custom Chatbot Logo:")
            print("-" * 60)
            logo_signed_url = custom_chatbot.get("logo_signed_url", "")
            logo_url = custom_chatbot.get("logo_url", "")
            logo_url_expires_in = custom_chatbot.get("logo_url_expires_in")
            
            if logo_signed_url:
                print(f"  ✅ logo_signed_url: {logo_signed_url[:80]}...")
                if logo_url_expires_in:
                    print(f"  ✅ Expires in: {logo_url_expires_in} seconds")
            elif logo_url:
                print(f"  ⚠️  logo_url (public): {logo_url[:80]}...")
                print(f"  ⚠️  logo_signed_url: EMPTY (should be generated)")
            else:
                print(f"  ❌ No logo URL found")
                print(f"     logo_signed_url: {logo_signed_url}")
                print(f"     logo_url: {logo_url}")
            
            print()
            
            # Check branding logo
            print("Branding Logo:")
            print("-" * 60)
            branding_logo_signed_url = branding.get("logo_signed_url", "")
            branding_logo_url = branding.get("logo_url", "")
            branding_logo_expires_in = branding.get("logo_url_expires_in")
            
            if branding_logo_signed_url:
                print(f"  ✅ logo_signed_url: {branding_logo_signed_url[:80]}...")
                if branding_logo_expires_in:
                    print(f"  ✅ Expires in: {branding_logo_expires_in} seconds")
            elif branding_logo_url:
                print(f"  ⚠️  logo_url (public): {branding_logo_url[:80]}...")
                print(f"  ⚠️  logo_signed_url: EMPTY (should be generated)")
            else:
                print(f"  ❌ No logo URL found")
            
            print()
            print("=" * 60)
            print()
            
            # Check if logo file exists in GCS
            if logo_url or logo_signed_url:
                logo_to_check = logo_signed_url or logo_url
                if 'storage.cloud.google.com' in logo_to_check or 'storage.googleapis.com' in logo_to_check:
                    print("To verify logo file exists in GCS:")
                    print(f"  Check: {logo_to_check}")
                elif logo_to_check.startswith('merchants/'):
                    print(f"Logo path in config: {logo_to_check}")
                    print("This should be converted to a URL")
            
            # Recommendations
            print()
            print("Recommendations:")
            print("-" * 60)
            if not logo_signed_url and not logo_url:
                print("1. Upload logo using: POST /files/upload-url (folder: brand-images)")
                print("2. Save logo_path using: POST /agents/custom-chatbot")
                print("   Example: { \"logo_path\": \"merchants/test-by/brand-images/logo.png\" }")
            elif logo_url and not logo_signed_url:
                print("1. The logo URL is stored but signed URL generation may have failed")
                print("2. Check server logs for errors")
                print("3. Try calling GET /merchants/{merchant_id}/config again")
            
        else:
            print(f"❌ Failed to get config: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 check_logo_config.py <api_base_url> <merchant_id> <user_id>")
        print()
        print("Example:")
        print('  python3 check_logo_config.py \\')
        print('    "https://merchant-onboarding-api-393304610205.us-central1.run.app" \\')
        print('    "test-by" \\')
        print('    "bZoS4eXGJbaHx6P2yrmOcEAbPfP2"')
        sys.exit(1)
    
    api_base_url = sys.argv[1].rstrip('/')
    merchant_id = sys.argv[2]
    user_id = sys.argv[3]
    
    check_logo_config(api_base_url, merchant_id, user_id)
