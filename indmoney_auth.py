"""
INDMONEY Authentication Handler
Manages login flow: Phone/Email → OTP → MPIN → Session Token
"""

import requests
import json
import os
from typing import Optional, Dict
from datetime import datetime, timedelta


class IndMoneyAuth:
    """Handles INDMONEY authentication flow"""
    
    def __init__(self, base_url: str = "https://mcp.indmoney.com/mcp"):
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        # Auth state
        self.phone: Optional[str] = None
        self.email: Optional[str] = None
        self.session_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None
        
        # Load saved session if exists
        self._load_session()
    
    def _get_session_file(self) -> str:
        """Get session storage file path"""
        return os.path.join(os.path.dirname(__file__), '.indmoney_session.json')
    
    def _save_session(self):
        """Save session to file"""
        if self.session_token:
            data = {
                'phone': self.phone,
                'email': self.email,
                'session_token': self.session_token,
                'refresh_token': self.refresh_token,
                'expiry': self.token_expiry.isoformat() if self.token_expiry else None,
                'saved_at': datetime.now().isoformat()
            }
            try:
                with open(self._get_session_file(), 'w') as f:
                    json.dump(data, f)
            except Exception:
                pass
    
    def _load_session(self) -> bool:
        """Load saved session"""
        try:
            file_path = self._get_session_file()
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                # Check if session is still valid (less than 24 hours old)
                saved_at = datetime.fromisoformat(data.get('saved_at', '2000-01-01'))
                if datetime.now() - saved_at < timedelta(hours=24):
                    self.phone = data.get('phone')
                    self.email = data.get('email')
                    self.session_token = data.get('session_token')
                    self.refresh_token = data.get('refresh_token')
                    
                    # Set auth header
                    if self.session_token:
                        self.session.headers['Authorization'] = f'Bearer {self.session_token}'
                    return True
                else:
                    # Session expired, clear it
                    os.remove(file_path)
        except Exception:
            pass
        return False
    
    def is_authenticated(self) -> bool:
        """Check if user is authenticated"""
        return self.session_token is not None
    
    def logout(self):
        """Clear session"""
        self.phone = None
        self.email = None
        self.session_token = None
        self.refresh_token = None
        self.token_expiry = None
        
        # Remove auth header
        if 'Authorization' in self.session.headers:
            del self.session.headers['Authorization']
        
        # Delete session file
        try:
            file_path = self._get_session_file()
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception:
            pass
    
    # ── Step 1: Request OTP ────────────────────────────────────────────
    
    def request_otp(self, phone: Optional[str] = None, email: Optional[str] = None) -> Dict:
        """
        Request OTP for login
        Provide either phone or email
        """
        if not phone and not email:
            return {'error': 'Provide phone or email'}
        
        self.phone = phone
        self.email = email
        
        # Common endpoints for OTP request
        endpoints = [
            '/api/v1/auth/otp/send',
            '/api/auth/otp/send',
            '/auth/otp/send',
            '/mcp/auth/otp/send',
            '/api/v2/auth/otp',
            '/api/v1/otp/send',
            '/api/v1/login/otp',
            '/v1/auth/otp/send',
            '/auth/otp',
            '/login/otp'
        ]
        
        payload = {}
        if phone:
            payload['phone'] = phone
            payload['country_code'] = '+91'  # India default
        if email:
            payload['email'] = email
        
        errors = []
        for endpoint in endpoints:
            try:
                url = f"{self.base_url}{endpoint}"
                print(f"[INDMONEY AUTH] Trying OTP verify: {url}")
                response = self.session.post(url, json=payload, timeout=10)
                print(f"[INDMONEY AUTH] Response: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    # Check for OTP reference ID
                    ref_id = data.get('reference_id') or data.get('ref_id') or data.get('request_id')
                    if ref_id:
                        return {
                            'success': True,
                            'message': 'OTP sent successfully',
                            'reference_id': ref_id,
                            'phone': self.phone,
                            'email': self.email
                        }
                elif response.status_code == 429:
                    return {'error': 'Too many requests. Please wait before retrying.'}
                    
            except requests.exceptions.RequestException as e:
                errors.append(f"{endpoint}: {str(e)}")
                print(f"[INDMONEY AUTH] Error on {endpoint}: {str(e)}")
                continue
        
        print(f"[INDMONEY AUTH] All endpoints failed. Errors: {errors}")
        return {'error': f'Failed to send OTP. Server may be unavailable or endpoints changed. Tried {len(endpoints)} endpoints.'}
    
    # ── Step 2: Verify OTP ────────────────────────────────────────────
    
    def verify_otp(self, otp: str, reference_id: Optional[str] = None) -> Dict:
        """
        Verify OTP
        Returns temp token for MPIN step
        """
        if not otp or len(otp) < 4:
            return {'error': 'Invalid OTP format'}
        
        endpoints = [
            '/api/v1/auth/otp/verify',
            '/api/auth/otp/verify',
            '/auth/otp/verify',
            '/mcp/auth/otp/verify',
            '/api/v1/otp/verify',
            '/v1/auth/otp/verify',
            '/api/v2/auth/otp/verify',
            '/auth/verify-otp',
            '/verify/otp'
        ]
        
        payload = {
            'otp': otp,
            'phone': self.phone,
            'email': self.email
        }
        if reference_id:
            payload['reference_id'] = reference_id
        
        errors = []
        for endpoint in endpoints:
            try:
                url = f"{self.base_url}{endpoint}"
                print(f"[INDMONEY AUTH] Trying OTP verify: {url}")
                response = self.session.post(url, json=payload, timeout=10)
                print(f"[INDMONEY AUTH] Response: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check for temp token or direct session token
                    temp_token = data.get('temp_token') or data.get('access_token')
                    
                    if temp_token:
                        # Store temp token for MPIN step
                        self.refresh_token = temp_token
                        return {
                            'success': True,
                            'message': 'OTP verified',
                            'requires_mpin': True,
                            'temp_token': temp_token
                        }
                    
                    # Check if already has session (some flows skip MPIN)
                    session_token = data.get('session_token') or data.get('token')
                    if session_token:
                        self.session_token = session_token
                        self.token_expiry = datetime.now() + timedelta(hours=24)
                        self.session.headers['Authorization'] = f'Bearer {session_token}'
                        self._save_session()
                        return {
                            'success': True,
                            'message': 'Login successful',
                            'requires_mpin': False
                        }
                        
                elif response.status_code == 401:
                    return {'error': 'Invalid OTP. Please try again.'}
                elif response.status_code == 410:
                    return {'error': 'OTP expired. Please request a new one.'}
                    
            except requests.exceptions.RequestException as e:
                errors.append(f"{endpoint}: {str(e)}")
                print(f"[INDMONEY AUTH] Error on {endpoint}: {str(e)}")
                continue
        
        print(f"[INDMONEY AUTH] All OTP verify endpoints failed. Errors: {errors}")
        return {'error': f'Failed to verify OTP. Tried {len(endpoints)} endpoints.'}
    
    # ── Step 3: Set/Verify MPIN ─────────────────────────────────────────
    
    def set_mpin(self, mpin: str, temp_token: Optional[str] = None) -> Dict:
        """
        Set or verify MPIN
        This is the final step to get session token
        """
        if not mpin or len(mpin) != 4 or not mpin.isdigit():
            return {'error': 'MPIN must be 4 digits'}
        
        token = temp_token or self.refresh_token
        if not token:
            return {'error': 'No temp token available. Verify OTP first.'}
        
        endpoints = [
            '/api/v1/auth/mpin/verify',
            '/api/auth/mpin',
            '/auth/mpin/verify',
            '/mcp/auth/mpin',
            '/api/v1/mpin/verify',
            '/v1/auth/mpin',
            '/api/v2/auth/mpin',
            '/auth/mpin',
            '/verify/mpin'
        ]
        
        # Try with auth header first
        headers_with_auth = self.session.headers.copy()
        headers_with_auth['Authorization'] = f'Bearer {token}'
        
        payload = {
            'mpin': mpin,
            'phone': self.phone,
            'email': self.email
        }
        
        errors = []
        for endpoint in endpoints:
            try:
                url = f"{self.base_url}{endpoint}"
                print(f"[INDMONEY AUTH] Trying MPIN verify: {url}")
                
                # Try with auth header
                response = requests.post(url, json=payload, headers=headers_with_auth, timeout=10)
                print(f"[INDMONEY AUTH] MPIN Response: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Extract session token
                    session_token = (data.get('session_token') or 
                                   data.get('access_token') or 
                                   data.get('token') or
                                   data.get('auth_token'))
                    
                    if session_token:
                        self.session_token = session_token
                        self.token_expiry = datetime.now() + timedelta(hours=24)
                        self.session.headers['Authorization'] = f'Bearer {session_token}'
                        self._save_session()
                        
                        return {
                            'success': True,
                            'message': 'Login successful',
                            'user': data.get('user', {})
                        }
                    else:
                        # Some APIs return token in different format
                        return {
                            'success': True,
                            'message': 'MPIN set. Check response for token.',
                            'response': data
                        }
                        
                elif response.status_code == 401:
                    return {'error': 'Invalid MPIN. Please try again.'}
                    
            except requests.exceptions.RequestException as e:
                errors.append(f"{endpoint}: {str(e)}")
                print(f"[INDMONEY AUTH] Error on {endpoint}: {str(e)}")
                continue
        
        print(f"[INDMONEY AUTH] All MPIN verify endpoints failed. Errors: {errors}")
        return {'error': f'Failed to verify MPIN. Tried {len(endpoints)} endpoints.'}
    
    # ── Refresh Session ────────────────────────────────────────────────
    
    def refresh_session(self) -> Dict:
        """Refresh session token if expired"""
        if not self.refresh_token:
            return {'error': 'No refresh token available'}
        
        endpoints = [
            '/api/v1/auth/refresh',
            '/api/auth/refresh',
            '/auth/refresh'
        ]
        
        for endpoint in endpoints:
            try:
                url = f"{self.base_url}{endpoint}"
                headers = {
                    'Authorization': f'Bearer {self.refresh_token}',
                    'Content-Type': 'application/json'
                }
                
                response = requests.post(url, headers=headers, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    session_token = (data.get('session_token') or 
                                   data.get('access_token') or 
                                   data.get('token'))
                    
                    if session_token:
                        self.session_token = session_token
                        self.token_expiry = datetime.now() + timedelta(hours=24)
                        self.session.headers['Authorization'] = f'Bearer {session_token}'
                        self._save_session()
                        return {'success': True, 'message': 'Session refreshed'}
                        
            except requests.exceptions.RequestException:
                continue
        
        return {'error': 'Failed to refresh session. Please login again.'}


# Singleton instance
_auth_instance = None

def get_auth() -> IndMoneyAuth:
    """Get or create auth instance"""
    global _auth_instance
    if _auth_instance is None:
        _auth_instance = IndMoneyAuth()
    return _auth_instance
