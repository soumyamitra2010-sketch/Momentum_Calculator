"""
INDMONEY MCP Server Connector
Fetches portfolio data from INDMONEY MCP server
"""

import requests
import json
from datetime import datetime
from typing import Optional, Dict, List, Any


class IndMoneyConnector:
    """Connector for INDMONEY MCP Server"""
    
    def __init__(self, base_url: str = "https://mcp.indmoney.com/mcp", api_key: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.session = requests.Session()
        
        # Set default headers
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        if api_key:
            self.session.headers.update({
                'Authorization': f'Bearer {api_key}'
            })
    
    def _make_request(self, endpoint: str, method: str = "GET", payload: Optional[Dict] = None) -> Dict:
        """Make authenticated request to MCP server"""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        try:
            if method.upper() == "GET":
                response = self.session.get(url, timeout=30)
            elif method.upper() == "POST":
                response = self.session.post(url, json=payload, timeout=30)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                return {"error": "Unauthorized - Invalid API key or authentication required"}
            elif response.status_code == 403:
                return {"error": "Forbidden - Access denied"}
            elif response.status_code == 404:
                return {"error": f"Endpoint not found: {endpoint}"}
            else:
                return {"error": f"HTTP {response.status_code}: {str(e)}"}
        except requests.exceptions.ConnectionError:
            return {"error": "Connection failed - Check URL and network"}
        except requests.exceptions.Timeout:
            return {"error": "Request timeout"}
        except Exception as e:
            return {"error": f"Request failed: {str(e)}"}
    
    def test_connection(self) -> Dict:
        """Test connectivity to MCP server"""
        # Try common health check endpoints
        endpoints = ["health", "ping", "status", ""]
        for endpoint in endpoints:
            result = self._make_request(endpoint)
            if "error" not in result:
                return {"success": True, "response": result}
        return {"success": False, "error": "Could not connect to MCP server"}
    
    def get_portfolio(self) -> Dict:
        """Fetch user's portfolio/holdings"""
        # Common endpoints for portfolio data
        endpoints = [
            "portfolio",
            "holdings",
            "assets",
            "investments",
            "api/v1/portfolio",
            "api/portfolio"
        ]
        
        for endpoint in endpoints:
            result = self._make_request(endpoint)
            if "error" not in result:
                return {"success": True, "data": result}
        
        return {"error": "Could not fetch portfolio - endpoint not found"}
    
    def get_transactions(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict:
        """Fetch transaction history"""
        params = {}
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
            
        endpoints = [
            "transactions",
            "orders",
            "trades",
            "api/v1/transactions"
        ]
        
        for endpoint in endpoints:
            if params:
                url = f"{endpoint}?{requests.compat.urlencode(params)}"
            else:
                url = endpoint
            result = self._make_request(url)
            if "error" not in result:
                return {"success": True, "data": result}
        
        return {"error": "Could not fetch transactions"}
    
    def get_account_summary(self) -> Dict:
        """Fetch account summary/total value"""
        endpoints = [
            "summary",
            "dashboard",
            "account",
            "networth",
            "api/v1/summary"
        ]
        
        for endpoint in endpoints:
            result = self._make_request(endpoint)
            if "error" not in result:
                return {"success": True, "data": result}
        
        return {"error": "Could not fetch account summary"}
    
    def get_mf_holdings(self) -> Dict:
        """Fetch mutual fund holdings specifically"""
        endpoints = [
            "mutualfunds",
            "mf/holdings",
            "mf",
            "funds",
            "api/v1/mf"
        ]
        
        for endpoint in endpoints:
            result = self._make_request(endpoint)
            if "error" not in result:
                return {"success": True, "data": result}
        
        return {"error": "Could not fetch MF holdings"}
    
    def get_stock_holdings(self) -> Dict:
        """Fetch stock/ETF holdings specifically"""
        endpoints = [
            "stocks",
            "equity",
            "shares",
            "etfs",
            "api/v1/stocks"
        ]
        
        for endpoint in endpoints:
            result = self._make_request(endpoint)
            if "error" not in result:
                return {"success": True, "data": result}
        
        return {"error": "Could not fetch stock holdings"}
    
    def format_portfolio_for_app(self, raw_data: Dict) -> List[Dict]:
        """Convert INDMONEY portfolio data to app-compatible format"""
        holdings = []
        
        # Try different data structures
        if isinstance(raw_data, list):
            items = raw_data
        elif isinstance(raw_data, dict):
            # Try common keys
            items = raw_data.get('holdings', []) or raw_data.get('assets', []) or raw_data.get('data', []) or []
            if not items and 'portfolio' in raw_data:
                items = raw_data['portfolio']
        else:
            items = []
        
        for item in items:
            if isinstance(item, dict):
                holding = {
                    'scrip': item.get('symbol') or item.get('ticker') or item.get('isin') or item.get('name', 'Unknown'),
                    'name': item.get('name') or item.get('scheme_name') or item.get('company_name', ''),
                    'sector': item.get('sector') or item.get('category') or item.get('type', 'Unknown'),
                    'units': float(item.get('quantity', 0) or item.get('units', 0)),
                    'avg_price': float(item.get('avg_price', 0) or item.get('buy_price', 0) or item.get('purchase_price', 0)),
                    'current_price': float(item.get('current_price', 0) or item.get('ltp', 0) or item.get('nav', 0)),
                    'current_value': float(item.get('current_value', 0) or item.get('market_value', 0)),
                    'invested_value': float(item.get('invested', 0) or item.get('investment_value', 0)),
                    'pnl_pct': float(item.get('returns_pct', 0) or item.get('pnl_percent', 0)),
                    'type': item.get('instrument_type') or item.get('asset_type') or 'unknown'
                }
                holdings.append(holding)
        
        return holdings


# Singleton instance for the app
_indmoney_connector = None

def get_connector(api_key: Optional[str] = None) -> IndMoneyConnector:
    """Get or create INDMONEY connector instance"""
    global _indmoney_connector
    if _indmoney_connector is None or api_key:
        _indmoney_connector = IndMoneyConnector(api_key=api_key)
    return _indmoney_connector
