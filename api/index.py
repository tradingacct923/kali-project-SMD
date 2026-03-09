"""
Vercel serverless entry point.
Vercel's @vercel/python runtime looks for a WSGI `app` variable.
"""
import sys
import os

# Add project root to Python path so all sibling modules resolve
_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
sys.path.insert(0, _root)

# Import the Flask WSGI app
from server import app

# Vercel handler — expose the WSGI app
app = app
