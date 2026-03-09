"""
Vercel serverless entry point — imports the Flask app from server.py.
Vercel's @vercel/python runtime expects an `app` object in this file.
"""
import sys
import os

# Add project root to Python path so imports resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from server import app
