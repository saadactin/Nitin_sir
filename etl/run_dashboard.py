#!/usr/bin/env python3
"""
Simple script to run the migration dashboard
"""

import sys
import os
from pathlib import Path

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

from dashboard_generator import main

if __name__ == "__main__":
    print("🚀 Starting Migration Dashboard...")
    exit(main()) 