#!/usr/bin/env python3
"""
Start Real-time Migration Monitor
Simple script to launch the monitoring dashboard
"""

import sys
import os
from pathlib import Path

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

from real_time_monitor import start_monitor

if __name__ == "__main__":
    print("🚀 Starting Real-time Migration Monitor...")
    print("📊 This will start a web-based dashboard for monitoring your migration")
    print("🌐 The dashboard will be available at: http://localhost:5000")
    print("⏹️  Press Ctrl+C to stop the monitor")
    print()
    
    try:
        start_monitor(host='localhost', port=5000, debug=False)
    except KeyboardInterrupt:
        print("\n🛑 Monitor stopped by user")
    except Exception as e:
        print(f"\n❌ Error starting monitor: {e}")
        sys.exit(1) 