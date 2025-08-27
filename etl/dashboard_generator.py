#!/usr/bin/env python3
"""
Dashboard Generator for SQL Server to PostgreSQL Migration
Generates beautiful, interactive dashboards with real-time metrics
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import webbrowser
from http.server import HTTPServer, SimpleHTTPRequestHandler
import threading
import time

# Add the current directory to Python path
sys.path.append(os.path.dirname(__file__))

from monitoring import MigrationMonitor

class DashboardServer:
    """Simple HTTP server to serve the dashboard"""
    
    def __init__(self, port=8080):
        self.port = port
        self.server = None
        
    def start(self, html_content):
        """Start the dashboard server"""
        # Create temporary HTML file
        dashboard_path = Path("migration_dashboard.html")
        with open(dashboard_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        # Start HTTP server in current directory
        self.server = HTTPServer(('localhost', self.port), SimpleHTTPRequestHandler)
        
        print(f"🚀 Dashboard server starting on http://localhost:{self.port}")
        print(f"📊 Dashboard file: {dashboard_path.absolute()}")
        
        # Open browser
        webbrowser.open(f'http://localhost:{self.port}/migration_dashboard.html')
        
        # Start server in background thread
        server_thread = threading.Thread(target=self.server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        
        return self.server
    
    def stop(self):
        """Stop the dashboard server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()

def generate_metrics_summary(monitor):
    """Generate a comprehensive metrics summary"""
    data = monitor.get_dashboard_data()
    
    summary = {
        'timestamp': datetime.now().isoformat(),
        'overall_health': 'GOOD',
        'critical_issues': 0,
        'warnings': 0,
        'recommendations': []
    }
    
    overall = data.get('overall', {})
    consistency_issues = data.get('consistency_issues', [])
    active_alerts = data.get('active_alerts', [])
    
    # Calculate health score
    total_syncs = overall.get('successful_syncs', 0) + overall.get('failed_syncs', 0)
    success_rate = (overall.get('successful_syncs', 0) / total_syncs * 100) if total_syncs > 0 else 0
    consistency_score = overall.get('avg_consistency_score', 0)
    
    # Determine overall health
    if success_rate < 90 or consistency_score < 95:
        summary['overall_health'] = 'CRITICAL'
    elif success_rate < 95 or consistency_score < 98:
        summary['overall_health'] = 'WARNING'
    
    # Count issues
    summary['critical_issues'] = len([a for a in active_alerts if a['severity'] == 'HIGH'])
    summary['warnings'] = len([a for a in active_alerts if a['severity'] == 'MEDIUM'])
    
    # Generate recommendations
    if success_rate < 95:
        summary['recommendations'].append(f"Sync success rate is {success_rate:.1f}%. Review failed syncs.")
    
    if consistency_score < 98:
        summary['recommendations'].append(f"Data consistency is {consistency_score:.1f}%. Investigate inconsistencies.")
    
    if len(consistency_issues) > 0:
        summary['recommendations'].append(f"{len(consistency_issues)} tables have data consistency issues.")
    
    if summary['critical_issues'] > 0:
        summary['recommendations'].append(f"{summary['critical_issues']} critical alerts need immediate attention.")
    
    return summary

def main():
    """Main dashboard generator function"""
    print("🔍 Initializing Migration Dashboard Generator...")
    
    try:
        # Initialize monitor
        monitor = MigrationMonitor()
        print("✅ Monitoring system initialized")
        
        # Generate dashboard
        print("📊 Generating dashboard...")
        html_content = monitor.generate_dashboard_report()
        
        # Generate summary
        summary = generate_metrics_summary(monitor)
        
        # Print summary
        print("\n" + "="*60)
        print("📈 MIGRATION DASHBOARD SUMMARY")
        print("="*60)
        print(f"🕒 Generated: {summary['timestamp']}")
        print(f"🏥 Overall Health: {summary['overall_health']}")
        print(f"🚨 Critical Issues: {summary['critical_issues']}")
        print(f"⚠️  Warnings: {summary['warnings']}")
        
        if summary['recommendations']:
            print("\n💡 Recommendations:")
            for rec in summary['recommendations']:
                print(f"   • {rec}")
        
        print("\n" + "="*60)
        
        # Save dashboard to file
        dashboard_path = Path("migration_dashboard.html")
        with open(dashboard_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"💾 Dashboard saved to: {dashboard_path.absolute()}")
        
        # Ask user if they want to start the server
        response = input("\n🌐 Start dashboard server? (y/n): ").lower().strip()
        
        if response in ['y', 'yes']:
            try:
                server = DashboardServer()
                server.start(html_content)
                
                print("\n🎉 Dashboard is now running!")
                print("📱 Open your browser to view the interactive dashboard")
                print("⏹️  Press Ctrl+C to stop the server")
                
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    print("\n🛑 Stopping dashboard server...")
                    server.stop()
                    print("✅ Dashboard server stopped")
            except Exception as e:
                print(f"\n⚠️  Could not start server: {e}")
                print("📂 Opening dashboard file directly in browser...")
                webbrowser.open(f'file://{dashboard_path.absolute()}')
        else:
            # Just open the file directly
            print("\n📂 Opening dashboard file directly in browser...")
            webbrowser.open(f'file://{dashboard_path.absolute()}')
        
        # Also save summary as JSON
        summary_path = Path("migration_summary.json")
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"📄 Summary saved to: {summary_path.absolute()}")
        
    except Exception as e:
        print(f"❌ Error generating dashboard: {e}")
        logging.error(f"Dashboard generation failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 