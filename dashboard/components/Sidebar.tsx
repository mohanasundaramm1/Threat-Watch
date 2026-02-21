'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Home, BarChart3, Search, Brain, Database, Shield, Activity } from 'lucide-react';

const navigation = [
  { name: 'Overview', href: '/', icon: Home },
  { name: 'Analytics', href: '/analytics', icon: BarChart3 },
  { name: 'Domain Lookup', href: '/lookup', icon: Search },
  { name: 'ML Model', href: '/model', icon: Brain },
  { name: 'Data Sources', href: '/sources', icon: Database },
  { name: 'About Project', href: '/about', icon: Shield },
];

export default function Sidebar() {
  const pathname = usePathname();

  return (
    <div className="w-72 bg-slate-950 border-r border-slate-800/50 h-screen fixed left-0 top-0 overflow-y-auto">
      <div className="p-6">
        {/* Logo Section */}
        <div className="flex items-center gap-3 mb-10 pb-6 border-b border-slate-800/50">
          <div className="relative">
            <div className="absolute inset-0 bg-gradient-to-br from-cyan-500 to-blue-600 rounded-xl blur-md opacity-75 glow-cyan"></div>
            <div className="relative p-3 bg-gradient-to-br from-cyan-500 to-blue-600 rounded-xl">
              <Shield className="w-7 h-7 text-white" />
            </div>
          </div>
          <div>
            <h1 className="text-2xl font-black gradient-text-cyan">ThreatWatch</h1>
            <div className="flex items-center gap-2 mt-1">
              <div className="w-2 h-2 bg-emerald-400 rounded-full pulse-dot"></div>
              <p className="text-xs text-emerald-400 font-bold">LIVE</p>
            </div>
          </div>
        </div>
        
        {/* Navigation */}
        <nav className="space-y-2">
          {navigation.map((item) => {
            const Icon = item.icon;
            const isActive = pathname === item.href;
            
            return (
              <Link
                key={item.name}
                href={item.href}
                className={`flex items-center gap-3 px-4 py-3.5 rounded-xl transition-all duration-200 relative group ${
                  isActive
                    ? 'bg-gradient-to-r from-cyan-500/20 to-blue-600/20 text-cyan-400 border border-cyan-500/30'
                    : 'text-slate-400 hover:text-slate-200 hover:bg-slate-800/40'
                }`}
              >
                {isActive && (
                  <div className="absolute left-0 top-1/2 -translate-y-1/2 w-1 h-8 bg-gradient-to-b from-cyan-500 to-blue-600 rounded-r-full"></div>
                )}
                <Icon className={`w-5 h-5 ${isActive ? 'text-cyan-400' : 'text-slate-500 group-hover:text-slate-300'}`} />
                <span className="font-bold">{item.name}</span>
              </Link>
            );
          })}
        </nav>

        {/* Stats Box */}
        <div className="mt-8 glass-card rounded-xl p-4">
          <div className="flex items-center gap-2 mb-3">
            <Activity className="w-4 h-4 text-emerald-400" />
            <p className="text-xs font-bold text-slate-400 uppercase">System Status</p>
          </div>
          <div className="space-y-2">
            <div className="flex justify-between items-center">
              <span className="text-xs text-slate-400">Threats Analyzed</span>
              <span className="text-xs font-bold text-white">69.6K</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-xs text-slate-400">ML Accuracy</span>
              <span className="text-xs font-bold text-emerald-400">99.08%</span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-xs text-slate-400">Uptime</span>
              <span className="text-xs font-bold text-cyan-400">100%</span>
            </div>
          </div>
        </div>

        {/* Footer */}
        <div className="mt-8 pt-6 border-t border-slate-800/50">
          <div className="text-xs space-y-2">
            <p className="text-slate-500 font-bold">Capstone Project 2025</p>
            <div className="space-y-1">
              <p className="text-slate-400">Nikita Desale (kf3051)</p>
              <p className="text-slate-400">Mohanasundaram Murugesan</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
