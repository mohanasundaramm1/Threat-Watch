'use client';

import { useEffect, useState } from 'react';
import { BarChart3, PieChart, Globe, TrendingUp } from 'lucide-react';
import dynamic from 'next/dynamic';

// Import the map dynamically with SSR disabled to prevent hydration errors from D3/react-simple-maps bounding boxes
const WorldMap = dynamic(() => import('@/components/WorldMap'), { ssr: false });
import { loadStatistics } from '@/lib/data';
import type { DashboardStats } from '@/types';
import { formatNumber } from '@/lib/utils';

export default function AnalyticsPage() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadStatistics().then(data => {
      setStats(data);
      setLoading(false);
    });
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="animate-spin rounded-full h-16 w-16 border-b-4 border-cyan-500"></div>
      </div>
    );
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-5xl font-black gradient-text-cyan mb-3">Analytics Dashboard</h1>
        <p className="text-slate-400 text-lg">Comprehensive threat intelligence analytics</p>
      </div>

      {stats && (
        <>
          {/* Threat Type Distribution */}
          <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
            <div className="flex items-center gap-3 mb-6">
              <div className="p-3 bg-gradient-to-br from-purple-500 to-purple-600 rounded-xl glow-cyan">
                <PieChart className="w-6 h-6 text-white" />
              </div>
              <h2 className="text-2xl font-bold text-white">Threat Type Distribution</h2>
            </div>
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
              {Object.entries(stats.threatTypeDistribution).map(([type, count]) => {
                const percentage = ((count / stats.totalSamples) * 100).toFixed(1);
                return (
                  <div key={type} className="text-center p-5 bg-slate-800/40 rounded-xl border border-slate-700/50 hover:border-cyan-500/30 transition-all">
                    <div className="text-4xl font-black text-white mb-2">
                      {formatNumber(count)}
                    </div>
                    <div className="text-sm font-bold text-cyan-400 capitalize mb-2">
                      {type}
                    </div>
                    <div className="text-xs text-slate-400 font-semibold">
                      {percentage}%
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Source Distribution */}
          <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
            <div className="flex items-center gap-3 mb-6">
              <div className="p-3 bg-gradient-to-br from-blue-500 to-blue-600 rounded-xl glow-cyan">
                <BarChart3 className="w-6 h-6 text-white" />
              </div>
              <h2 className="text-2xl font-bold text-white">Data Sources</h2>
            </div>
            <div className="space-y-4">
              {Object.entries(stats.sourceDistribution).map(([source, count]) => {
                const percentage = (count / stats.totalSamples) * 100;
                return (
                  <div key={source} className="space-y-2">
                    <div className="flex justify-between items-center">
                      <span className="font-bold text-white">{source}</span>
                      <span className="text-sm text-slate-400 font-semibold">
                        {formatNumber(count)} ({percentage.toFixed(1)}%)
                      </span>
                    </div>
                    <div className="w-full bg-slate-800 rounded-full h-3">
                      <div
                        className="bg-gradient-to-r from-cyan-500 to-blue-600 h-3 rounded-full shadow-lg"
                        style={{ width: `${percentage}%` }}
                      ></div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Geographic Distribution */}
          {stats.topCountries && (
            <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
              <div className="flex items-center gap-3 mb-6">
                <div className="p-3 bg-gradient-to-br from-green-500 to-emerald-600 rounded-xl glow-green">
                  <Globe className="w-6 h-6 text-white" />
                </div>
                <h2 className="text-2xl font-bold text-white">Top Countries</h2>
              </div>
              <div className="mt-4">
                <WorldMap data={stats.topCountries} />
              </div>
            </div>
          )}

          {/* Top Registrars */}
          {stats.topRegistrars && (
            <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
              <div className="flex items-center gap-3 mb-6">
                <div className="p-3 bg-gradient-to-br from-orange-500 to-red-600 rounded-xl glow-red">
                  <TrendingUp className="w-6 h-6 text-white" />
                </div>
                <h2 className="text-2xl font-bold text-white">Top Registrars</h2>
              </div>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {Object.entries(stats.topRegistrars).slice(0, 10).map(([registrar, count], idx) => (
                  <div key={registrar} className="flex items-center gap-4 p-4 bg-slate-800/40 rounded-xl border border-slate-700/50 hover:border-orange-500/30 transition-all">
                    <div className="text-2xl font-black text-slate-600">#{idx + 1}</div>
                    <div className="flex-1">
                      <div className="font-bold text-white truncate">{registrar}</div>
                      <div className="text-sm text-slate-400 font-medium">{formatNumber(count)} domains</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Summary Stats */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div className="glass-card rounded-xl p-6 card-gradient-border">
              <h3 className="text-sm font-bold text-cyan-400 mb-2 uppercase tracking-wide">Total Samples</h3>
              <p className="text-4xl font-black text-white">{formatNumber(stats.totalSamples)}</p>
            </div>
            <div className="glass-card rounded-xl p-6 card-gradient-border">
              <h3 className="text-sm font-bold text-cyan-400 mb-2 uppercase tracking-wide">Average Risk Score</h3>
              <p className="text-4xl font-black text-white">{stats.averageRiskScore.toFixed(3)}</p>
            </div>
            <div className="glass-card rounded-xl p-6 card-gradient-border">
              <h3 className="text-sm font-bold text-cyan-400 mb-2 uppercase tracking-wide">High Risk Count</h3>
              <p className="text-4xl font-black bg-gradient-to-r from-red-500 to-red-600 bg-clip-text text-transparent">{formatNumber(stats.highRiskCount)}</p>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
