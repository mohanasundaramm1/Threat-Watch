'use client';

import { useEffect, useState } from 'react';
import StatCard from '@/components/StatCard';
import { Shield, AlertTriangle, TrendingUp, Database } from 'lucide-react';
import type { ThreatDomain, DashboardStats } from '@/types';
import { loadHighRiskThreats, loadStatistics } from '@/lib/data';
import { formatDate, getRiskLevelColor, getThreatTypeColor, formatNumber } from '@/lib/utils';

export default function DashboardPage() {
  const [threats, setThreats] = useState<ThreatDomain[]>([]);
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function loadData() {
      const [threatsData, statsData] = await Promise.all([
        loadHighRiskThreats(),
        loadStatistics()
      ]);
      setThreats(threatsData);
      setStats(statsData);
      setLoading(false);
    }
    loadData();
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="text-center">
          <div className="animate-spin rounded-full h-16 w-16 border-b-4 border-cyan-500 mx-auto mb-4"></div>
          <p className="text-slate-400 text-lg font-medium">Loading threat intelligence data...</p>
        </div>
      </div>
    );
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-5xl font-black mb-3 gradient-text-cyan">
          Threat Intelligence Dashboard
        </h1>
        <p className="text-slate-400 text-lg">Real-time monitoring and analysis of malicious domains</p>
      </div>

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <StatCard
          title="Total Threats Analyzed"
          value={formatNumber(stats?.totalSamples || 0)}
          icon={Database}
          description="Processed domains"
          colorClass="from-cyan-500 to-blue-600"
          glowColor="cyan"
        />
        <StatCard
          title="Malicious Domains"
          value={formatNumber(stats?.maliciousCount || 0)}
          icon={AlertTriangle}
          description={`${((stats?.maliciousCount || 0) / (stats?.totalSamples || 1) * 100).toFixed(1)}% of total`}
          colorClass="from-red-500 to-red-600"
          glowColor="red"
        />
        <StatCard
          title="High Risk Threats"
          value={formatNumber(stats?.highRiskCount || 0)}
          icon={Shield}
          description="Critical attention required"
          colorClass="from-orange-500 to-orange-600"
        />
        <StatCard
          title="Average Risk Score"
          value={(stats?.averageRiskScore || 0).toFixed(3)}
          icon={TrendingUp}
          description="ML model confidence"
          colorClass="from-purple-500 to-purple-600"
          glowColor="purple"
        />
      </div>

      {/* Risk Distribution */}
      {stats && (
        <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
          <h2 className="text-2xl font-bold mb-6 gradient-text-cyan">
            Risk Level Distribution
          </h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            {Object.entries(stats.riskDistribution).map(([level, count]) => {
              const getBgColor = (lvl: string) => {
                switch (lvl) {
                  case 'CRITICAL': return 'from-red-500/20 to-red-600/20 border-red-500/30';
                  case 'HIGH': return 'from-orange-500/20 to-orange-600/20 border-orange-500/30';
                  case 'MEDIUM': return 'from-yellow-500/20 to-yellow-600/20 border-yellow-500/30';
                  case 'LOW': return 'from-green-500/20 to-green-600/20 border-green-500/30';
                  default: return 'from-slate-500/20 to-slate-600/20 border-slate-500/30';
                }
              };

              return (
                <div key={level} className={`text-center p-6 bg-gradient-to-br ${getBgColor(level)} rounded-xl border backdrop-blur-sm hover:shadow-xl transition-all duration-300`}>
                  <div className={`inline-block px-4 py-2 rounded-full text-sm font-bold mb-3 shadow-lg ${getRiskLevelColor(level)}`}>
                    {level}
                  </div>
                  <div className="text-4xl font-black text-white mb-2">{formatNumber(count)}</div>
                  <div className="text-sm font-semibold text-slate-400">
                    {((count / stats.totalSamples) * 100).toFixed(1)}%
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {/* Recent Threats Table */}
      <div className="glass-card rounded-xl overflow-hidden card-gradient-border">
        <div className="p-6 border-b border-slate-700/50 bg-slate-900/50">
          <h2 className="text-2xl font-bold gradient-text-cyan">
            Recent High-Risk Threats
          </h2>
          <p className="text-sm text-slate-400 mt-2 font-medium">Latest detected malicious domains</p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-slate-900/50 border-b border-slate-700/50">
              <tr>
                <th className="px-6 py-4 text-left text-xs font-bold text-cyan-400 uppercase tracking-wider">Domain</th>
                <th className="px-6 py-4 text-left text-xs font-bold text-cyan-400 uppercase tracking-wider">Risk Score</th>
                <th className="px-6 py-4 text-left text-xs font-bold text-cyan-400 uppercase tracking-wider">Risk Level</th>
                <th className="px-6 py-4 text-left text-xs font-bold text-cyan-400 uppercase tracking-wider">Threat Type</th>
                <th className="px-6 py-4 text-left text-xs font-bold text-cyan-400 uppercase tracking-wider">Country</th>
                <th className="px-6 py-4 text-left text-xs font-bold text-cyan-400 uppercase tracking-wider">Source</th>
                <th className="px-6 py-4 text-left text-xs font-bold text-cyan-400 uppercase tracking-wider">First Seen</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {threats.slice(0, 20).map((threat, idx) => (
                <tr key={idx} className="hover:bg-cyan-500/5 transition-colors duration-150">
                  <td className="px-6 py-4 text-sm">
                    <div className="font-bold text-white truncate max-w-xs" title={threat.domain}>
                      {threat.domain}
                    </div>
                    <div className="text-slate-500 truncate max-w-xs text-xs font-mono" title={threat.url}>
                      {threat.url}
                    </div>
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex items-center gap-3">
                      <div className="w-24 bg-slate-800 rounded-full h-2.5">
                        <div
                          className={`h-2.5 rounded-full ${threat.riskScore >= 0.75 ? 'bg-gradient-to-r from-red-500 to-red-600' :
                            threat.riskScore >= 0.50 ? 'bg-gradient-to-r from-orange-500 to-orange-600' :
                              threat.riskScore >= 0.30 ? 'bg-gradient-to-r from-yellow-500 to-yellow-600' :
                                'bg-gradient-to-r from-green-500 to-green-600'
                            }`}
                          style={{ width: `${threat.riskScore * 100}%` }}
                        ></div>
                      </div>
                      <span className="text-sm font-bold text-white">{threat.riskScore.toFixed(3)}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4">
                    <span className={`inline-block px-3 py-1.5 text-xs font-bold rounded-lg shadow-md ${getRiskLevelColor(threat.riskLevel)}`}>
                      {threat.riskLevel}
                    </span>
                  </td>
                  <td className="px-6 py-4">
                    <span className={`inline-block px-3 py-1.5 text-xs font-bold rounded-lg shadow-md ${getThreatTypeColor(threat.threatType)}`}>
                      {threat.threatType}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-sm font-medium text-slate-300">
                    {threat.country || 'Unknown'}
                  </td>
                  <td className="px-6 py-4 text-sm font-medium text-slate-300">
                    {threat.source}
                  </td>
                  <td className="px-6 py-4 text-sm text-slate-400">
                    {formatDate(threat.firstSeen)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
