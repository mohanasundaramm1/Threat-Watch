'use client';

import { useState, useEffect } from 'react';
import { Search, AlertCircle, CheckCircle } from 'lucide-react';
import type { ThreatDomain } from '@/types';
import { loadThreatData } from '@/lib/data';
import { getRiskLevelColor, getThreatTypeColor, formatDate } from '@/lib/utils';

export default function LookupPage() {
  const [query, setQuery] = useState('');
  const [result, setResult] = useState<ThreatDomain | null>(null);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const [allThreats, setAllThreats] = useState<ThreatDomain[]>([]);

  useEffect(() => {
    loadThreatData().then(setAllThreats);
  }, []);

  const handleSearch = () => {
    if (!query.trim()) return;

    setLoading(true);
    setSearched(true);

    const found = allThreats.find(t =>
      t.domain.toLowerCase() === query.toLowerCase().trim() ||
      t.url.toLowerCase().includes(query.toLowerCase().trim())
    );

    setTimeout(() => {
      setResult(found || null);
      setLoading(false);
    }, 300);
  };

  return (
    <div className="max-w-5xl mx-auto">
      <div className="mb-8">
        <h1 className="text-5xl font-black gradient-text-cyan mb-3">Domain Lookup</h1>
        <p className="text-slate-400 text-lg">Check if a domain is in our threat database</p>
      </div>

      {/* Search Bar */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex gap-4">
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyPress={(e) => e.key === 'Enter' && handleSearch()}
            placeholder="Enter domain or URL (e.g., example.com)"
            className="flex-1 px-5 py-4 bg-slate-800/50 border border-slate-700 rounded-xl focus:ring-2 focus:ring-cyan-500 focus:border-cyan-500 text-white placeholder-slate-500 font-medium"
          />
          <button
            onClick={handleSearch}
            disabled={loading || !query.trim()}
            className="px-8 py-4 bg-gradient-to-r from-cyan-500 to-blue-600 text-white rounded-xl font-bold hover:shadow-lg hover:shadow-cyan-500/50 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-3 transition-all glow-cyan"
          >
            <Search className="w-5 h-5" />
            Search
          </button>
        </div>
      </div>

      {/* Loading State */}
      {loading && (
        <div className="glass-card rounded-xl p-12 text-center card-gradient-border">
          <div className="animate-spin rounded-full h-16 w-16 border-b-4 border-cyan-500 mx-auto mb-4"></div>
          <p className="text-slate-400 text-lg font-medium">Searching database...</p>
        </div>
      )}

      {/* Results */}
      {!loading && searched && (
        <div className="glass-card rounded-xl p-6 card-gradient-border">
          {result ? (
            <div>
              {/* Threat Found */}
              <div className="flex items-start gap-4 mb-6 p-4 bg-red-500/10 border border-red-500/30 rounded-xl">
                <div className="p-4 bg-gradient-to-br from-red-500 to-red-600 rounded-xl glow-red">
                  <AlertCircle className="w-8 h-8 text-white" />
                </div>
                <div className="flex-1">
                  <h2 className="text-3xl font-black gradient-text-red mb-2">Threat Detected!</h2>
                  <p className="text-slate-400 font-medium">This domain is in our threat database</p>
                </div>
                <span className={`px-5 py-2.5 rounded-xl text-sm font-black shadow-lg ${getRiskLevelColor(result.riskLevel)}`}>
                  {result.riskLevel}
                </span>
              </div>

              <div className="space-y-5">
                <div className="grid grid-cols-2 gap-5">
                  <div className="p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
                    <label className="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-2 block">Domain</label>
                    <p className="text-xl font-bold text-white break-all">{result.domain}</p>
                  </div>
                  <div className="p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
                    <label className="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-2 block">Risk Score</label>
                    <div className="flex items-center gap-3">
                      <div className="flex-1 bg-slate-700 rounded-full h-3">
                        <div
                          className={`h-3 rounded-full shadow-lg ${result.riskScore >= 0.75 ? 'bg-gradient-to-r from-red-500 to-red-600' :
                              result.riskScore >= 0.50 ? 'bg-gradient-to-r from-orange-500 to-orange-600' :
                                result.riskScore >= 0.30 ? 'bg-gradient-to-r from-yellow-500 to-yellow-600' :
                                  'bg-gradient-to-r from-green-500 to-green-600'
                            }`}
                          style={{ width: `${result.riskScore * 100}%` }}
                        ></div>
                      </div>
                      <span className="text-xl font-black text-white">{result.riskScore.toFixed(3)}</span>
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-5">
                  <div className="p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
                    <label className="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-2 block">Threat Type</label>
                    <span className={`inline-block px-4 py-2 text-sm font-black rounded-xl shadow-lg ${getThreatTypeColor(result.threatType)}`}>
                      {result.threatType}
                    </span>
                  </div>
                  <div className="p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
                    <label className="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-2 block">Source</label>
                    <p className="text-xl font-bold text-white">{result.source}</p>
                  </div>
                </div>

                <div className="grid grid-cols-2 gap-5">
                  <div className="p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
                    <label className="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-2 block">Country</label>
                    <p className="text-xl font-bold text-white">{result.country || 'Unknown'}</p>
                  </div>
                  <div className="p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
                    <label className="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-2 block">Registrar</label>
                    <p className="text-xl font-bold text-white">{result.registrar || 'Unknown'}</p>
                  </div>
                </div>

                <div className="p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
                  <label className="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-2 block">Full URL</label>
                  <p className="text-sm text-slate-300 bg-slate-900/50 p-4 rounded-lg break-all font-mono border border-slate-700/50">{result.url}</p>
                </div>

                <div className="p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
                  <label className="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-2 block">First Seen</label>
                  <p className="text-xl font-bold text-white">{formatDate(result.firstSeen)}</p>
                </div>

                {result.domainAgeDays !== undefined && result.domainAgeDays >= 0 && (
                  <div className="p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
                    <label className="text-xs font-bold text-cyan-400 uppercase tracking-wider mb-2 block">Domain Age</label>
                    <p className="text-xl font-bold text-white">{result.domainAgeDays} days</p>
                  </div>
                )}
              </div>

              <div className="mt-6 p-5 bg-red-500/10 border border-red-500/30 rounded-xl">
                <h3 className="font-black text-red-400 mb-3 flex items-center gap-2 text-lg">
                  <AlertCircle className="w-5 h-5" />
                  Warning
                </h3>
                <p className="text-sm text-red-300 leading-relaxed">
                  This domain has been identified as potentially malicious. Exercise caution and do not visit
                  this site or download any content from it.
                </p>
              </div>
            </div>
          ) : (
            <div>
              {/* Not Found */}
              <div className="flex items-start gap-4 mb-6 p-4 bg-green-500/10 border border-green-500/30 rounded-xl">
                <div className="p-4 bg-gradient-to-br from-green-500 to-emerald-600 rounded-xl glow-green">
                  <CheckCircle className="w-8 h-8 text-white" />
                </div>
                <div className="flex-1">
                  <h2 className="text-3xl font-black text-green-400 mb-2">Domain Not Found</h2>
                  <p className="text-slate-400 font-medium">This domain is not in our threat database</p>
                </div>
              </div>

              <div className="p-5 bg-cyan-500/10 border border-cyan-500/30 rounded-xl">
                <h3 className="font-black text-cyan-400 mb-3 text-lg">ℹ️ Note</h3>
                <p className="text-sm text-cyan-300 leading-relaxed">
                  A domain not being in our database doesn&apos;t guarantee it&apos;s safe. Our database contains
                  {' '}{allThreats.length.toLocaleString()} known threats. Always exercise caution when visiting
                  unfamiliar websites.
                </p>
              </div>

              <div className="mt-5 p-4 bg-slate-800/30 rounded-xl border border-slate-700/50">
                <p className="text-sm text-slate-400">Searched for: <span className="font-mono font-bold text-white">{query}</span></p>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Examples */}
      {!searched && (
        <div className="glass-card rounded-xl p-6 card-gradient-border">
          <h3 className="font-bold text-white mb-4 text-lg">Try searching for:</h3>
          <div className="space-y-2">
            {allThreats.slice(0, 5).map((threat, idx) => (
              <button
                key={idx}
                onClick={() => {
                  setQuery(threat.domain);
                  setSearched(false);
                }}
                className="block w-full text-left px-5 py-3 bg-slate-800/40 hover:bg-slate-700/40 rounded-xl text-sm font-medium text-slate-300 hover:text-white border border-slate-700/50 hover:border-cyan-500/30 transition-all"
              >
                {threat.domain}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
