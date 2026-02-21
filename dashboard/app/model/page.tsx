'use client';

import { useEffect, useState } from 'react';
import { Brain, Award, TrendingUp, Zap } from 'lucide-react';
import { loadModelMeta } from '@/lib/data';
import type { ModelMeta } from '@/types';

export default function ModelPage() {
  const [meta, setMeta] = useState<ModelMeta | null>(null);

  useEffect(() => {
    async function init() {
      const data = await loadModelMeta();
      setMeta(data);
    }
    init();
  }, []);

  // Formatting helpers
  const rocAuc = meta ? (meta.metrics.roc_auc * 100).toFixed(2) + '%' : '99.08%';
  const prAuc = meta ? (meta.metrics.pr_auc * 100).toFixed(2) + '%' : '98.94%';
  const r1 = meta ? (meta.metrics.recall_at_1pct * 100).toFixed(1) + '%' : '94.0%';
  const samples = meta ? meta.n_rows.toLocaleString() : '17,805';
  const pos = meta ? meta.n_pos.toLocaleString() : '7,053';
  const neg = meta ? meta.n_neg.toLocaleString() : '10,752';

  let trainedDate = 'Nov 29, 2025';
  if (meta && meta.created_utc) {
    const d = new Date(meta.created_utc);
    trainedDate = d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-5xl font-black gradient-text-cyan mb-3">Machine Learning Model</h1>
        <p className="text-slate-400 text-lg">Production ML model details and performance metrics</p>
      </div>

      {/* Model Overview */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex items-center gap-4 mb-6 pb-4 border-b border-slate-700/50">
          <div className="p-4 bg-gradient-to-br from-purple-500 to-purple-600 rounded-xl shadow-lg">
            <Brain className="w-8 h-8 text-white" />
          </div>
          <div>
            <h2 className="text-3xl font-black text-white">LightGBM (Full Features)</h2>
            <p className="text-slate-400 font-medium">Production Model - Trained {trainedDate}</p>
          </div>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <div className="text-center p-6 bg-gradient-to-br from-purple-500/20 to-purple-600/20 rounded-xl border border-purple-500/30">
            <div className="text-5xl font-black text-purple-400 mb-2">{rocAuc}</div>
            <div className="text-sm text-slate-300 font-bold">ROC-AUC Score</div>
          </div>
          <div className="text-center p-6 bg-gradient-to-br from-blue-500/20 to-blue-600/20 rounded-xl border border-blue-500/30">
            <div className="text-5xl font-black text-blue-400 mb-2">{samples}</div>
            <div className="text-sm text-slate-300 font-bold">Training Samples</div>
          </div>
          <div className="text-center p-6 bg-gradient-to-br from-green-500/20 to-emerald-600/20 rounded-xl border border-green-500/30">
            <div className="text-5xl font-black text-green-400 mb-2">4,371</div>
            <div className="text-sm text-slate-300 font-bold">Features</div>
          </div>
          <div className="text-center p-6 bg-gradient-to-br from-orange-500/20 to-red-600/20 rounded-xl border border-orange-500/30 glow-red">
            <div className="text-5xl font-black text-orange-400 mb-2">{rocAuc}</div>
            <div className="text-sm text-slate-300 font-bold">Best Model (LGBM)</div>
          </div>
        </div>
      </div>

      {/* Model Comparison */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-gradient-to-br from-cyan-500 to-blue-600 rounded-xl">
            <Award className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white">Model Comparison</h2>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full">
            <thead className="bg-slate-800/50 border-b border-slate-700/50">
              <tr>
                <th className="px-6 py-4 text-left text-xs font-black text-cyan-400 uppercase tracking-wider">Model</th>
                <th className="px-6 py-4 text-left text-xs font-black text-cyan-400 uppercase tracking-wider">Features</th>
                <th className="px-6 py-4 text-left text-xs font-black text-cyan-400 uppercase tracking-wider">ROC-AUC</th>
                <th className="px-6 py-4 text-left text-xs font-black text-cyan-400 uppercase tracking-wider">PR-AUC</th>
                <th className="px-6 py-4 text-left text-xs font-black text-cyan-400 uppercase tracking-wider">Recall@1%FPR</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              <tr className="hover:bg-slate-800/30 transition-colors">
                <td className="px-6 py-4 font-bold text-white">LogReg (Lexical)</td>
                <td className="px-6 py-4 text-slate-400">Lexical only</td>
                <td className="px-6 py-4 text-green-400 font-black">91.83%</td>
                <td className="px-6 py-4 text-slate-300 font-semibold">87.73%</td>
                <td className="px-6 py-4 text-slate-300 font-semibold">26.7%</td>
              </tr>
              <tr className="hover:bg-slate-800/30 transition-colors">
                <td className="px-6 py-4 font-bold text-white">LogReg (Full)</td>
                <td className="px-6 py-4 text-slate-400">Lex + DNS + WHOIS</td>
                <td className="px-6 py-4 text-green-400 font-black">91.48%</td>
                <td className="px-6 py-4 text-slate-300 font-semibold">86.29%</td>
                <td className="px-6 py-4 text-slate-300 font-semibold">23.5%</td>
              </tr>
              <tr className="hover:bg-slate-800/30 transition-colors">
                <td className="px-6 py-4 font-bold text-white">LightGBM (Lexical)</td>
                <td className="px-6 py-4 text-slate-400">Lexical only</td>
                <td className="px-6 py-4 text-green-400 font-black">97.06%</td>
                <td className="px-6 py-4 text-slate-300 font-semibold">97.29%</td>
                <td className="px-6 py-4 text-slate-300 font-semibold">90.4%</td>
              </tr>
              <tr className="bg-purple-500/10 hover:bg-purple-500/15 transition-colors border-l-4 border-purple-500">
                <td className="px-6 py-4 font-bold text-white flex items-center gap-2">
                  LightGBM (Full)
                  <span className="text-xs bg-purple-500/30 text-purple-300 px-2 py-1 rounded-md font-black">BEST / DEPLOYED</span>
                </td>
                <td className="px-6 py-4 text-slate-400">Lex + DNS + WHOIS + MISP</td>
                <td className="px-6 py-4 text-purple-400 font-black text-lg">{rocAuc}</td>
                <td className="px-6 py-4 text-purple-300 font-bold">{prAuc}</td>
                <td className="px-6 py-4 text-purple-300 font-bold">{r1}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p className="text-sm text-slate-500 mt-4 font-medium">
          <span className="text-cyan-400">●</span> Currently deployed model &nbsp;&nbsp;
          <span className="text-purple-400">●</span> Best performing model
        </p>
      </div>

      {/* Feature Categories */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-gradient-to-br from-orange-500 to-red-600 rounded-xl">
            <TrendingUp className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white">Feature Categories</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="p-5 bg-slate-800/30 rounded-xl border border-slate-700/50 hover:border-purple-500/30 transition-all">
            <h3 className="font-black text-white mb-4 text-lg">Lexical Features</h3>
            <ul className="text-sm text-slate-400 space-y-2 font-medium">
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> Domain length & entropy</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> Character composition</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> Digit/hyphen ratios</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> TLD analysis</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> Subdomain count</li>
            </ul>
          </div>
          <div className="p-5 bg-slate-800/30 rounded-xl border border-slate-700/50 hover:border-blue-500/30 transition-all">
            <h3 className="font-black text-white mb-4 text-lg">DNS/Geo Features</h3>
            <ul className="text-sm text-slate-400 space-y-2 font-medium">
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> IP geolocation</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> ASN information</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> Country & region</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> ISP/hosting provider</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> Proxy detection</li>
            </ul>
          </div>
          <div className="p-5 bg-slate-800/30 rounded-xl border border-slate-700/50 hover:border-green-500/30 transition-all">
            <h3 className="font-black text-white mb-4 text-lg">MISP OSINT Features</h3>
            <ul className="text-sm text-slate-400 space-y-2 font-medium">
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> Known malicious domains</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> Phishing hostnames</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> Malware URLs</li>
              <li className="flex items-center gap-2"><span className="text-cyan-500">▸</span> CIRCL OSINT Feed (8000+/day)</li>
            </ul>
          </div>
        </div>
      </div>

      {/* Training Details */}
      <div className="glass-card rounded-xl p-6 card-gradient-border">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-gradient-to-br from-emerald-500 to-green-600 rounded-xl glow-green">
            <Zap className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white">Training Details</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="p-5 bg-slate-800/30 rounded-xl border border-slate-700/50">
            <h3 className="font-black text-cyan-400 mb-4 text-lg">Dataset</h3>
            <div className="space-y-3 text-sm">
              <div className="flex justify-between items-center">
                <span className="text-slate-400 font-medium">Total Samples:</span>
                <span className="font-black text-white">{samples}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-400 font-medium">Positive (Malicious):</span>
                <span className="font-black text-red-400">{pos}</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-400 font-medium">Negative (Benign):</span>
                <span className="font-black text-green-400">{neg}</span>
              </div>
            </div>
          </div>
          <div className="p-5 bg-slate-800/30 rounded-xl border border-slate-700/50">
            <h3 className="font-black text-cyan-400 mb-4 text-lg">Data Coverage</h3>
            <div className="space-y-3 text-sm">
              <div className="flex justify-between items-center">
                <span className="text-slate-400 font-medium">DNS/Geo Coverage:</span>
                <span className="font-black text-white">85.1%</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-400 font-medium">WHOIS Coverage:</span>
                <span className="font-black text-white">100%</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-400 font-medium">Date Range (Malicious):</span>
                <span className="font-black text-white text-xs">Nov 17-26, 2025</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-400 font-medium">Date Range (Benign):</span>
                <span className="font-black text-white text-xs">Oct 28-31, 2025</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
