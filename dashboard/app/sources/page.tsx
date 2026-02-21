'use client';

import { Database, Shield, Globe, Lock } from 'lucide-react';

export default function SourcesPage() {
  return (
    <div>
      <div className="mb-8">
        <h1 className="text-5xl font-black gradient-text-cyan mb-3">Data Sources</h1>
        <p className="text-slate-400 text-lg">Threat intelligence feeds and enrichment sources</p>
      </div>

      {/* Primary Threat Feeds */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-gradient-to-br from-red-500 to-red-600 rounded-xl glow-red">
            <Shield className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white">Primary Threat Intelligence Feeds</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="p-6 bg-slate-800/30 border border-slate-700/50 rounded-xl hover:border-red-500/30 transition-all">
            <h3 className="text-xl font-black text-white mb-3">URLhaus</h3>
            <p className="text-sm text-slate-400 mb-5 leading-relaxed">
              abuse.ch URLhaus database - malware distribution URLs
            </p>
            <div className="space-y-2.5 text-sm">
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Type:</span>
                <span className="font-bold text-red-400">Malware Download URLs</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Update Frequency:</span>
                <span className="font-bold text-white">Daily</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Coverage:</span>
                <span className="font-bold text-white">Global</span>
              </div>
            </div>
          </div>

          <div className="p-6 bg-slate-800/30 border border-slate-700/50 rounded-xl hover:border-purple-500/30 transition-all">
            <h3 className="text-xl font-black text-white mb-3">OpenPhish</h3>
            <p className="text-sm text-slate-400 mb-5 leading-relaxed">
              OpenPhish community feed - active phishing sites
            </p>
            <div className="space-y-2.5 text-sm">
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Type:</span>
                <span className="font-bold text-purple-400">Phishing URLs</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Update Frequency:</span>
                <span className="font-bold text-white">Daily</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Coverage:</span>
                <span className="font-bold text-white">Global</span>
              </div>
            </div>
          </div>

          <div className="p-6 bg-slate-800/30 border border-slate-700/50 rounded-xl hover:border-blue-500/30 transition-all">
            <h3 className="text-xl font-black text-white mb-3">MISP</h3>
            <p className="text-sm text-slate-400 mb-5 leading-relaxed">
              Malware Information Sharing Platform - threat indicators
            </p>
            <div className="space-y-2.5 text-sm">
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Type:</span>
                <span className="font-bold text-blue-400">IOCs & Campaigns</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Update Frequency:</span>
                <span className="font-bold text-white">Real-time</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Coverage:</span>
                <span className="font-bold text-white">Community Shared</span>
              </div>
            </div>
          </div>

          <div className="p-6 bg-slate-800/30 border border-slate-700/50 rounded-xl hover:border-green-500/30 transition-all">
            <h3 className="text-xl font-black text-white mb-3">Top-1M Benign</h3>
            <p className="text-sm text-slate-400 mb-5 leading-relaxed">
              Top 1 million legitimate domains for training balance
            </p>
            <div className="space-y-2.5 text-sm">
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Type:</span>
                <span className="font-bold text-green-400">Benign Domains</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Update Frequency:</span>
                <span className="font-bold text-white">Monthly</span>
              </div>
              <div className="flex justify-between items-center">
                <span className="text-slate-500 font-medium">Coverage:</span>
                <span className="font-bold text-white">Top Sites</span>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Enrichment Sources */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-gradient-to-br from-cyan-500 to-blue-600 rounded-xl glow-cyan">
            <Database className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white">Enrichment Sources</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <div className="p-6 bg-slate-800/30 border border-slate-700/50 rounded-xl hover:border-cyan-500/30 transition-all text-center">
            <div className="inline-flex p-4 bg-gradient-to-br from-blue-500 to-blue-600 rounded-xl mb-4">
              <Globe className="w-8 h-8 text-white" />
            </div>
            <h3 className="text-lg font-black text-white mb-3">DNS Resolution</h3>
            <p className="text-sm text-slate-400 mb-4 leading-relaxed">
              Real-time DNS lookups for domain-to-IP mapping
            </p>
            <div className="text-xs text-slate-500 font-medium">
              Provides: IP addresses, DNS records
            </div>
          </div>

          <div className="p-6 bg-slate-800/30 border border-slate-700/50 rounded-xl hover:border-green-500/30 transition-all text-center">
            <div className="inline-flex p-4 bg-gradient-to-br from-green-500 to-emerald-600 rounded-xl mb-4">
              <Globe className="w-8 h-8 text-white" />
            </div>
            <h3 className="text-lg font-black text-white mb-3">IP Geolocation</h3>
            <p className="text-sm text-slate-400 mb-4 leading-relaxed">
              MaxMind GeoIP database for geographic mapping
            </p>
            <div className="text-xs text-slate-500 font-medium">
              Provides: Country, city, ASN, ISP
            </div>
          </div>

          <div className="p-6 bg-slate-800/30 border border-slate-700/50 rounded-xl hover:border-purple-500/30 transition-all text-center">
            <div className="inline-flex p-4 bg-gradient-to-br from-purple-500 to-purple-600 rounded-xl mb-4">
              <Lock className="w-8 h-8 text-white" />
            </div>
            <h3 className="text-lg font-black text-white mb-3">WHOIS/RDAP</h3>
            <p className="text-sm text-slate-400 mb-4 leading-relaxed">
              Domain registration information queries
            </p>
            <div className="text-xs text-slate-500 font-medium">
              Provides: Registrar, age, privacy status
            </div>
          </div>
        </div>
      </div>

      {/* Data Pipeline */}
      <div className="glass-card rounded-xl p-6 card-gradient-border">
        <h2 className="text-2xl font-bold text-white mb-6">Data Processing Pipeline</h2>
        <div className="space-y-5">
          <div className="flex items-start gap-5 p-4 bg-slate-800/30 rounded-xl border border-slate-700/50 hover:border-blue-500/30 transition-all">
            <div className="w-14 h-14 bg-gradient-to-br from-blue-500 to-blue-600 rounded-full flex items-center justify-center text-white font-black text-xl shadow-lg">
              1
            </div>
            <div className="flex-1">
              <h3 className="font-black text-white text-lg mb-2">Bronze Layer - Raw Ingestion</h3>
              <p className="text-sm text-slate-400 leading-relaxed">
                Daily ingestion from URLhaus, OpenPhish, and MISP feeds. Raw data stored with minimal transformation.
              </p>
            </div>
          </div>

          <div className="flex items-start gap-5 p-4 bg-slate-800/30 rounded-xl border border-slate-700/50 hover:border-green-500/30 transition-all">
            <div className="w-14 h-14 bg-gradient-to-br from-green-500 to-emerald-600 rounded-full flex items-center justify-center text-white font-black text-xl shadow-lg">
              2
            </div>
            <div className="flex-1">
              <h3 className="font-black text-white text-lg mb-2">Silver Layer - Data Cleaning & Enrichment</h3>
              <p className="text-sm text-slate-400 leading-relaxed">
                Deduplication, normalization, DNS resolution, IP geolocation, and WHOIS lookups applied.
              </p>
            </div>
          </div>

          <div className="flex items-start gap-5 p-4 bg-slate-800/30 rounded-xl border border-slate-700/50 hover:border-purple-500/30 transition-all">
            <div className="w-14 h-14 bg-gradient-to-br from-purple-500 to-purple-600 rounded-full flex items-center justify-center text-white font-black text-xl shadow-lg">
              3
            </div>
            <div className="flex-1">
              <h3 className="font-black text-white text-lg mb-2">Gold Layer - ML Feature Engineering</h3>
              <p className="text-sm text-slate-400 leading-relaxed">
                Extract 4,371 features including lexical, DNS/Geo, and WHOIS attributes for ML models.
              </p>
            </div>
          </div>

          <div className="flex items-start gap-5 p-4 bg-slate-800/30 rounded-xl border border-slate-700/50 hover:border-orange-500/30 transition-all">
            <div className="w-14 h-14 bg-gradient-to-br from-orange-500 to-red-600 rounded-full flex items-center justify-center text-white font-black text-xl shadow-lg glow-red">
              4
            </div>
            <div className="flex-1">
              <h3 className="font-black text-white text-lg mb-2">ML Prediction & Risk Scoring</h3>
              <p className="text-sm text-slate-400 leading-relaxed">
                Apply trained models (91-99% AUC) to generate risk scores and threat classifications.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
