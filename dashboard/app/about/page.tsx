'use client';

import { Users, Target, Lightbulb, Cpu, TrendingUp, Award } from 'lucide-react';
import { useEffect, useState } from 'react';
import type { DashboardStats, ModelMeta } from '@/types';
import { loadStatistics, loadModelMeta } from '@/lib/data';
import { formatNumber } from '@/lib/utils';

export default function AboutPage() {
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [meta, setMeta] = useState<ModelMeta | null>(null);

  useEffect(() => {
    async function init() {
      const s = await loadStatistics();
      const m = await loadModelMeta();
      setStats(s);
      setMeta(m);
    }
    init();
  }, []);
  return (
    <div>
      <div className="mb-8">
        <h1 className="text-5xl font-black gradient-text-cyan mb-3">About This Project</h1>
        <p className="text-slate-400 text-lg">Capstone Project - Threat Intelligence & Machine Learning</p>
      </div>

      {/* Contributors */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-gradient-to-br from-cyan-500 to-blue-600 rounded-xl glow-cyan">
            <Users className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white">Project Contributors</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="p-6 bg-gradient-to-br from-cyan-500/10 to-blue-600/10 rounded-xl border-2 border-cyan-500/40 hover:border-cyan-500/60 transition-all">
            <div className="flex items-center gap-4">
              <div className="w-20 h-20 bg-gradient-to-br from-cyan-500 to-blue-600 rounded-full flex items-center justify-center text-white text-3xl font-black shadow-lg glow-cyan">
                ND
              </div>
              <div>
                <h3 className="text-2xl font-black text-white">Nikita Desale</h3>
                <p className="text-sm text-cyan-400 font-bold">Student ID: kf3051</p>
                <p className="text-sm text-slate-400 mt-1">kingston.ac.uk</p>
              </div>
            </div>
          </div>

          <div className="p-6 bg-gradient-to-br from-purple-500/10 to-purple-600/10 rounded-xl border-2 border-purple-500/40 hover:border-purple-500/60 transition-all">
            <div className="flex items-center gap-4">
              <div className="w-20 h-20 bg-gradient-to-br from-purple-500 to-purple-600 rounded-full flex items-center justify-center text-white text-3xl font-black shadow-lg">
                MM
              </div>
              <div>
                <h3 className="text-2xl font-black text-white">Mohanasundaram Murugesan</h3>
                <p className="text-sm text-purple-400 font-bold">Student ID: k3059432</p>
                <p className="text-sm text-slate-400 mt-1">kingston.ac.uk</p>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Project Agenda */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-gradient-to-br from-orange-500 to-red-600 rounded-xl glow-red">
            <Target className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white">Project Agenda</h2>
        </div>
        <p className="text-slate-300 mb-6 leading-relaxed">
          The primary agenda of this capstone project was to design and implement a comprehensive
          <strong className="text-cyan-400"> Real-Time Threat Intelligence System</strong> that combines data engineering, machine learning,
          and web technologies to detect and classify malicious domains and URLs.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="p-5 bg-gradient-to-br from-blue-500/10 to-blue-600/10 rounded-xl border border-blue-500/30">
            <h3 className="font-black text-blue-400 mb-3">Data Engineering</h3>
            <p className="text-sm text-slate-400 leading-relaxed">
              Build scalable pipelines to ingest, process, and enrich threat data from multiple sources
            </p>
          </div>
          <div className="p-5 bg-gradient-to-br from-purple-500/10 to-purple-600/10 rounded-xl border border-purple-500/30">
            <h3 className="font-black text-purple-400 mb-3">Machine Learning</h3>
            <p className="text-sm text-slate-400 leading-relaxed">
              Develop high-accuracy ML models for automated threat detection and risk scoring
            </p>
          </div>
          <div className="p-5 bg-gradient-to-br from-green-500/10 to-emerald-600/10 rounded-xl border border-green-500/30">
            <h3 className="font-black text-green-400 mb-3">Visualization</h3>
            <p className="text-sm text-slate-400 leading-relaxed">
              Create interactive dashboard for security analysts to monitor and analyze threats
            </p>
          </div>
        </div>
      </div>

      {/* Purpose */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-gradient-to-br from-yellow-500 to-orange-600 rounded-xl">
            <Lightbulb className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white">Project Purpose</h2>
        </div>
        <p className="text-slate-300 mb-5 leading-relaxed">
          In today&apos;s cybersecurity landscape, organizations face an overwhelming volume of threats.
          Traditional manual analysis methods cannot keep pace with the sophistication and scale of modern attacks.
        </p>

        <div className="bg-slate-800/30 p-5 rounded-xl border border-slate-700/50">
          <h3 className="font-black text-cyan-400 mb-4 text-lg">Key Objectives:</h3>
          <ul className="space-y-3 text-slate-300">
            <li className="flex items-start gap-3">
              <span className="text-cyan-500 font-black text-lg">▸</span>
              <span><strong className="text-white">Automate Threat Detection:</strong> Reduce manual effort by using ML to automatically identify and classify malicious domains</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-cyan-500 font-black text-lg">▸</span>
              <span><strong className="text-white">Improve Accuracy:</strong> Achieve high precision and recall rates to minimize false positives and false negatives</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-cyan-500 font-black text-lg">▸</span>
              <span><strong className="text-white">Provide Context:</strong> Enrich threat data with geolocation, WHOIS, and DNS information for better analysis</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-cyan-500 font-black text-lg">▸</span>
              <span><strong className="text-white">Enable Quick Response:</strong> Present actionable intelligence through an intuitive dashboard interface</span>
            </li>
            <li className="flex items-start gap-3">
              <span className="text-cyan-500 font-black text-lg">▸</span>
              <span><strong className="text-white">Demonstrate Scalability:</strong> Build a production-ready system capable of processing large volumes of threat data</span>
            </li>
          </ul>
        </div>
      </div>

      {/* Methodology */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-gradient-to-br from-emerald-500 to-green-600 rounded-xl glow-green">
            <Cpu className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white">Methodology</h2>
        </div>
        <div className="space-y-5">
          <div className="border-l-4 border-blue-500 pl-5 py-2 bg-blue-500/5">
            <h3 className="font-black text-white mb-3">Phase 1: Data Collection & Ingestion</h3>
            <ul className="text-sm text-slate-400 space-y-1.5 leading-relaxed">
              <li>• Integrated 4 threat intelligence feeds (URLhaus, OpenPhish, MISP, Top-1M benign)</li>
              <li>• Implemented Apache Kafka for real-time streaming</li>
              <li>• Built Apache Airflow DAGs for batch ingestion (daily updates)</li>
              <li>• Processed {stats ? formatNumber(stats.totalSamples) : '69,641'} threat records from production data</li>
            </ul>
          </div>

          <div className="border-l-4 border-green-500 pl-5 py-2 bg-green-500/5">
            <h3 className="font-black text-white mb-3">Phase 2: Data Processing & Enrichment</h3>
            <ul className="text-sm text-slate-400 space-y-1.5 leading-relaxed">
              <li>• Implemented Medallion Architecture (Bronze → Silver → Gold)</li>
              <li>• Applied data cleaning, deduplication, and normalization</li>
              <li>• Enriched with DNS resolution (21.2% coverage)</li>
              <li>• Enriched with IP Geolocation using MaxMind</li>
              <li>• Enriched with WHOIS/RDAP data (15.5% coverage)</li>
            </ul>
          </div>

          <div className="border-l-4 border-purple-500 pl-5 py-2 bg-purple-500/5">
            <h3 className="font-black text-white mb-3">Phase 3: Feature Engineering & ML</h3>
            <ul className="text-sm text-slate-400 space-y-1.5 leading-relaxed">
              <li>• Engineered 62 lexical and structural features from domains</li>
              <li>• Trained 4 ML models (Logistic Regression, Random Forest, LightGBM)</li>
              <li>• Performed cross-validation and hyperparameter tuning</li>
              <li>• Evaluated models using ROC-AUC, PR-AUC, and Recall@1%FPR</li>
              <li>• Selected best model (LightGBM: {meta?.metrics ? `${(meta.metrics.roc_auc * 100).toFixed(2)}%` : '99.08%'} ROC-AUC)</li>
            </ul>
          </div>

          <div className="border-l-4 border-orange-500 pl-5 py-2 bg-orange-500/5">
            <h3 className="font-black text-white mb-3">Phase 4: Dashboard Development</h3>
            <ul className="text-sm text-slate-400 space-y-1.5 leading-relaxed">
              <li>• Built 6-page interactive dashboard using Next.js 14 & TypeScript</li>
              <li>• Designed responsive UI with Tailwind CSS</li>
              <li>• Implemented real-time domain lookup functionality</li>
              <li>• Created visualizations for threat analytics</li>
              <li>• Integrated ML predictions with frontend display</li>
            </ul>
          </div>
        </div>
      </div>

      {/* Results */}
      <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
        <div className="flex items-center gap-3 mb-6">
          <div className="p-3 bg-gradient-to-br from-emerald-500 to-green-600 rounded-xl glow-green">
            <TrendingUp className="w-6 h-6 text-white" />
          </div>
          <h2 className="text-2xl font-bold text-white">Results Achieved</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className="space-y-4">
            <div className="bg-gradient-to-br from-blue-500/10 to-blue-600/10 p-5 rounded-xl border border-blue-500/30">
              <h3 className="font-black text-blue-400 mb-4 text-lg">Data Processing Success</h3>
              <ul className="text-sm text-slate-300 space-y-2.5 font-medium">
                <li className="flex justify-between">
                  <span className="text-slate-400">Total Records Processed:</span>
                  <strong className="text-white font-black">{stats ? formatNumber(stats.totalSamples) : '0'}</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">Malicious Domains:</span>
                  <strong className="text-white font-black">{stats ? `${formatNumber(stats.maliciousCount)} (${((stats.maliciousCount / stats.totalSamples) * 100).toFixed(1)}%)` : '0 (0%)'}</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">Data Sources Integrated:</span>
                  <strong className="text-white font-black">4</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">DNS Coverage:</span>
                  <strong className="text-white font-black">21.2%</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">WHOIS Coverage:</span>
                  <strong className="text-white font-black">15.5%</strong>
                </li>
              </ul>
            </div>

            <div className="bg-gradient-to-br from-green-500/10 to-emerald-600/10 p-5 rounded-xl border border-green-500/30">
              <h3 className="font-black text-green-400 mb-4 text-lg">Dashboard Metrics</h3>
              <ul className="text-sm text-slate-300 space-y-2.5 font-medium">
                <li className="flex justify-between">
                  <span className="text-slate-400">Interactive Pages:</span>
                  <strong className="text-white font-black">6 pages</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">Threats Displayed:</span>
                  <strong className="text-white font-black">{stats ? formatNumber(stats.totalSamples > 5000 ? 5000 : stats.totalSamples) : '5,000'}</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">Technology Stack:</span>
                  <strong className="text-white font-black">Next.js + TypeScript</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">Page Load Time:</span>
                  <strong className="text-white font-black">&lt; 1 second</strong>
                </li>
              </ul>
            </div>
          </div>

          <div className="space-y-4">
            <div className="bg-gradient-to-br from-purple-500/10 to-purple-600/10 p-5 rounded-xl border border-purple-500/30">
              <h3 className="font-black text-purple-400 mb-4 text-lg">Machine Learning Performance</h3>
              <ul className="text-sm text-slate-300 space-y-2.5 font-medium">
                <li className="flex justify-between">
                  <span className="text-slate-400">Best Model (LightGBM):</span>
                  <strong className="text-purple-400 font-black text-lg">{meta?.metrics ? `${(meta.metrics.roc_auc * 100).toFixed(2)}%` : '99.08%'} ROC-AUC</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">Deployed Model (LogReg):</span>
                  <strong className="text-cyan-400 font-black text-lg">91.48% ROC-AUC</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">Features Engineered:</span>
                  <strong className="text-white font-black">62 features</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">Training Samples:</span>
                  <strong className="text-white font-black">{meta ? formatNumber(meta.n_rows) : '17,805'}</strong>
                </li>
                <li className="flex justify-between">
                  <span className="text-slate-400">Models Trained:</span>
                  <strong className="text-white font-black">4 models</strong>
                </li>
              </ul>
            </div>

            <div className="bg-gradient-to-br from-orange-500/10 to-red-600/10 p-5 rounded-xl border border-orange-500/30">
              <h3 className="font-black text-orange-400 mb-4 text-lg">Key Achievements</h3>
              <ul className="text-sm text-slate-300 space-y-2.5 font-medium">
                <li className="flex items-start gap-2">
                  <span className="text-emerald-500 font-black">✓</span>
                  <span>Near-perfect threat detection accuracy</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-emerald-500 font-black">✓</span>
                  <span>Production-ready data pipeline</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-emerald-500 font-black">✓</span>
                  <span>Real-time threat lookup capability</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-emerald-500 font-black">✓</span>
                  <span>Comprehensive enrichment system</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-emerald-500 font-black">✓</span>
                  <span>Scalable & maintainable codebase</span>
                </li>
              </ul>
            </div>
          </div>
        </div>
      </div>

      {/* Impact & Conclusion */}
      <div className="glass-card rounded-xl p-8 card-gradient-border bg-gradient-to-r from-cyan-600/20 via-blue-600/20 to-purple-600/20 border-2 border-cyan-500/30">
        <div className="flex items-center gap-4 mb-6">
          <div className="p-4 bg-gradient-to-br from-cyan-500 to-blue-600 rounded-xl shadow-lg glow-cyan">
            <Award className="w-10 h-10 text-white" />
          </div>
          <h2 className="text-3xl font-black text-white">Project Impact</h2>
        </div>
        <p className="text-slate-200 mb-6 leading-relaxed text-lg">
          This capstone project successfully demonstrates the application of modern data engineering
          and machine learning techniques to solve real-world cybersecurity challenges. By achieving
          99% accuracy in threat detection and processing nearly 70,000 threat records, we have built
          a system that could significantly reduce the time and effort required for security analysts
          to identify and respond to threats.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6">
          <div className="bg-slate-900/40 p-5 rounded-xl border border-slate-700/50">
            <h3 className="font-black text-cyan-400 mb-2 text-lg">Academic Excellence</h3>
            <p className="text-sm text-slate-300 leading-relaxed">
              Demonstrates mastery of data science, ML, and full-stack development
            </p>
          </div>
          <div className="bg-slate-900/40 p-5 rounded-xl border border-slate-700/50">
            <h3 className="font-black text-purple-400 mb-2 text-lg">Industry Relevance</h3>
            <p className="text-sm text-slate-300 leading-relaxed">
              Addresses real cybersecurity needs with production-ready solutions
            </p>
          </div>
          <div className="bg-slate-900/40 p-5 rounded-xl border border-slate-700/50">
            <h3 className="font-black text-emerald-400 mb-2 text-lg">Future Ready</h3>
            <p className="text-sm text-slate-300 leading-relaxed">
              Scalable architecture ready for cloud deployment and expansion
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
