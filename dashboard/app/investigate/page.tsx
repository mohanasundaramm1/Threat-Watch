'use client';

import { useState } from 'react';
import { Search, Bot, Globe, FileKey, Database, Brain, Shield, ExternalLink, Clock, AlertTriangle, CheckCircle, XCircle, Loader2 } from 'lucide-react';
import type { InvestigationResult } from '@/types';

const RISK_COLORS: Record<string, string> = {
    CRITICAL: 'from-red-600 to-red-700 text-white',
    HIGH: 'from-orange-500 to-red-500 text-white',
    MEDIUM: 'from-yellow-500 to-orange-500 text-white',
    LOW: 'from-green-500 to-emerald-600 text-white',
};

const RISK_BG: Record<string, string> = {
    CRITICAL: 'bg-red-500/10 border-red-500/30',
    HIGH: 'bg-orange-500/10 border-orange-500/30',
    MEDIUM: 'bg-yellow-500/10 border-yellow-500/30',
    LOW: 'bg-green-500/10 border-green-500/30',
};

type InvestigationStep = {
    label: string;
    icon: typeof Globe;
    status: 'pending' | 'active' | 'done' | 'error';
    detail?: string;
};

export default function InvestigatePage() {
    const [query, setQuery] = useState('');
    const [result, setResult] = useState<InvestigationResult | null>(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [steps, setSteps] = useState<InvestigationStep[]>([]);

    const updateStep = (index: number, updates: Partial<InvestigationStep>) => {
        setSteps(prev => prev.map((s, i) => i === index ? { ...s, ...updates } : s));
    };

    const handleInvestigate = async () => {
        const domain = query.trim().toLowerCase().replace(/^https?:\/\//, '').split('/')[0];
        if (!domain) return;

        setLoading(true);
        setError(null);
        setResult(null);

        const initialSteps: InvestigationStep[] = [
            { label: 'DNS Resolution', icon: Globe, status: 'active' },
            { label: 'WHOIS Lookup', icon: FileKey, status: 'pending' },
            { label: 'Threat Database', icon: Database, status: 'pending' },
            { label: 'ML Risk Scoring', icon: Brain, status: 'pending' },
            { label: 'AI Analysis', icon: Bot, status: 'pending' },
        ];
        setSteps(initialSteps);

        try {
            // Simulate step progress while API call runs
            const progressTimer = setInterval(() => {
                setSteps(prev => {
                    const activeIdx = prev.findIndex(s => s.status === 'active');
                    if (activeIdx >= 0 && activeIdx < prev.length - 1) {
                        const next = [...prev];
                        next[activeIdx] = { ...next[activeIdx], status: 'done' };
                        next[activeIdx + 1] = { ...next[activeIdx + 1], status: 'active' };
                        return next;
                    }
                    return prev;
                });
            }, 2500);

            const response = await fetch('/api/investigate', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ domain }),
            });

            clearInterval(progressTimer);

            if (!response.ok) {
                const errData = await response.json().catch(() => ({ error: 'Unknown error' }));
                throw new Error(errData.error || `HTTP ${response.status}`);
            }

            const data: InvestigationResult = await response.json();

            // Mark all steps done
            setSteps(prev => prev.map(s => ({
                ...s,
                status: 'done' as const,
                detail: s.label === 'DNS Resolution' ? `${data.findings.dns.ip_count} IPs, ${data.findings.dns.countries.join(', ') || 'unknown'}` :
                    s.label === 'WHOIS Lookup' ? `${data.findings.whois.age_days ?? '?'} days old, ${data.findings.whois.registrar || 'unknown'}` :
                        s.label === 'Threat Database' ? (data.findings.threat_db.found ? `Found in ${data.findings.threat_db.sources.join(', ')}` : 'Not found') :
                            s.label === 'ML Risk Scoring' ? `Score: ${data.risk_score?.toFixed(3) ?? 'N/A'} (${data.risk_level || 'N/A'})` :
                                s.label === 'AI Analysis' ? `${data.agent_mode === 'full' ? 'Sonar' : 'Template'} report generated` : undefined,
            })));

            setResult(data);
        } catch (e: any) {
            setError(e.message);
            setSteps(prev => prev.map(s => s.status === 'active' ? { ...s, status: 'error' as const } : s));
        } finally {
            setLoading(false);
        }
    };

    return (
        <div className="max-w-6xl mx-auto">
            {/* Header */}
            <div className="mb-8">
                <div className="flex items-center gap-3 mb-3">
                    <div className="p-2.5 bg-gradient-to-br from-violet-500 to-purple-600 rounded-xl">
                        <Bot className="w-6 h-6 text-white" />
                    </div>
                    <h1 className="text-5xl font-black gradient-text-cyan">AI Investigate</h1>
                </div>
                <p className="text-slate-400 text-lg">Enter any domain — our agent will investigate it autonomously using DNS, WHOIS, ML scoring, and web-grounded AI analysis.</p>
            </div>

            {/* Search */}
            <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
                <div className="flex gap-4">
                    <input
                        type="text"
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                        onKeyDown={(e) => e.key === 'Enter' && !loading && handleInvestigate()}
                        placeholder="Enter any domain (e.g., suspicious-site.com)"
                        className="flex-1 px-5 py-4 bg-slate-800/50 border border-slate-700 rounded-xl focus:ring-2 focus:ring-violet-500 focus:border-violet-500 text-white placeholder-slate-500 font-medium"
                    />
                    <button
                        onClick={handleInvestigate}
                        disabled={loading || !query.trim()}
                        className="px-8 py-4 bg-gradient-to-r from-violet-500 to-purple-600 text-white rounded-xl font-bold hover:shadow-lg hover:shadow-violet-500/50 disabled:opacity-50 disabled:cursor-not-allowed flex items-center gap-3 transition-all"
                    >
                        {loading ? <Loader2 className="w-5 h-5 animate-spin" /> : <Bot className="w-5 h-5" />}
                        {loading ? 'Investigating...' : 'Investigate'}
                    </button>
                </div>
            </div>

            {/* Investigation Steps Progress */}
            {steps.length > 0 && (
                <div className="glass-card rounded-xl p-6 mb-8 card-gradient-border">
                    <h3 className="text-sm font-bold text-slate-400 uppercase tracking-wider mb-4">Investigation Progress</h3>
                    <div className="space-y-3">
                        {steps.map((step, i) => {
                            const Icon = step.icon;
                            return (
                                <div key={i} className={`flex items-center gap-4 p-3 rounded-lg transition-all ${step.status === 'active' ? 'bg-violet-500/10 border border-violet-500/30' :
                                        step.status === 'done' ? 'bg-slate-800/30' :
                                            step.status === 'error' ? 'bg-red-500/10 border border-red-500/30' :
                                                'opacity-40'
                                    }`}>
                                    <div className={`p-2 rounded-lg ${step.status === 'active' ? 'bg-violet-500/20' :
                                            step.status === 'done' ? 'bg-emerald-500/20' :
                                                step.status === 'error' ? 'bg-red-500/20' :
                                                    'bg-slate-700/30'
                                        }`}>
                                        {step.status === 'active' ? <Loader2 className="w-4 h-4 text-violet-400 animate-spin" /> :
                                            step.status === 'done' ? <CheckCircle className="w-4 h-4 text-emerald-400" /> :
                                                step.status === 'error' ? <XCircle className="w-4 h-4 text-red-400" /> :
                                                    <Icon className="w-4 h-4 text-slate-500" />}
                                    </div>
                                    <span className={`font-bold text-sm ${step.status === 'done' ? 'text-emerald-400' :
                                            step.status === 'active' ? 'text-violet-400' :
                                                step.status === 'error' ? 'text-red-400' :
                                                    'text-slate-500'
                                        }`}>{step.label}</span>
                                    {step.detail && (
                                        <span className="text-xs text-slate-500 ml-auto font-medium">{step.detail}</span>
                                    )}
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}

            {/* Error */}
            {error && (
                <div className="glass-card rounded-xl p-6 mb-8 bg-red-500/10 border border-red-500/30">
                    <div className="flex items-center gap-3">
                        <AlertTriangle className="w-6 h-6 text-red-400" />
                        <p className="text-red-300 font-medium">{error}</p>
                    </div>
                </div>
            )}

            {/* Results */}
            {result && (
                <div className="space-y-6">
                    {/* Verdict Card */}
                    <div className={`glass-card rounded-xl p-6 border ${RISK_BG[result.risk_level || 'LOW']}`}>
                        <div className="flex items-center justify-between">
                            <div>
                                <h2 className="text-3xl font-black text-white mb-1">{result.domain}</h2>
                                <div className="flex items-center gap-3 text-sm text-slate-400">
                                    <Clock className="w-4 h-4" />
                                    <span>{result.latency_seconds?.toFixed(1)}s</span>
                                    <span>•</span>
                                    <span>{result.agent_mode === 'full' ? '🌐 Web-Grounded AI' : '📝 Template Analysis'}</span>
                                </div>
                            </div>
                            <div className="text-right">
                                <span className={`inline-block px-5 py-2.5 rounded-xl text-lg font-black bg-gradient-to-r ${RISK_COLORS[result.risk_level || 'LOW']}`}>
                                    {result.risk_level || 'N/A'}
                                </span>
                                <p className="text-2xl font-black text-white mt-2">{result.risk_score?.toFixed(3) ?? 'N/A'}</p>
                            </div>
                        </div>
                    </div>

                    {/* Evidence Grid */}
                    <div className="grid grid-cols-3 gap-6">
                        {/* DNS Card */}
                        <div className="glass-card rounded-xl p-5 card-gradient-border">
                            <div className="flex items-center gap-2 mb-4">
                                <Globe className="w-5 h-5 text-cyan-400" />
                                <h3 className="font-bold text-cyan-400 text-sm uppercase tracking-wider">DNS</h3>
                            </div>
                            {result.findings.dns.error ? (
                                <p className="text-sm text-red-400">{result.findings.dns.error}</p>
                            ) : (
                                <div className="space-y-2 text-sm">
                                    <div><span className="text-slate-500">IPs:</span> <span className="text-white font-medium">{result.findings.dns.ips.join(', ') || 'None'}</span></div>
                                    <div><span className="text-slate-500">Countries:</span> <span className="text-white font-medium">{result.findings.dns.countries.join(', ') || 'Unknown'}</span></div>
                                    <div><span className="text-slate-500">ASNs:</span> <span className="text-white font-medium">{result.findings.dns.asns.join(', ') || 'Unknown'}</span></div>
                                    <div><span className="text-slate-500">IPv6:</span> <span className="text-white font-medium">{result.findings.dns.has_ipv6 ? 'Yes' : 'No'}</span></div>
                                </div>
                            )}
                        </div>

                        {/* WHOIS Card */}
                        <div className="glass-card rounded-xl p-5 card-gradient-border">
                            <div className="flex items-center gap-2 mb-4">
                                <FileKey className="w-5 h-5 text-violet-400" />
                                <h3 className="font-bold text-violet-400 text-sm uppercase tracking-wider">WHOIS</h3>
                            </div>
                            {result.findings.whois.error ? (
                                <p className="text-sm text-red-400">{result.findings.whois.error}</p>
                            ) : (
                                <div className="space-y-2 text-sm">
                                    <div><span className="text-slate-500">Registrar:</span> <span className="text-white font-medium">{result.findings.whois.registrar || 'Unknown'}</span></div>
                                    <div><span className="text-slate-500">Age:</span> <span className="text-white font-medium">{result.findings.whois.age_days != null ? `${result.findings.whois.age_days} days` : 'Unknown'}</span></div>
                                    <div><span className="text-slate-500">Created:</span> <span className="text-white font-medium">{result.findings.whois.created_date?.split(' ')[0] || 'Unknown'}</span></div>
                                    <div><span className="text-slate-500">Status:</span> <span className="text-white font-medium truncate block">{result.findings.whois.status || 'Unknown'}</span></div>
                                </div>
                            )}
                        </div>

                        {/* Threat DB Card */}
                        <div className="glass-card rounded-xl p-5 card-gradient-border">
                            <div className="flex items-center gap-2 mb-4">
                                <Database className="w-5 h-5 text-orange-400" />
                                <h3 className="font-bold text-orange-400 text-sm uppercase tracking-wider">Threat DB</h3>
                            </div>
                            {result.findings.threat_db.found ? (
                                <div className="space-y-2 text-sm">
                                    <div className="flex items-center gap-2">
                                        <AlertTriangle className="w-4 h-4 text-red-400" />
                                        <span className="text-red-400 font-bold">FOUND IN DATABASE</span>
                                    </div>
                                    <div><span className="text-slate-500">Sources:</span> <span className="text-white font-medium">{result.findings.threat_db.sources.join(', ')}</span></div>
                                    <div><span className="text-slate-500">Labels:</span> <span className="text-white font-medium">{result.findings.threat_db.labels.join(', ')}</span></div>
                                </div>
                            ) : (
                                <div className="flex items-center gap-2">
                                    <CheckCircle className="w-4 h-4 text-emerald-400" />
                                    <span className="text-emerald-400 font-medium text-sm">Not found in local database</span>
                                </div>
                            )}
                        </div>
                    </div>

                    {/* SHAP Feature Contributions */}
                    {Object.keys(result.findings.shap.feature_contributions).length > 0 && (
                        <div className="glass-card rounded-xl p-6 card-gradient-border">
                            <div className="flex items-center gap-2 mb-5">
                                <Brain className="w-5 h-5 text-emerald-400" />
                                <h3 className="font-bold text-emerald-400 text-sm uppercase tracking-wider">ML Explainability (SHAP)</h3>
                            </div>
                            <div className="space-y-3">
                                {Object.entries(result.findings.shap.feature_contributions).map(([feature, value]) => {
                                    const maxVal = Math.max(...Object.values(result.findings.shap.feature_contributions).map(Math.abs));
                                    const pct = Math.abs(value) / (maxVal || 1) * 100;
                                    const isPositive = value > 0;
                                    return (
                                        <div key={feature} className="flex items-center gap-4">
                                            <span className="text-xs text-slate-400 w-36 text-right font-medium truncate">
                                                {feature.replace(/_/g, ' ')}
                                            </span>
                                            <div className="flex-1 flex items-center gap-2">
                                                <div className="flex-1 bg-slate-800 rounded-full h-3 overflow-hidden">
                                                    <div
                                                        className={`h-full rounded-full ${isPositive ? 'bg-gradient-to-r from-red-500 to-red-400' : 'bg-gradient-to-r from-emerald-500 to-emerald-400'}`}
                                                        style={{ width: `${Math.min(pct, 100)}%` }}
                                                    />
                                                </div>
                                                <span className={`text-xs font-bold w-16 ${isPositive ? 'text-red-400' : 'text-emerald-400'}`}>
                                                    {value > 0 ? '+' : ''}{value.toFixed(3)}
                                                </span>
                                            </div>
                                        </div>
                                    );
                                })}
                                <p className="text-xs text-slate-500 mt-2">
                                    <span className="text-red-400">Red</span> = increases risk · <span className="text-emerald-400">Green</span> = decreases risk
                                </p>
                            </div>
                        </div>
                    )}

                    {/* AI Report */}
                    <div className="glass-card rounded-xl p-6 card-gradient-border">
                        <div className="flex items-center gap-2 mb-5">
                            <Bot className="w-5 h-5 text-violet-400" />
                            <h3 className="font-bold text-violet-400 text-sm uppercase tracking-wider">
                                AI Threat Analysis {result.agent_mode === 'full' && '(Perplexity Sonar)'}
                            </h3>
                        </div>
                        <div className="prose prose-invert prose-sm max-w-none text-slate-300 leading-relaxed"
                            dangerouslySetInnerHTML={{ __html: renderMarkdown(result.ai_report) }}
                        />
                        {/* Citations */}
                        {result.citations.length > 0 && (
                            <div className="mt-6 pt-4 border-t border-slate-700/50">
                                <h4 className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">Sources</h4>
                                <div className="space-y-1">
                                    {result.citations.map((url, i) => (
                                        <a key={i} href={url} target="_blank" rel="noopener noreferrer"
                                            className="flex items-center gap-2 text-xs text-cyan-400 hover:text-cyan-300 transition-colors">
                                            <ExternalLink className="w-3 h-3" />
                                            {url}
                                        </a>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>

                    {/* Recommendations */}
                    {result.recommendations.length > 0 && (
                        <div className="glass-card rounded-xl p-6 card-gradient-border">
                            <div className="flex items-center gap-2 mb-5">
                                <Shield className="w-5 h-5 text-cyan-400" />
                                <h3 className="font-bold text-cyan-400 text-sm uppercase tracking-wider">Recommended Actions</h3>
                            </div>
                            <div className="space-y-3">
                                {result.recommendations.map((rec, i) => (
                                    <div key={i} className="flex items-start gap-3 p-3 bg-slate-800/30 rounded-lg">
                                        <span className="flex-shrink-0 w-6 h-6 bg-cyan-500/20 text-cyan-400 rounded-full flex items-center justify-center text-xs font-bold">{i + 1}</span>
                                        <p className="text-sm text-slate-300">{rec}</p>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>
            )}

            {/* Empty State */}
            {!loading && !result && !error && steps.length === 0 && (
                <div className="glass-card rounded-xl p-12 text-center card-gradient-border">
                    <Bot className="w-16 h-16 text-violet-500/40 mx-auto mb-4" />
                    <h3 className="text-xl font-bold text-white mb-2">Ready to Investigate</h3>
                    <p className="text-slate-400 mb-6">Enter any domain above — our AI agent will gather DNS, WHOIS, and threat data, score it with ML, and generate an explainable threat report.</p>
                    <div className="flex flex-wrap gap-2 justify-center">
                        {['google.com', 'suspicious-domain.xyz', 'paypal-secure-login.com'].map(d => (
                            <button
                                key={d}
                                onClick={() => { setQuery(d); }}
                                className="px-4 py-2 bg-slate-800/50 hover:bg-slate-700/50 border border-slate-700 hover:border-violet-500/30 rounded-lg text-sm text-slate-400 hover:text-white transition-all"
                            >
                                {d}
                            </button>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}

/** Minimal markdown → HTML renderer */
function renderMarkdown(md: string): string {
    if (!md) return '';
    return md
        .replace(/### (.*)/g, '<h3 class="text-lg font-bold text-white mt-4 mb-2">$1</h3>')
        .replace(/## (.*)/g, '<h2 class="text-xl font-bold text-white mt-4 mb-2">$1</h2>')
        .replace(/\*\*(.*?)\*\*/g, '<strong class="text-white">$1</strong>')
        .replace(/^\- (.*)/gm, '<li class="ml-4">$1</li>')
        .replace(/^\d+\. (.*)/gm, '<li class="ml-4 list-decimal">$1</li>')
        .replace(/\n\n/g, '<br/><br/>')
        .replace(/\n/g, '<br/>');
}
