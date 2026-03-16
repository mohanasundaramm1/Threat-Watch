export interface ThreatDomain {
  domain: string;
  url: string;
  firstSeen: string;
  label: string;
  source: string;
  riskScore: number;
  riskLevel: 'LOW' | 'MEDIUM' | 'HIGH' | 'CRITICAL';
  threatType: string;
  country?: string;
  registrar?: string;
  domainAgeDays?: number;
}

export interface DashboardStats {
  totalSamples: number;
  maliciousCount: number;
  benignCount: number;
  riskDistribution: Record<string, number>;
  threatTypeDistribution: Record<string, number>;
  sourceDistribution: Record<string, number>;
  averageRiskScore: number;
  highRiskCount: number;
  dateRange: {
    earliest: string | null;
    latest: string | null;
  };
  topCountries?: Record<string, number>;
  topRegistrars?: Record<string, number>;
}

export interface ModelMeta {
  created_utc: string;
  n_rows: number;
  n_pos: number;
  n_neg: number;
  metrics: {
    roc_auc: number;
    pr_auc: number;
    recall_at_1pct: number;
  };
  model_file: string;
}

// ── Agentic Investigation Types ──────────────────────────────────

export interface DNSFindings {
  ips: string[];
  countries: string[];
  asns: string[];
  isps: string[];
  has_ipv6: boolean;
  ip_count: number;
  error?: string;
}

export interface WHOISFindings {
  registrar?: string;
  created_date?: string;
  expiry_date?: string;
  age_days?: number;
  status?: string;
  name_servers: string[];
  error?: string;
}

export interface ThreatDBFindings {
  found: boolean;
  sources: string[];
  labels: string[];
  first_seen?: string;
  risk_score_from_db?: number;
}

export interface SHAPExplanation {
  feature_contributions: Record<string, number>;
  top_risk_factors: string[];
}

export interface InvestigationFindings {
  dns: DNSFindings;
  whois: WHOISFindings;
  threat_db: ThreatDBFindings;
  shap: SHAPExplanation;
}

export interface InvestigationResult {
  id: string;
  domain: string;
  risk_score: number | null;
  risk_level: string | null;
  findings: InvestigationFindings;
  ai_report: string;
  citations: string[];
  recommendations: string[];
  investigated_at: string;
  latency_seconds: number | null;
  agent_mode: string;
}

export interface InvestigationSummary {
  id: string;
  domain: string;
  risk_score: number | null;
  risk_level: string | null;
  agent_mode: string;
  investigated_at: string;
}
